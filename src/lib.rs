use std::io::{BufReader, BufWriter, Read, Write};

use adapters::{BufReadWritePyFileObject, PyFileObject};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBufferError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyList, PyType};
use pyo3::{create_exception, wrap_pyfunction};

mod adapters;

create_exception!(lazrs, LazrsError, pyo3::exceptions::PyRuntimeError);

/// Read-only view on the memory of a Python object implementing the buffer protocol.
///
/// The buffer export is held as long as the view lives: the object is kept alive
/// and cannot be resized, so slices borrowed from the view stay valid.
struct BufferView(PyBuffer<u8>);

impl BufferView {
    fn new(object: &Bound<'_, PyAny>) -> PyResult<Self> {
        let buffer = PyBuffer::<u8>::get(object)?;
        if !buffer.is_c_contiguous() {
            return Err(PyBufferError::new_err("buffer is not C-contiguous"));
        }
        Ok(Self(buffer))
    }

    fn as_slice<'a>(&'a self, py: Python<'a>) -> &'a [u8] {
        let cells = self.0.as_slice(py).expect("contiguity is checked in new");
        // SAFETY: ReadOnlyCell<u8> has the same layout as u8, and the export is held
        // for 'a so the memory cannot be freed or resized.
        // The buffer protocol cannot prevent other code from writing to the memory
        // through another view, callers must not do that while lazrs uses the buffer.
        unsafe { std::slice::from_raw_parts(cells.as_ptr().cast::<u8>(), cells.len()) }
    }
}

/// Writable view on the memory of a Python object implementing the buffer protocol.
///
/// See [`BufferView`].
struct BufferViewMut(PyBuffer<u8>);

impl BufferViewMut {
    fn new(object: &Bound<'_, PyAny>) -> PyResult<Self> {
        let buffer = PyBuffer::<u8>::get(object)?;
        if buffer.readonly() {
            return Err(PyTypeError::new_err("buffer is readonly"));
        }
        if !buffer.is_c_contiguous() {
            return Err(PyBufferError::new_err("buffer is not C-contiguous"));
        }
        Ok(Self(buffer))
    }

    fn as_mut_slice<'a>(&'a mut self, py: Python<'a>) -> &'a mut [u8] {
        let cells = self
            .0
            .as_mut_slice(py)
            .expect("writability and contiguity are checked in new");
        // SAFETY: Cell<u8> has the same layout as u8, and the export is held
        // for 'a so the memory cannot be freed or resized.
        // Taking &mut self prevents getting two slices from the same view, but not from
        // two views on the same object, nor writes from other code through another view.
        // Callers must not access the buffer while lazrs writes into it.
        unsafe {
            std::slice::from_raw_parts_mut(cells.as_ptr().cast_mut().cast::<u8>(), cells.len())
        }
    }
}

fn into_py_err<T: std::fmt::Display>(error: T) -> PyErr {
    PyErr::new::<LazrsError, _>(format!("{}", error))
}

#[pyclass(from_py_object)]
#[derive(Copy, Clone, Debug)]
struct DecompressionSelection(laz::DecompressionSelection);

#[pymethods]
impl DecompressionSelection {
    #[new]
    fn new(value: u32) -> Self {
        Self(laz::DecompressionSelection(value))
    }
}

#[pyclass]
struct LazVlr {
    vlr: laz::LazVlr,
}

#[pymethods]
impl LazVlr {
    #[new]
    fn new<'py>(record_data: &Bound<'py, PyAny>) -> PyResult<Self> {
        let vlr_data = BufferView::new(record_data)?;
        let vlr =
            laz::LazVlr::read_from(vlr_data.as_slice(record_data.py())).map_err(into_py_err)?;
        Ok(LazVlr { vlr })
    }

    #[classmethod]
    #[pyo3(signature = (point_format_id, num_extra_bytes, use_variable_size_chunks=false))]
    fn new_for_compression<'py>(
        _cls: &Bound<'py, PyType>,
        point_format_id: u8,
        num_extra_bytes: u16,
        use_variable_size_chunks: bool,
    ) -> PyResult<Self> {
        let mut builder = laz::LazVlrBuilder::default()
            .with_point_format(point_format_id, num_extra_bytes)
            .map_err(into_py_err)?;

        if use_variable_size_chunks {
            builder = builder.with_variable_chunk_size();
        }

        let vlr = builder.build();
        Ok(LazVlr { vlr })
    }

    fn uses_variable_size_chunks(&self) -> bool {
        self.vlr.uses_variable_size_chunks()
    }

    fn chunk_size(&self) -> u32 {
        self.vlr.chunk_size()
    }

    fn item_size(&self) -> u64 {
        self.vlr.items_size()
    }

    fn record_data(&self) -> PyResult<Py<PyAny>> {
        let mut data = std::io::Cursor::new(Vec::<u8>::new());
        self.vlr
            .write_to(&mut data)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;

        Python::attach(|py| {
            let bytes = PyBytes::new(py, data.get_ref()).into_any().unbind();
            Ok(bytes)
        })
    }
}

#[pyclass]
struct ParLasZipCompressor {
    compressor: laz::ParLasZipCompressor<BufWriter<PyFileObject>>,
}

#[pymethods]
impl ParLasZipCompressor {
    #[new]
    fn new(dest: Py<PyAny>, vlr: &LazVlr) -> PyResult<Self> {
        let dest = Python::attach(|py| PyFileObject::new(py, dest))?;
        let dest = BufWriter::new(dest);
        let compressor =
            laz::ParLasZipCompressor::new(dest, vlr.vlr.clone()).map_err(into_py_err)?;
        Ok(ParLasZipCompressor { compressor })
    }

    pub fn reserve_offset_to_chunk_table(&mut self) -> PyResult<()> {
        self.compressor
            .reserve_offset_to_chunk_table()
            .map_err(into_py_err)?;
        self.compressor.get_mut().flush().map_err(into_py_err)
    }

    fn compress_many<'py>(&mut self, points: &Bound<'py, PyAny>) -> PyResult<()> {
        let point_bytes = BufferView::new(points)?;

        self.compressor
            .compress_many(point_bytes.as_slice(points.py()))
            .map_err(into_py_err)
    }

    pub fn compress_chunks<'py>(&mut self, chunks: &Bound<'py, PyList>) -> PyResult<()> {
        let py = chunks.py();
        let views = chunks
            .iter()
            .map(|chunk| BufferView::new(&chunk))
            .collect::<PyResult<Vec<BufferView>>>()?;
        let chunks = views
            .iter()
            .map(|view| view.as_slice(py))
            .collect::<Vec<&[u8]>>();
        self.compressor.compress_chunks(chunks)?;
        Ok(())
    }

    fn done(&mut self) -> PyResult<()> {
        self.compressor.done().map_err(into_py_err)?;
        self.compressor.get_mut().flush().map_err(into_py_err)
    }
}

#[pyclass]
struct ParLasZipDecompressor {
    decompressor: laz::ParLasZipDecompressor<BufReader<PyFileObject>>,
}

#[pymethods]
impl ParLasZipDecompressor {
    #[new]
    #[pyo3(signature=(source, vlr_record_data, selection = None))]
    fn new<'py>(
        source: Py<PyAny>,
        vlr_record_data: &Bound<'py, PyAny>,
        selection: Option<DecompressionSelection>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            let source = BufReader::new(PyFileObject::new(py, source)?);
            let vlr_data = BufferView::new(vlr_record_data)?;
            let vlr = laz::LazVlr::read_from(vlr_data.as_slice(py)).map_err(into_py_err)?;

            if let Some(selection) = selection {
                Ok(ParLasZipDecompressor {
                    decompressor: laz::ParLasZipDecompressor::selective(source, vlr, selection.0)
                        .map_err(into_py_err)?,
                })
            } else {
                Ok(ParLasZipDecompressor {
                    decompressor: laz::ParLasZipDecompressor::new(source, vlr)
                        .map_err(into_py_err)?,
                })
            }
        })
    }

    fn decompress_many<'py>(&mut self, points: &Bound<'py, PyAny>) -> PyResult<()> {
        let mut point_bytes = BufferViewMut::new(points)?;
        self.decompressor
            .decompress_many(point_bytes.as_mut_slice(points.py()))
            .map_err(into_py_err)?;
        Ok(())
    }

    pub fn seek(&mut self, point_idx: u64) -> PyResult<()> {
        self.decompressor.seek(point_idx).map_err(into_py_err)
    }

    pub fn read_raw_bytes_into<'py>(&mut self, bytes: &Bound<'py, PyAny>) -> PyResult<()> {
        let mut view = BufferViewMut::new(bytes)?;
        self.decompressor
            .get_mut()
            .read_exact(view.as_mut_slice(bytes.py()))
            .map_err(into_py_err)
    }
}

#[pyclass]
struct LasZipDecompressor {
    decompressor: laz::LasZipDecompressor<'static, BufReader<PyFileObject>>,
}

#[pymethods]
impl LasZipDecompressor {
    #[new]
    #[pyo3(signature = (source, record_data, selection = None))]
    pub fn new<'py>(
        source: Py<PyAny>,
        record_data: &Bound<'py, PyAny>,
        selection: Option<DecompressionSelection>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            let source = BufReader::new(PyFileObject::new(py, source)?);
            let vlr_data = BufferView::new(record_data)?;
            let vlr = laz::LazVlr::read_from(vlr_data.as_slice(py)).map_err(into_py_err)?;

            if let Some(selection) = selection {
                Ok(Self {
                    decompressor: laz::LasZipDecompressor::selective(source, vlr, selection.0)
                        .map_err(into_py_err)?,
                })
            } else {
                Ok(Self {
                    decompressor: laz::LasZipDecompressor::new(source, vlr).map_err(into_py_err)?,
                })
            }
        })
    }

    pub fn decompress_many<'py>(&mut self, dest: &Bound<'py, PyAny>) -> PyResult<()> {
        let mut view = BufferViewMut::new(dest)?;
        self.decompressor
            .decompress_many(view.as_mut_slice(dest.py()))
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))
    }

    pub fn seek(&mut self, point_idx: u64) -> PyResult<()> {
        self.decompressor.seek(point_idx).map_err(into_py_err)
    }

    pub fn vlr(&self) -> LazVlr {
        LazVlr {
            vlr: self.decompressor.vlr().clone(),
        }
    }

    // See the documentation of the free function with the same name.
    // it has the same requirements.
    pub fn read_chunk_table_only(&mut self) -> PyResult<Py<PyAny>> {
        Python::attach(|py| {
            let uses_variable_chunk_size = self.decompressor.vlr().uses_variable_size_chunks();
            let chunk_table = laz::laszip::ChunkTable::read(
                self.decompressor.get_mut(),
                uses_variable_chunk_size,
            )
            .map_err(into_py_err)?;
            let elements = chunk_table
                .as_ref()
                .iter()
                .map(|entry| (entry.point_count, entry.byte_count));
            let list = PyList::new(py, elements)?;
            Ok(list.into_any().unbind())
        })
    }

    pub fn read_raw_bytes_into<'py>(&mut self, bytes: &Bound<'py, PyAny>) -> PyResult<()> {
        let mut view = BufferViewMut::new(bytes)?;
        self.decompressor
            .get_mut()
            .read_exact(view.as_mut_slice(bytes.py()))
            .map_err(into_py_err)
    }
}

#[pyclass]
struct LasZipCompressor {
    compressor: laz::LasZipCompressor<'static, BufWriter<PyFileObject>>,
}

#[pymethods]
impl LasZipCompressor {
    #[new]
    pub fn new(dest: Py<PyAny>, vlr: &LazVlr) -> PyResult<Self> {
        let dest = Python::attach(|py| PyFileObject::new(py, dest))?;
        let dest = BufWriter::new(dest);
        let compressor = laz::LasZipCompressor::new(dest, vlr.vlr.clone()).map_err(into_py_err)?;
        Ok(Self { compressor })
    }

    pub fn reserve_offset_to_chunk_table(&mut self) -> PyResult<()> {
        self.compressor
            .reserve_offset_to_chunk_table()
            .map_err(into_py_err)?;
        self.compressor.get_mut().flush().map_err(into_py_err)
    }

    pub fn compress_many<'py>(&mut self, points: &Bound<'py, PyAny>) -> PyResult<()> {
        let point_bytes = BufferView::new(points)?;
        self.compressor
            .compress_many(point_bytes.as_slice(points.py()))
            .map_err(into_py_err)
    }

    pub fn done(&mut self) -> PyResult<()> {
        self.compressor.done().map_err(into_py_err)?;
        self.compressor.get_mut().flush().map_err(into_py_err)
    }

    pub fn compress_chunks<'py>(&mut self, chunks: &Bound<'py, PyList>) -> PyResult<()> {
        for chunk in chunks.iter() {
            self.compress_many(&chunk)?;
            self.finish_current_chunk()?;
        }
        Ok(())
    }

    pub fn finish_current_chunk(&mut self) -> PyResult<()> {
        self.compressor.finish_current_chunk().map_err(into_py_err)
    }
}

#[pyfunction]
fn decompress_points<'py>(
    compressed_points_data: &Bound<'py, PyAny>,
    laszip_vlr_record_data: &Bound<'py, PyAny>,
    decompression_output: &Bound<'py, PyAny>,
    parallel: bool,
) -> PyResult<()> {
    let py = decompression_output.py();
    let vlr_data = BufferView::new(laszip_vlr_record_data)?;
    let data = BufferView::new(compressed_points_data)?;
    let mut output_view = BufferViewMut::new(decompression_output)?;
    let data_slc = data.as_slice(py);
    let output = output_view.as_mut_slice(py);

    laz::LazVlr::read_from(vlr_data.as_slice(py))
        .and_then(|vlr| {
            if !parallel {
                laz::decompress_buffer(data_slc, output, vlr)
            } else {
                laz::par_decompress_buffer(data_slc, output, &vlr)
            }
        })
        .map_err(into_py_err)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (
    compressed_points_data,
    laszip_vlr_record_data,
    decompression_output,
    py_chunk_table,
    selection = None
))]
fn decompress_points_with_chunk_table<'py>(
    compressed_points_data: &Bound<'py, PyAny>,
    laszip_vlr_record_data: &Bound<'py, PyAny>,
    decompression_output: &Bound<'py, PyAny>,
    py_chunk_table: &Bound<'py, PyList>,
    selection: Option<DecompressionSelection>,
) -> PyResult<()> {
    let py = decompression_output.py();
    let vlr_data = BufferView::new(laszip_vlr_record_data)?;
    let data = BufferView::new(compressed_points_data)?;
    let mut output_view = BufferViewMut::new(decompression_output)?;
    let chunk_table = chunk_table_from_py_list(py_chunk_table)?;
    let data_slc = data.as_slice(py);
    let output = output_view.as_mut_slice(py);

    laz::LazVlr::read_from(vlr_data.as_slice(py))
        .and_then(|vlr| {
            if let Some(selection) = selection {
                laz::par_decompress_selective(
                    data_slc,
                    output,
                    &vlr,
                    chunk_table.as_ref(),
                    selection.0,
                )
            } else {
                laz::par_decompress(data_slc, output, &vlr, chunk_table.as_ref())
            }
        })
        .map_err(into_py_err)?;

    Ok(())
}

#[pyfunction]
fn compress_points<'py>(
    laszip_vlr: &LazVlr,
    uncompressed_points: &Bound<'py, PyAny>,
    parallel: bool,
) -> PyResult<Py<PyAny>> {
    let point_bytes = BufferView::new(uncompressed_points)?;
    let point_bytes = point_bytes.as_slice(uncompressed_points.py());
    let mut compression_result = std::io::Cursor::new(Vec::<u8>::new());
    if !parallel {
        laz::compress_buffer(&mut compression_result, point_bytes, laszip_vlr.vlr.clone())
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;
    } else {
        laz::par_compress_buffer(&mut compression_result, point_bytes, &laszip_vlr.vlr)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;
    }
    Python::attach(|py| {
        let bytes = PyBytes::new(py, compression_result.get_ref())
            .into_any()
            .unbind();
        Ok(bytes)
    })
}

/// This reads the chunks table.
///
/// It reads it by first reading the offset to the table,
/// seeks to the position given by the offset, and reads the table.
///
/// Afterwards, it leaves the source position at that actual start of points.
///
/// The `source` position **must** be at the beginning of the points data
#[pyfunction]
fn read_chunk_table(source: Py<PyAny>, vlr: &LazVlr) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let mut src = BufReader::new(PyFileObject::new(py, source)?);

        let chunk_table =
            laz::laszip::ChunkTable::read_from(&mut src, &vlr.vlr).map_err(into_py_err)?;
        let elements = chunk_table
            .as_ref()
            .iter()
            .map(|entry| (entry.point_count, entry.byte_count));
        let list = PyList::new(py, elements)?;
        Ok(list.into_any().unbind())
    })
}

/// This reads the chunks table.
///
/// This simply reads the chunk table, it *does not* read offset nor seeks
/// *nor* does it puts the source position to the actual start of points data
/// afterwards.
///
/// The `source` position **must** be at the beginning of the chunk table
#[pyfunction]
fn read_chunk_table_only(source: Py<PyAny>, vlr: &LazVlr) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let mut src = BufReader::new(PyFileObject::new(py, source)?);

        let chunk_table = laz::laszip::ChunkTable::read(&mut src, vlr.uses_variable_size_chunks())
            .map_err(into_py_err)?;
        let elements = chunk_table
            .as_ref()
            .iter()
            .map(|entry| (entry.point_count, entry.byte_count));
        let list = PyList::new(py, elements)?;
        Ok(list.into_any().unbind())
    })
}

fn chunk_table_from_py_list<'py>(
    py_chunk_table: &Bound<'py, PyList>,
) -> PyResult<laz::laszip::ChunkTable> {
    let mut chunk_table = laz::laszip::ChunkTable::with_capacity(py_chunk_table.len());

    for object in py_chunk_table.iter() {
        let (point_count, byte_count): (u64, u64) = object.extract()?;
        chunk_table.push(laz::laszip::ChunkTableEntry {
            point_count,
            byte_count,
        });
    }
    Ok(chunk_table)
}

#[pyfunction]
fn write_chunk_table<'py>(
    dest: Py<PyAny>,
    py_chunk_table: &Bound<'py, PyList>,
    vlr: &LazVlr,
) -> PyResult<()> {
    let chunk_table = chunk_table_from_py_list(py_chunk_table)?;

    let dest = Python::attach(|py| PyFileObject::new(py, dest).map(BufWriter::new))?;
    chunk_table.write_to(dest, &vlr.vlr).map_err(into_py_err)
}

#[pyclass]
struct ParLasZipAppender {
    appender: laz::ParLasZipAppender<BufReadWritePyFileObject>,
}

#[pymethods]
impl ParLasZipAppender {
    #[new]
    fn new<'py>(
        dest: Py<PyAny>,
        laz_vlr_record_data: &Bound<'py, PyAny>,
        point_count: u64,
    ) -> PyResult<Self> {
        let data =
            Python::attach(|py| PyFileObject::new(py, dest).map(BufReadWritePyFileObject::new))?;
        let vlr_data = BufferView::new(laz_vlr_record_data)?;
        let vlr = laz::LazVlr::read_from(vlr_data.as_slice(laz_vlr_record_data.py()))
            .map_err(into_py_err)?;
        let appender = laz::ParLasZipAppender::new(data, vlr, point_count).map_err(into_py_err)?;
        Ok(ParLasZipAppender { appender })
    }

    fn compress_many<'py>(&mut self, points: &Bound<'py, PyAny>) -> PyResult<()> {
        let point_bytes = BufferView::new(points)?;

        self.appender
            .compress_many(point_bytes.as_slice(points.py()))
            .map_err(into_py_err)
    }

    pub fn compress_chunks<'py>(&mut self, chunks: &Bound<'py, PyList>) -> PyResult<()> {
        let py = chunks.py();
        let views = chunks
            .iter()
            .map(|chunk| BufferView::new(&chunk))
            .collect::<PyResult<Vec<BufferView>>>()?;
        let chunks = views
            .iter()
            .map(|view| view.as_slice(py))
            .collect::<Vec<&[u8]>>();
        self.appender.compress_chunks(chunks)?;
        Ok(())
    }

    fn done(&mut self) -> PyResult<()> {
        self.appender.done().map_err(into_py_err)?;
        self.appender.get_mut().flush().map_err(into_py_err)
    }
}

#[pyclass]
struct LasZipAppender {
    appender: laz::LasZipAppender<'static, BufReadWritePyFileObject>,
}

#[pymethods]
impl LasZipAppender {
    #[new]
    fn new<'py>(
        dest: Py<PyAny>,
        laz_vlr_record_data: &Bound<'py, PyAny>,
        point_count: u64,
    ) -> PyResult<Self> {
        let data =
            Python::attach(|py| PyFileObject::new(py, dest).map(BufReadWritePyFileObject::new))?;
        let vlr_data = BufferView::new(laz_vlr_record_data)?;
        let vlr = laz::LazVlr::read_from(vlr_data.as_slice(laz_vlr_record_data.py()))
            .map_err(into_py_err)?;
        let appender = laz::LasZipAppender::new(data, vlr, point_count).map_err(into_py_err)?;
        Ok(LasZipAppender { appender })
    }

    fn compress_many<'py>(&mut self, points: &Bound<'py, PyAny>) -> PyResult<()> {
        let point_bytes = BufferView::new(points)?;

        self.appender
            .compress_many(point_bytes.as_slice(points.py()))
            .map_err(into_py_err)
    }

    pub fn compress_chunks<'py>(&mut self, chunks: &Bound<'py, PyList>) -> PyResult<()> {
        let py = chunks.py();
        let views = chunks
            .iter()
            .map(|chunk| BufferView::new(&chunk))
            .collect::<PyResult<Vec<BufferView>>>()?;
        let chunks = views
            .iter()
            .map(|view| view.as_slice(py))
            .collect::<Vec<&[u8]>>();
        self.appender.compress_chunks(chunks)?;
        Ok(())
    }

    fn done(&mut self) -> PyResult<()> {
        self.appender.done().map_err(into_py_err)?;
        self.appender.get_mut().flush().map_err(into_py_err)
    }
}

/// This module is a python module implemented in Rust.
#[pymodule(gil_used = false)]
fn lazrs<'py>(py: Python, m: &Bound<'py, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(decompress_points))?;
    m.add_wrapped(wrap_pyfunction!(compress_points))?;
    m.add_wrapped(wrap_pyfunction!(read_chunk_table))?;
    m.add_wrapped(wrap_pyfunction!(read_chunk_table_only))?;
    m.add_wrapped(wrap_pyfunction!(write_chunk_table))?;
    m.add_wrapped(wrap_pyfunction!(decompress_points_with_chunk_table))?;
    m.add("LazrsError", py.get_type::<LazrsError>())?;
    m.add_class::<LazVlr>()?;
    m.add_class::<LasZipDecompressor>()?;
    m.add_class::<LasZipCompressor>()?;
    m.add_class::<LasZipAppender>()?;
    m.add_class::<ParLasZipCompressor>()?;
    m.add_class::<ParLasZipDecompressor>()?;
    m.add_class::<ParLasZipAppender>()?;
    m.add_class::<DecompressionSelection>()?;

    m.add(
        "SELECTIVE_DECOMPRESS_XY_RETURNS_CHANNEL",
        laz::DecompressionSelection::XY_RETURNS_CHANNEL,
    )?;
    m.add("SELECTIVE_DECOMPRESS_ALL", laz::DecompressionSelection::ALL)?;
    m.add("SELECTIVE_DECOMPRESS_Z", laz::DecompressionSelection::Z)?;
    m.add(
        "SELECTIVE_DECOMPRESS_CLASSIFICATION",
        laz::DecompressionSelection::CLASSIFICATION,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_FLAGS",
        laz::DecompressionSelection::FLAGS,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_INTENSITY",
        laz::DecompressionSelection::INTENSITY,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_SCAN_ANGLE",
        laz::DecompressionSelection::SCAN_ANGLE,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_USER_DATA",
        laz::DecompressionSelection::USER_DATA,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_POINT_SOURCE_ID",
        laz::DecompressionSelection::POINT_SOURCE_ID,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_GPS_TIME",
        laz::DecompressionSelection::GPS_TIME,
    )?;
    m.add("SELECTIVE_DECOMPRESS_RGB", laz::DecompressionSelection::RGB)?;
    m.add("SELECTIVE_DECOMPRESS_NIR", laz::DecompressionSelection::NIR)?;
    m.add(
        "SELECTIVE_DECOMPRESS_WAVEPACKET",
        laz::DecompressionSelection::WAVEPACKET,
    )?;
    m.add(
        "SELECTIVE_DECOMPRESS_ALL_EXTRA_BYTES",
        laz::DecompressionSelection::ALL_EXTRA_BYTES,
    )?;

    Ok(())
}
