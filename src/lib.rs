use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyList, PyType};
use pyo3::{create_exception, wrap_pyfunction};

use crate::adapters::{PyReadableFileObject, PyWriteableFileObject};
use std::io::{BufReader, BufWriter, Read, Write};

mod adapters;

create_exception!(pylaz, LazrsError, pyo3::exceptions::PyRuntimeError);

fn as_bytes(object: &PyAny) -> PyResult<&[u8]> {
    let buffer = pyo3::buffer::PyBuffer::<u8>::get(object)?;

    let slc =
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const u8, buffer.len_bytes()) };

    return Ok(slc);
}

fn as_mut_bytes(object: &PyAny) -> PyResult<&mut [u8]> {
    let buffer = pyo3::buffer::PyBuffer::<u8>::get(object)?;

    if buffer.readonly() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "buffer is readonly",
        ));
    }

    let slc =
        unsafe { std::slice::from_raw_parts_mut(buffer.buf_ptr() as *mut u8, buffer.len_bytes()) };

    return Ok(slc);
}

fn into_py_err<T: std::fmt::Display>(error: T) -> PyErr {
    PyErr::new::<LazrsError, _>(format!("{}", error))
}

#[pyclass]
struct LazVlr {
    vlr: laz::LazVlr,
}

#[pymethods]
impl LazVlr {
    #[new]
    fn new(record_data: &PyAny) -> PyResult<Self> {
        let vlr_data = as_bytes(record_data)?;
        let vlr = laz::LazVlr::read_from(vlr_data).map_err(into_py_err)?;
        Ok(LazVlr { vlr })
    }

    #[classmethod]
    fn new_for_compression(
        _cls: &PyType,
        point_format_id: u8,
        num_extra_bytes: u16,
        use_variable_size_chunks: Option<bool>,
    ) -> PyResult<Self> {
        let mut builder = laz::LazVlrBuilder::default()
            .with_point_format(point_format_id, num_extra_bytes)
            .map_err(into_py_err)?;

        if use_variable_size_chunks.unwrap_or(false) {
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

    fn record_data(&self) -> PyResult<PyObject> {
        let mut data = std::io::Cursor::new(Vec::<u8>::new());
        self.vlr
            .write_to(&mut data)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;

        let gil = Python::acquire_gil();
        let py = gil.python();
        let bytes = PyBytes::new(py, data.get_ref()).to_object(py);
        Ok(bytes)
    }
}

#[pyclass]
struct ParLasZipCompressor {
    compressor: laz::ParLasZipCompressor<BufWriter<PyWriteableFileObject>>,
}

#[pymethods]
impl ParLasZipCompressor {
    #[new]
    fn new(dest: PyObject, vlr: &LazVlr) -> PyResult<Self> {
        let dest = BufWriter::new(PyWriteableFileObject::new(dest)?);
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

    fn compress_many(&mut self, points: &PyAny) -> PyResult<()> {
        let point_bytes = as_bytes(points)?;

        self.compressor
            .compress_many(point_bytes)
            .map_err(into_py_err)
    }

    pub fn compress_chunks(&mut self, chunks: &PyList) -> PyResult<()> {
        let chunks = chunks
            .iter()
            .map(as_bytes)
            .collect::<PyResult<Vec<&[u8]>>>()?;
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
    decompressor: laz::ParLasZipDecompressor<BufReader<PyReadableFileObject>>,
}

#[pymethods]
impl ParLasZipDecompressor {
    #[new]
    fn new(source: PyObject, vlr_record_data: &PyAny) -> PyResult<Self> {
        let gil = Python::acquire_gil();
        let source = BufReader::new(PyReadableFileObject::new(gil.python(), source)?);
        let vlr = laz::LazVlr::read_from(as_bytes(vlr_record_data)?).map_err(into_py_err)?;
        Ok(ParLasZipDecompressor {
            decompressor: laz::ParLasZipDecompressor::new(source, vlr).map_err(into_py_err)?,
        })
    }

    fn decompress_many(&mut self, points: &PyAny) -> PyResult<()> {
        let points = as_mut_bytes(points)?;
        self.decompressor
            .decompress_many(points)
            .map_err(into_py_err)?;
        Ok(())
    }

    pub fn seek(&mut self, point_idx: u64) -> PyResult<()> {
        self.decompressor.seek(point_idx).map_err(into_py_err)
    }

    pub fn read_raw_bytes_into(&mut self, bytes: &PyAny) -> PyResult<()> {
        let slc = as_mut_bytes(bytes)?;
        self.decompressor.get_mut().read_exact(slc).map_err(into_py_err)
    }
}

#[pyclass]
struct LasZipDecompressor {
    decompressor: laz::LasZipDecompressor<'static, BufReader<PyReadableFileObject>>,
}

#[pymethods]
impl LasZipDecompressor {
    #[new]
    pub fn new(source: pyo3::PyObject, record_data: &pyo3::types::PyAny) -> PyResult<Self> {
        let gil = Python::acquire_gil();
        let source = BufReader::new(PyReadableFileObject::new(gil.python(), source)?);
        let vlr = laz::LazVlr::read_from(as_bytes(record_data)?).map_err(into_py_err)?;
        Ok(Self {
            decompressor: laz::LasZipDecompressor::new(source, vlr).map_err(into_py_err)?,
        })
    }

    pub fn decompress_many(&mut self, dest: &PyAny) -> PyResult<()> {
        let slc = as_mut_bytes(dest)?;
        self.decompressor
            .decompress_many(slc)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))
    }

    pub fn seek(&mut self, point_idx: u64) -> PyResult<()> {
        self.decompressor.seek(point_idx).map_err(into_py_err)
    }

    pub fn vlr(&self) -> LazVlr {
        return LazVlr {
            vlr: self.decompressor.vlr().clone(),
        };
    }

    // See the documentation of the free function with the same name.
    // it has the same requirements.
    pub fn read_chunk_table_only(&mut self) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let uses_variable_chunk_size = self.decompressor.vlr().uses_variable_size_chunks();
        let chunk_table =
            laz::laszip::ChunkTable::read(
                self.decompressor.get_mut(),
                uses_variable_chunk_size)
                .map_err(into_py_err)?;
        let elements = chunk_table
            .as_ref()
            .iter()
            .map(|entry| (entry.point_count, entry.byte_count));
        let list = PyList::new(py, elements);
        Ok(list.to_object(py))
    }


    pub fn read_raw_bytes_into(&mut self, bytes: &PyAny) -> PyResult<()> {
        let slc = as_mut_bytes(bytes)?;
        self.decompressor.get_mut().read_exact(slc).map_err(into_py_err)
    }
}

#[pyclass]
struct LasZipCompressor {
    compressor: laz::LasZipCompressor<'static, BufWriter<PyWriteableFileObject>>,
}

#[pymethods]
impl LasZipCompressor {
    #[new]
    pub fn new(dest: pyo3::PyObject, vlr: &LazVlr) -> PyResult<Self> {
        let dest = BufWriter::new(PyWriteableFileObject::new(dest)?);
        let compressor = laz::LasZipCompressor::new(dest, vlr.vlr.clone()).map_err(into_py_err)?;
        Ok(Self { compressor })
    }

    pub fn reserve_offset_to_chunk_table(&mut self) -> PyResult<()> {
        self.compressor
            .reserve_offset_to_chunk_table()
            .map_err(into_py_err)?;
        self.compressor.get_mut().flush().map_err(into_py_err)
    }

    pub fn compress_many(&mut self, points: &PyAny) -> PyResult<()> {
        self.compressor
            .compress_many(as_bytes(points)?)
            .map_err(into_py_err)
    }

    pub fn done(&mut self) -> PyResult<()> {
        self.compressor.done().map_err(into_py_err)?;
        self.compressor.get_mut().flush().map_err(into_py_err)
    }

    pub fn compress_chunks(&mut self, chunks: &PyList) -> PyResult<()> {
        for chunk in chunks.iter() {
            self.compress_many(chunk)?;
            self.finish_current_chunk()?;
        }
        Ok(())
    }

    pub fn finish_current_chunk(&mut self) -> PyResult<()> {
        self.compressor.finish_current_chunk().map_err(into_py_err)
    }
}

#[pyfunction]
fn decompress_points(
    compressed_points_data: &PyAny,
    laszip_vlr_record_data: &PyAny,
    decompression_output: &PyAny,
    parallel: bool,
) -> PyResult<()> {
    let vlr_data = as_bytes(laszip_vlr_record_data)?;
    let data_slc = as_bytes(compressed_points_data)?;
    let output = as_mut_bytes(decompression_output)?;

    laz::LazVlr::read_from(vlr_data)
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
fn decompress_points_with_chunk_table(
    compressed_points_data: &PyAny,
    laszip_vlr_record_data: &PyAny,
    decompression_output: &PyAny,
    py_chunk_table: &PyList,
) -> PyResult<()> {
    let vlr_data = as_bytes(laszip_vlr_record_data)?;
    let data_slc = as_bytes(compressed_points_data)?;
    let output = as_mut_bytes(decompression_output)?;
    let chunk_table = chunk_table_from_py_list(py_chunk_table)?;

    laz::LazVlr::read_from(vlr_data)
        .and_then(|vlr| {
            // TODO
            laz::par_decompress(data_slc, output, &vlr, chunk_table.as_ref())
        })
        .map_err(into_py_err)?;

    Ok(())
}

#[pyfunction]
fn compress_points(
    laszip_vlr: &LazVlr,
    uncompressed_points: &PyAny,
    parallel: bool,
) -> PyResult<PyObject> {
    let mut compression_result = std::io::Cursor::new(Vec::<u8>::new());
    if !parallel {
        laz::compress_buffer(
            &mut compression_result,
            as_bytes(uncompressed_points)?,
            laszip_vlr.vlr.clone(),
        )
        .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;
    } else {
        laz::par_compress_buffer(
            &mut compression_result,
            as_bytes(uncompressed_points)?,
            &laszip_vlr.vlr,
        )
        .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;
    }
    let gil = Python::acquire_gil();
    let py = gil.python();
    let bytes = PyBytes::new(py, compression_result.get_ref()).to_object(py);
    Ok(bytes)
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
fn read_chunk_table(source: pyo3::PyObject, vlr: &LazVlr) -> pyo3::PyResult<pyo3::PyObject> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let mut src = BufReader::new(PyReadableFileObject::new(py, source)?);

    let chunk_table =
        laz::laszip::ChunkTable::read_from(&mut src, &vlr.vlr).map_err(into_py_err)?;
    let elements = chunk_table
        .as_ref()
        .iter()
        .map(|entry| (entry.point_count, entry.byte_count));
    let list = pyo3::types::PyList::new(py, elements);
    Ok(list.to_object(py))
}

/// This reads the chunks table.
///
/// This simply reads the chunk table, it *does not* read offset nor seeks
/// *nor* does it puts the source position to the actual start of points data
/// afterwards.
///
/// The `source` position **must** be at the beginning of the chunk table
#[pyfunction]
fn read_chunk_table_only(source: pyo3::PyObject, vlr: &LazVlr) -> pyo3::PyResult<pyo3::PyObject> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let mut src = BufReader::new(PyReadableFileObject::new(py, source)?);

    let chunk_table =
        laz::laszip::ChunkTable::read(&mut src, vlr.uses_variable_size_chunks()).map_err(into_py_err)?;
    let elements = chunk_table
        .as_ref()
        .iter()
        .map(|entry| (entry.point_count, entry.byte_count));
    let list = pyo3::types::PyList::new(py, elements);
    Ok(list.to_object(py))
}

fn chunk_table_from_py_list(py_chunk_table: &PyList) -> PyResult<laz::laszip::ChunkTable> {
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
fn write_chunk_table(
    dest: pyo3::PyObject,
    py_chunk_table: &PyList,
    vlr: &LazVlr,
) -> pyo3::PyResult<()> {
    let chunk_table = chunk_table_from_py_list(py_chunk_table)?;

    let dest = BufWriter::new(PyWriteableFileObject::new(dest)?);
    chunk_table.write_to(dest, &vlr.vlr).map_err(into_py_err)
}

/// This module is a python module implemented in Rust.
#[pymodule]
fn lazrs(py: Python, m: &PyModule) -> PyResult<()> {
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
    m.add_class::<ParLasZipCompressor>()?;
    m.add_class::<ParLasZipDecompressor>()?;
    Ok(())
}
