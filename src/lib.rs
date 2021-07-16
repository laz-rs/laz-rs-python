use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyType};
use pyo3::{create_exception, wrap_pyfunction};

use crate::adapters::{PyReadableFileObject, PyWriteableFileObject};
use std::io::{BufReader, BufWriter, Write};

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
        let vlr = laz::LazVlr::from_buffer(vlr_data).map_err(into_py_err)?;
        Ok(LazVlr { vlr })
    }

    #[classmethod]
    fn new_for_compression(
        _cls: &PyType,
        point_format_id: u8,
        num_extra_bytes: u16,
    ) -> PyResult<Self> {
        let items = laz::LazItemRecordBuilder::default_for_point_format_id(
            point_format_id,
            num_extra_bytes,
        )
        .map_err(into_py_err)?;
        let vlr = laz::LazVlr::from_laz_items(items);
        Ok(LazVlr { vlr })
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
        let vlr = laz::LazVlr::from_buffer(as_bytes(vlr_record_data)?).map_err(into_py_err)?;
        let source = BufReader::new(PyReadableFileObject::new(gil.python(), source)?);
        Ok(ParLasZipDecompressor {
            decompressor: laz::ParLasZipDecompressor::<_>::new(source, vlr).map_err(into_py_err)?,
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
        let vlr = laz::LazVlr::from_buffer(as_bytes(record_data)?).map_err(into_py_err)?;
        let source = BufReader::new(PyReadableFileObject::new(gil.python(), source)?);
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

    laz::LazVlr::from_buffer(vlr_data)
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

#[pyfunction]
fn read_chunk_table(source: pyo3::PyObject) -> pyo3::PyResult<pyo3::PyObject> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let mut src = BufReader::new(PyReadableFileObject::new(py, source)?);

    match laz::read_chunk_table(&mut src) {
        None => PyResult::Ok(py.None()),
        Some(Ok(chunk_table)) => {
            let list: pyo3::PyObject = chunk_table.into_py(py);
            PyResult::Ok(list)
        }
        Some(Err(err)) => PyResult::Err(PyErr::new::<LazrsError, String>(format!("{}", err))),
    }
}

#[pyfunction]
fn write_chunk_table(dest: pyo3::PyObject, chunk_table: Vec<usize>) -> pyo3::PyResult<()> {
    let mut dest = BufWriter::new(PyWriteableFileObject::new(dest)?);

    laz::write_chunk_table(&mut dest, &chunk_table).map_err(into_py_err)
}

/// This module is a python module implemented in Rust.
#[pymodule]
fn lazrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(decompress_points))?;
    m.add_wrapped(wrap_pyfunction!(compress_points))?;
    m.add_wrapped(wrap_pyfunction!(read_chunk_table))?;
    m.add_wrapped(wrap_pyfunction!(write_chunk_table))?;
    m.add("LazrsError", py.get_type::<LazrsError>())?;
    m.add_class::<LazVlr>()?;
    m.add_class::<LasZipDecompressor>()?;
    m.add_class::<LasZipCompressor>()?;
    m.add_class::<ParLasZipCompressor>()?;
    m.add_class::<ParLasZipDecompressor>()?;
    Ok(())
}
