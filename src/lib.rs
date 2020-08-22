use pyo3::prelude::*;
use pyo3::types::{PyAny, PyType};
use pyo3::{create_exception, wrap_pyfunction};

use crate::adapters::{PyReadableFileObject, PyWriteableFileObject};

mod adapters;

create_exception!(pylaz, LazrsError, pyo3::exceptions::RuntimeError);

fn as_bytes<'a>(py: pyo3::Python, object: &'a pyo3::types::PyAny) -> PyResult<&'a [u8]> {
    let buffer = pyo3::buffer::PyBuffer::get(py, object)?;

    assert_eq!(buffer.item_size(), std::mem::size_of::<u8>()); // is this truly mandatory ?

    let slc =
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const u8, buffer.len_bytes()) };

    return Ok(slc);
}

fn as_mut_bytes<'a>(py: pyo3::Python, object: &'a pyo3::types::PyAny) -> PyResult<&'a mut [u8]> {
    let buffer = pyo3::buffer::PyBuffer::get(py, object)?;

    if buffer.readonly() {
        return Err(PyErr::new::<pyo3::exceptions::TypeError, _>(
            "buffer is readonly",
        ));
    }

    assert_eq!(buffer.item_size(), std::mem::size_of::<u8>()); // is this truly mandatory ?
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
    fn new(obj: &PyRawObject, record_data: &PyAny) -> PyResult<()> {
        let vlr_data = as_bytes(Python::acquire_gil().python(), record_data)?;
        let vlr = laz::LazVlr::from_buffer(vlr_data).map_err(into_py_err)?;
        obj.init(LazVlr { vlr });
        Ok(())
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

    fn record_data(&self) -> PyResult<Py<numpy::PyArray1<u8>>> {
        let mut data = std::io::Cursor::new(Vec::<u8>::new());
        self.vlr
            .write_to(&mut data)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;

        let gil = pyo3::Python::acquire_gil();
        let np_array = numpy::PyArray1::<u8>::from_slice(gil.python(), data.get_ref().as_slice());
        Ok(np_array.to_owned())
    }
}

#[pyclass]
struct ParLasZipCompressor {
    compressor: laz::ParLasZipCompressor<PyWriteableFileObject>,
}

#[pymethods]
impl ParLasZipCompressor {
    #[new]
    fn new(obj: &PyRawObject, dest: PyObject, vlr: &LazVlr) -> PyResult<()> {
        obj.init({
            ParLasZipCompressor {
                compressor: laz::ParLasZipCompressor::new(
                    PyWriteableFileObject::new(dest)?,
                    vlr.vlr.clone(),
                )
                .map_err(into_py_err)?,
            }
        });
        Ok(())
    }

    fn compress_many(&mut self, points: &PyAny) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let point_bytes = as_bytes(py, points)?;

        self.compressor
            .compress_many(point_bytes)
            .map_err(into_py_err)
    }

    fn done(&mut self) -> PyResult<()> {
        self.compressor.done().map_err(into_py_err)
    }
}

#[pyclass]
struct ParLasZipDecompressor {
    decompressor: laz::ParLasZipDecompressor<PyReadableFileObject>,
}

#[pymethods]
impl ParLasZipDecompressor {
    #[new]
    fn new(obj: &PyRawObject, source: PyObject, vlr_record_data: &PyAny) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let vlr = laz::LazVlr::from_buffer(as_bytes(gil.python(), vlr_record_data)?)
            .map_err(into_py_err)?;
        obj.init(ParLasZipDecompressor {
            decompressor: laz::ParLasZipDecompressor::<PyReadableFileObject>::new(
                PyReadableFileObject::new(gil.python(), source)?,
                vlr,
            )
            .unwrap(),
        });
        Ok(())
    }

    fn decompress_many(&mut self, points: &PyAny) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let points = as_mut_bytes(gil.python(), points)?;
        self.decompressor
            .decompress_many(points)
            .map_err(into_py_err)?;
        Ok(())
    }
}

#[pyclass]
struct LasZipDecompressor {
    decompressor: laz::LasZipDecompressor<'static, PyReadableFileObject>,
}

#[pymethods]
impl LasZipDecompressor {
    #[new]
    pub fn new(
        obj: &PyRawObject,
        record_data: pyo3::PyObject,
        source: pyo3::PyObject,
    ) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let vlr = laz::LazVlr::from_buffer(
            record_data
                .cast_as::<pyo3::types::PyBytes>(py)
                .expect("Object is not byte object")
                .as_bytes(),
        )
        .map_err(into_py_err)?;
        let source = PyReadableFileObject::new(py, source)?;
        obj.init(Self {
            decompressor: laz::LasZipDecompressor::new(source, vlr).map_err(into_py_err)?,
        });
        Ok(())
    }

    pub fn decompress_many(&mut self, dest: &mut numpy::PyArray1<u8>) -> PyResult<()> {
        let slc = dest.as_slice_mut()?;
        self.decompressor
            .decompress_many(slc)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))
    }
}

#[pyclass]
struct LasZipCompressor {
    compressor: laz::LasZipCompressor<'static, PyWriteableFileObject>,
}

#[pymethods]
impl LasZipCompressor {
    #[new]
    pub fn new(obj: &PyRawObject, dest: pyo3::PyObject, vlr: &LazVlr) -> PyResult<()> {
        let dest = PyWriteableFileObject::new(dest)?;
        obj.init(Self {
            compressor: laz::LasZipCompressor::from_laz_vlr(dest, vlr.vlr.clone())
                .map_err(into_py_err)?,
        });
        Ok(())
    }

    pub fn compress_many(&mut self, points: &PyAny) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        self.compressor
            .compress_many(as_bytes(py, points)?)
            .map_err(into_py_err)
    }
}

#[pyfunction]
fn decompress_points(
    compressed_points_data: &PyAny,
    laszip_vlr_record_data: &PyAny,
    decompression_output: &PyAny,
    parallel: bool,
) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let vlr_data = as_bytes(gil.python(), laszip_vlr_record_data)?;
    let data_slc = as_bytes(gil.python(), compressed_points_data)?;
    let output = as_mut_bytes(gil.python(), decompression_output)?;

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
) -> PyResult<Py<numpy::PyArray1<u8>>> {
    let mut compression_result = std::io::Cursor::new(Vec::<u8>::new());
    if !parallel {
        laz::compress_buffer(
            &mut compression_result,
            as_bytes(Python::acquire_gil().python(), uncompressed_points)?,
            laszip_vlr.vlr.clone(),
        )
        .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;
    } else {
        laz::par_compress_buffer(
            &mut compression_result,
            as_bytes(Python::acquire_gil().python(), uncompressed_points)?,
            &laszip_vlr.vlr,
        )
        .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;
    }
    let gil = pyo3::Python::acquire_gil();
    let np_array =
        numpy::PyArray1::<u8>::from_slice(gil.python(), compression_result.get_ref().as_slice());
    Ok(np_array.to_owned())
}

#[pyfunction]
fn read_chunk_table(source: pyo3::PyObject) -> pyo3::PyResult<pyo3::PyObject> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let mut src = PyReadableFileObject::new(py, source)?;

    match laz::read_chunk_table(&mut src) {
        None => PyResult::Ok(py.None()),
        Some(Ok(chunk_table)) => {
            let list: pyo3::PyObject = chunk_table.into_py(py);
            PyResult::Ok(list)
        }
        Some(Err(err)) => PyResult::Err(PyErr::new::<LazrsError, String>(format!("{}", err))),
    }
}

/// This module is a python module implemented in Rust.
#[pymodule]
fn lazrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(decompress_points))?;
    m.add_wrapped(wrap_pyfunction!(compress_points))?;
    m.add_wrapped(wrap_pyfunction!(read_chunk_table))?;
    m.add("LazrsError", py.get_type::<LazrsError>())?;
    m.add_class::<LazVlr>()?;
    m.add_class::<LasZipDecompressor>()?;
    m.add_class::<ParLasZipCompressor>()?;
    m.add_class::<ParLasZipDecompressor>()?;
    Ok(())
}
