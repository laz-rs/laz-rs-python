use pyo3::prelude::*;
use pyo3::types::{PyAny, PyType, PyBytes};
use pyo3::{create_exception, wrap_pyfunction};

use crate::adapters::{PyReadableFileObject, PyWriteableFileObject};

mod adapters;

create_exception!(pylaz, LazrsError, pyo3::exceptions::RuntimeError);

fn as_bytes<'a>(py: pyo3::Python, object: &'a pyo3::types::PyAny) -> PyResult<&'a [u8]> {
    let buffer = pyo3::buffer::PyBuffer::get(py, object)?;


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

    fn record_data(&self) -> PyResult<PyObject> {
        let mut data = std::io::Cursor::new(Vec::<u8>::new());
        self.vlr
            .write_to(&mut data)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))?;

        let gil = Python::acquire_gil();
        let py = gil.python();
        let bytes = PyBytes::new(py, data.get_ref())
            .to_object(py);
        Ok(bytes)
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
            .map_err(into_py_err)?,
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
        source: pyo3::PyObject,
        record_data: &pyo3::types::PyAny,
    ) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let vlr = laz::LazVlr::from_buffer(
            as_bytes(py, record_data)?
        )
        .map_err(into_py_err)?;
        let source = PyReadableFileObject::new(py, source)?;
        obj.init(Self {
            decompressor: laz::LasZipDecompressor::new(source, vlr).map_err(into_py_err)?,
        });
        Ok(())
    }

    pub fn decompress_many(&mut self, dest: &mut pyo3::types::PyAny) -> PyResult<()> {
        let slc = as_mut_bytes(pyo3::Python::acquire_gil().python(), dest)?;
        self.decompressor
            .decompress_many(slc)
            .map_err(|e| PyErr::new::<LazrsError, String>(format!("{}", e)))
    }

    pub fn  seek(&mut self, point_idx: u64) -> PyResult<()> {
        self.decompressor.seek(point_idx)
            .map_err(into_py_err)
    }

    pub fn vlr(&self) -> LazVlr {
        return LazVlr {
            vlr: self.decompressor.vlr().clone()
        }
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
            compressor: laz::LasZipCompressor::new(dest, vlr.vlr.clone())
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

    pub fn done(&mut self) -> PyResult<()> {
        self.compressor.done().map_err(into_py_err)
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
) -> PyResult<PyObject> {
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
    let gil = Python::acquire_gil();
    let py = gil.python();
    let bytes = PyBytes::new(py, compression_result.get_ref())
        .to_object(py);
    Ok(bytes)
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

#[pyfunction]
fn write_chunk_table(dest: pyo3::PyObject, chunk_table: Vec<usize>) -> pyo3::PyResult<()> {
    let mut dest = PyWriteableFileObject::new(dest)?;

    laz::write_chunk_table(&mut dest, &chunk_table)
        .map_err(into_py_err)
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
