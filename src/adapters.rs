use std::io::SeekFrom;
use std::os::raw::c_char;

use pyo3::ffi::Py_ssize_t;
use pyo3::types::{PyAnyMethods, PyBytesMethods};
use pyo3::{IntoPyObject, PyAny, PyResult, Python};

fn to_other_io_error(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
}

fn py_seek_args_from_rust_seek(
    seek: SeekFrom,
    py: pyo3::Python,
) -> (pyo3::Py<PyAny>, pyo3::Py<PyAny>) {
    let io_module = py.import("io").unwrap();
    match seek {
        SeekFrom::Start(n) => {
            let value: pyo3::Py<PyAny> = n.into_pyobject(py).unwrap().into_any().unbind();
            (
                value,
                io_module
                    .getattr("SEEK_SET")
                    .unwrap()
                    .into_pyobject(py)
                    .unwrap()
                    .unbind(),
            )
        }
        SeekFrom::End(n) => {
            let value: pyo3::Py<PyAny> = n.into_pyobject(py).unwrap().into_any().unbind();
            (
                value,
                io_module
                    .getattr("SEEK_END")
                    .unwrap()
                    .into_pyobject(py)
                    .unwrap()
                    .unbind(),
            )
        }
        SeekFrom::Current(n) => {
            let value: pyo3::Py<PyAny> = n.into_pyobject(py).unwrap().into_any().unbind();
            (
                value,
                io_module
                    .getattr("SEEK_CUR")
                    .unwrap()
                    .into_pyobject(py)
                    .unwrap()
                    .unbind(),
            )
        }
    }
}

#[derive(Clone)]
pub(crate) struct PyFileObject {
    file_obj: pyo3::Py<PyAny>,
    write_fn: Option<pyo3::Py<PyAny>>,
    read_fn: Option<pyo3::Py<PyAny>>,
    readinto_fn: Option<pyo3::Py<PyAny>>,
}

impl PyFileObject {
    pub(crate) fn new(py: pyo3::Python, file_obj: pyo3::Py<PyAny>) -> PyResult<Self> {
        let write_fn = file_obj.getattr(py, "write").ok();
        let read_fn = file_obj.getattr(py, "read").ok();
        let readinto_fn = file_obj.getattr(py, "readinto").ok();

        Ok(Self {
            file_obj,
            write_fn,
            read_fn,
            readinto_fn,
        })
    }
}

impl std::io::Read for PyFileObject {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Python::attach(|py| {
            if let Some(ref readinto) = self.readinto_fn {
                let memview = unsafe {
                    let view_object = pyo3::ffi::PyMemoryView_FromMemory(
                        buf.as_mut_ptr() as *mut c_char,
                        buf.len() as Py_ssize_t,
                        pyo3::ffi::PyBUF_WRITE,
                    );

                    pyo3::Py::<PyAny>::from_owned_ptr(py, view_object)
                };
                readinto
                    .call1(py, (memview,))
                    .and_then(|num_bytes_read| num_bytes_read.extract::<usize>(py))
                    .map_err(|_err| {
                        to_other_io_error("Failed to use readinto to read bytes".to_string())
                    })
            } else {
                let num_bytes_to_read: pyo3::Py<PyAny> =
                    buf.len().into_pyobject(py).unwrap().into_any().unbind();

                let object = self
                    .read_fn
                    .as_ref()
                    .ok_or_else(|| to_other_io_error("No read method on file object".to_string()))?
                    .call1(py, (num_bytes_to_read,))
                    .map_err(|_err| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Failed to call read".to_string(),
                        )
                    })?;

                match object.downcast_bound::<pyo3::types::PyBytes>(py) {
                    Ok(py_bytes) => {
                        let read_bytes = py_bytes.as_bytes();
                        let shortest = std::cmp::min(buf.len(), read_bytes.len());
                        buf[..shortest].copy_from_slice(read_bytes);
                        Ok(read_bytes.len())
                    }
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "read did not return bytes".to_string(),
                    )),
                }
            }
        })
    }
}

impl std::io::Write for PyFileObject {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Python::attach(|py| {
            let memview = unsafe {
                let view_object = pyo3::ffi::PyMemoryView_FromMemory(
                    buf.as_ptr() as *mut c_char,
                    buf.len() as Py_ssize_t,
                    pyo3::ffi::PyBUF_READ,
                );

                pyo3::Py::<PyAny>::from_owned_ptr(py, view_object)
            };

            self.write_fn
                .as_ref()
                .ok_or_else(|| to_other_io_error("Ne read method on file object".to_string()))?
                .call1(py, (memview,))
                .and_then(|ret_val| ret_val.extract::<usize>(py))
                .map_err(|_err| to_other_io_error("Failed to call write".to_string()))
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Python::attach(|py| {
            self.file_obj
                .call_method0(py, "flush")
                .map_err(|_err| to_other_io_error("Failed to call flush".to_string()))?;
            Ok(())
        })
    }
}

impl std::io::Seek for PyFileObject {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        Python::attach(|py| {
            let args = py_seek_args_from_rust_seek(pos, py);
            let new_pos = self
                .file_obj
                .call_method(py, "seek", args, None)
                .and_then(|py_long| py_long.extract::<u64>(py))
                .map_err(|_err| to_other_io_error("Failed to call seek".to_string()))?;
            Ok(new_pos)
        })
    }
}

pub struct BufReadWritePyFileObject {
    input: std::io::BufReader<PyFileObject>,
    output: std::io::BufWriter<PyFileObject>,
}

impl BufReadWritePyFileObject {
    pub(crate) fn new(file: PyFileObject) -> Self {
        let input = std::io::BufReader::new(file.clone());
        let output = std::io::BufWriter::new(file);

        Self { input, output }
    }
}

impl std::io::Read for BufReadWritePyFileObject {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.input.read(buf)
    }
}

impl std::io::Write for BufReadWritePyFileObject {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.output.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

impl std::io::Seek for BufReadWritePyFileObject {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // We have to get the absolute pos after the first seek
        // and use SeekFrom::Start for the second seek because if the orginal
        // pos is a SeekFrom::Current, we are actually going to seek twice that
        let pos = self.output.seek(pos)?;
        self.input.seek(SeekFrom::Start(pos))
    }
}
