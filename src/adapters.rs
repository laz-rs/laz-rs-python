use std::io::SeekFrom;

use pyo3::ffi::Py_ssize_t;
use pyo3::PyObject;
use pyo3::{IntoPy, PyResult, Python, ToPyObject};
use std::os::raw::c_char;

fn to_other_io_error(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
}

fn seek_file_object(file_object: &PyObject, pos: SeekFrom) -> std::io::Result<u64> {
    Python::with_gil(|py| {
        let args = py_seek_args_from_rust_seek(pos, py);
        let new_pos = file_object
            .call_method(py, "seek", args, None)
            .and_then(|py_long| py_long.extract::<u64>(py))
            .map_err(|_err| to_other_io_error(format!("Failed to call seek")))?;
        Ok(new_pos)
    })
}

fn py_seek_args_from_rust_seek(
    seek: SeekFrom,
    py: pyo3::Python,
) -> (pyo3::PyObject, pyo3::PyObject) {
    let io_module = py.import("io").unwrap();
    match seek {
        SeekFrom::Start(n) => {
            let value: pyo3::PyObject = n.into_py(py);
            (value, io_module.getattr("SEEK_SET").unwrap().to_object(py))
        }
        SeekFrom::End(n) => {
            let value: pyo3::PyObject = n.into_py(py);
            (value, io_module.getattr("SEEK_END").unwrap().to_object(py))
        }
        SeekFrom::Current(n) => {
            let value: pyo3::PyObject = n.into_py(py);
            (value, io_module.getattr("SEEK_CUR").unwrap().to_object(py))
        }
    }
}

pub struct PyWriteableFileObject {
    file_obj: pyo3::PyObject,
    write_fn: pyo3::PyObject,
}

impl PyWriteableFileObject {
    pub fn new(file_obj: pyo3::PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            let write_fn = file_obj.getattr(py, "write")?;

            Ok(Self { file_obj, write_fn })
        })
    }
}

impl std::io::Write for PyWriteableFileObject {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Python::with_gil(|py| {
            let memview = unsafe {
                let view_object = pyo3::ffi::PyMemoryView_FromMemory(
                    buf.as_ptr() as *mut c_char,
                    buf.len() as Py_ssize_t,
                    pyo3::ffi::PyBUF_READ,
                );

                pyo3::PyObject::from_owned_ptr(py, view_object)
            };

            self.write_fn
                .call1(py, (memview,))
                .and_then(|ret_val| ret_val.extract::<usize>(py))
                .map_err(|_err| to_other_io_error(format!("Failed to call write")))
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Python::with_gil(|py| {
            self.file_obj
                .call_method0(py, "flush")
                .map_err(|_err| to_other_io_error(format!("Failed to call flush")))?;
            Ok(())
        })
    }
}

impl std::io::Seek for PyWriteableFileObject {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        seek_file_object(&self.file_obj, pos)
    }
}

pub struct PyReadableFileObject {
    file_object: pyo3::PyObject,
    read_fn: pyo3::PyObject,
    readinto_fn: Option<pyo3::PyObject>,
}

impl PyReadableFileObject {
    pub fn new(py: pyo3::Python, object: pyo3::PyObject) -> PyResult<Self> {
        let read_fn = object.getattr(py, "read")?;
        let readinto_fn = object.getattr(py, "readinto").ok();
        Ok(Self {
            file_object: object,
            read_fn,
            readinto_fn,
        })
    }
}

impl std::io::Read for PyReadableFileObject {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Python::with_gil(|py| {
            if let Some(ref readinto) = self.readinto_fn {
                let memview = unsafe {
                    let view_object = pyo3::ffi::PyMemoryView_FromMemory(
                        buf.as_mut_ptr() as *mut c_char,
                        buf.len() as Py_ssize_t,
                        pyo3::ffi::PyBUF_WRITE,
                    );

                    pyo3::PyObject::from_owned_ptr(py, view_object)
                };
                readinto
                    .call1(py, (memview,))
                    .and_then(|num_bytes_read| num_bytes_read.extract::<usize>(py))
                    .map_err(|_err| {
                        to_other_io_error(format!("Failed to use readinto to read bytes"))
                    })
            } else {
                let num_bytes_to_read: pyo3::PyObject = buf.len().into_py(py);

                let object = self
                    .read_fn
                    .call1(py, (num_bytes_to_read,))
                    .map_err(|_err| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to call read"),
                        )
                    })?;

                match object.downcast::<pyo3::types::PyBytes>(py) {
                    Ok(py_bytes) => {
                        let read_bytes = py_bytes.as_bytes();
                        let shortest = std::cmp::min(buf.len(), read_bytes.len());
                        buf[..shortest].copy_from_slice(read_bytes);
                        Ok(read_bytes.len())
                    }
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("read did not return bytes"),
                    )),
                }
            }
        })
    }
}

impl std::io::Seek for PyReadableFileObject {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        seek_file_object(&self.file_object, pos)
    }
}
