use std::io::SeekFrom;

use pyo3::{IntoPy, PyResult, Python, ToPyObject};

fn py_seek_args_from_rust_seek(
    seek: SeekFrom,
    py: pyo3::Python,
) -> (pyo3::PyObject, pyo3::PyObject) {
    let io_module = py.import("io").unwrap();
    match seek {
        SeekFrom::Start(n) => {
            let value: pyo3::PyObject = n.into_py(py);
            (value, io_module.get("SEEK_SET").unwrap().to_object(py))
        }
        SeekFrom::End(n) => {
            let value: pyo3::PyObject = n.into_py(py);
            (value, io_module.get("SEEK_END").unwrap().to_object(py))
        }
        SeekFrom::Current(n) => {
            let value: pyo3::PyObject = n.into_py(py);
            (value, io_module.get("SEEK_CUR").unwrap().to_object(py))
        }
    }
}

pub struct PyWriteableFileObject {
    file_obj: pyo3::PyObject,
}

impl PyWriteableFileObject {
    pub fn new(file_obj: pyo3::PyObject) -> PyResult<Self> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        file_obj.getattr(py, "write")?;
        file_obj.getattr(py, "seek")?;

        Ok(Self { file_obj })
    }
}

impl std::io::Write for PyWriteableFileObject {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let gil = pyo3::Python::acquire_gil();
        let py_bytes = pyo3::types::PyBytes::new(gil.python(), buf);
        self.file_obj
            .call_method1(gil.python(), "write", (py_bytes.to_object(gil.python()),))
            .and_then(|ret_val| ret_val.extract::<usize>(gil.python()))
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", err)))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let gil = pyo3::Python::acquire_gil();
        self.file_obj.call_method0(gil.python(), "flush").unwrap();
        Ok(())
    }
}

impl std::io::Seek for PyWriteableFileObject {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let args = py_seek_args_from_rust_seek(pos, py);
        let new_pos = self
            .file_obj
            .call_method(py, "seek", args, None)
            .unwrap()
            .cast_as::<pyo3::types::PyLong>(py)
            .unwrap()
            .extract()
            .unwrap();
        Ok(new_pos)
    }
}

pub struct PyReadableFileObject {
    read_fn: pyo3::PyObject,
    seek_fn: pyo3::PyObject,
}

impl PyReadableFileObject {
    pub fn new(py: pyo3::Python, object: pyo3::PyObject) -> PyResult<Self> {
        let read_fn = object.getattr(py, "read")?;
        let seek_fn = object.getattr(py, "seek")?;
        Ok(Self { read_fn, seek_fn })
    }
}

impl std::io::Read for PyReadableFileObject {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let num_bytes_to_read: pyo3::PyObject = buf.len().into_py(py);
        let ret_obj = match self.read_fn.call(py, (num_bytes_to_read,), None) {
            Ok(ret_val) => ret_val,
            Err(err) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("{:?}", err),
                ));
            }
        };

        match ret_obj.cast_as::<pyo3::types::PyBytes>(py) {
            Ok(bytes) => {
                buf.copy_from_slice(bytes.as_bytes());
                Ok(bytes.as_bytes().len())
            }
            Err(err) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", err),
            )),
        }
    }
}

impl std::io::Seek for PyReadableFileObject {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let args = py_seek_args_from_rust_seek(pos, py);
        let new_pos = self
            .seek_fn
            .call(py, args, None)
            .expect("Failed to call seek")
            .cast_as::<pyo3::types::PyLong>(py)
            .expect("Failed to cast to pylong")
            .extract()
            .expect("Failed to cast to u64zz");
        Ok(new_pos)
    }
}

