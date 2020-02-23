use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::{create_exception, wrap_pyfunction};

create_exception!(pylaz, PyLazError, pyo3::exceptions::RuntimeError);

#[pyclass]
struct LazVlr {
    vlr: laz::LazVlr,
}

#[pymethods]
impl LazVlr {
    #[new]
    fn new(obj: &PyRawObject, record_data: &numpy::PyArray1<u8>) -> PyResult<()> {
        let vlr_data = record_data.as_slice()?;
        let vlr = laz::LazVlr::from_buffer(vlr_data)
            .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;
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
        .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;
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
        // TODO we could compute the size before to only have one alloc
        let mut data = std::io::Cursor::new(Vec::<u8>::new());
        self.vlr
            .write_to(&mut data)
            .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;

        let gil = pyo3::Python::acquire_gil();
        let np_array = numpy::PyArray1::<u8>::from_slice(gil.python(), data.get_ref().as_slice());
        Ok(np_array.to_owned())
    }
}

#[pyfunction]
fn decompress_points(
    compressed_points_data: &numpy::PyArray1<u8>,
    laszip_vlr_record_data: &numpy::PyArray1<u8>,
    decompression_output: &mut numpy::PyArray1<u8>,
    parallel: bool,
) -> PyResult<()> {
    let vlr_data = laszip_vlr_record_data.as_slice()?;
    let data_slc = compressed_points_data.as_slice()?;
    let output = decompression_output.as_slice_mut()?;
    if !parallel {
        let data_source = std::io::Cursor::new(data_slc);

        laz::LazVlr::from_buffer(vlr_data)
            .and_then(|vlr| laz::LasZipDecompressor::new(data_source, vlr))
            .and_then(|mut decompressor| {
                decompressor.decompress_many(output)?;
                Ok(())
            })
            .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;
    } else {
        laz::LazVlr::from_buffer(vlr_data)
            .and_then(|vlr| laz::las::laszip::par_decompress_buffer(data_slc, output, &vlr))
            .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;
    }
    Ok(())
}

#[pyfunction]
fn compress_points(
    laszip_vlr: &LazVlr,
    uncompressed_points: &numpy::PyArray1<u8>,
    parallel: bool,
) -> PyResult<Py<numpy::PyArray1<u8>>> {
    let mut compression_result = std::io::Cursor::new(Vec::<u8>::new());
    if !parallel {
        laz::las::laszip::compress_all(
            &mut compression_result,
            uncompressed_points.as_slice()?,
            laszip_vlr.vlr.clone(),
        )
        .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;
    } else {
        laz::las::laszip::par_compress_all(
            &mut compression_result,
            uncompressed_points.as_slice()?,
            &laszip_vlr.vlr,
        )
        .map_err(|e| PyErr::new::<PyLazError, String>(format!("{}", e)))?;
    }
    let gil = pyo3::Python::acquire_gil();
    let np_array =
        numpy::PyArray1::<u8>::from_slice(gil.python(), compression_result.get_ref().as_slice());
    Ok(np_array.to_owned())
}

/// This module is a python module implemented in Rust.
#[pymodule]
fn pylaz(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(decompress_points))?;
    m.add_wrapped(wrap_pyfunction!(compress_points))?;
    m.add("PyLazError", py.get_type::<PyLazError>())?;
    m.add_class::<LazVlr>()?;
    Ok(())
}
