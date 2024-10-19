use datafusion::arrow::array::*;
use datafusion_functions_nested::greatest::greatest_inner;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyfunction]
fn run_greatest_query(input: Vec<Vec<i32>>) -> PyResult<Vec<i32>> {
    // Convert input to ArrayRefs
    let arrays: Vec<ArrayRef> = input
        .iter()
        .map(|col| Arc::new(Int32Array::from(col.clone())) as ArrayRef)
        .collect();

    // Compute greatest
    let greatest_array = greatest_inner(&arrays)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Downcast to Int32Array
    let greatest_int_array = greatest_array
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to downcast to Int32Array")
        })?;

    // Collect results
    let mut result = Vec::new();
    for i in 0..greatest_int_array.len() {
        if greatest_int_array.is_null(i) {
            result.push(0); // Or handle nulls appropriately
        } else {
            result.push(greatest_int_array.value(i));
        }
    }

    Ok(result)
}

#[pymodule]
fn datafusion_greatest(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_greatest_query, m)?)?;
    Ok(())
}
