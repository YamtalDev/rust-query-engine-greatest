use datafusion::arrow::array::*;
use datafusion_functions_nested::greatest::greatest_inner;
use pyo3::prelude::*;
use std::sync::Arc;

/// PyO3 wrapper function to execute the greatest query.
#[pyfunction]
fn run_greatest_query(input: Vec<Vec<i32>>) -> PyResult<Vec<Option<i32>>> {
    // Convert input to ArrayRefs
    let arrays: Vec<ArrayRef> = input
        .iter()
        .map(|col| Arc::new(Int32Array::from(col.clone())) as ArrayRef)
        .collect();

    // Compute greatest
    let greatest_array = greatest_inner(&arrays).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Error computing greatest: {}",
            e
        ))
    })?;

    // Downcast to Int32Array
    let greatest_int_array = greatest_array
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to downcast to Int32Array")
        })?;

    // Collect results with proper handling of nulls
    let result = (0..greatest_int_array.len())
        .map(|i| {
            if greatest_int_array.is_null(i) {
                None
            } else {
                Some(greatest_int_array.value(i))
            }
        })
        .collect();

    Ok(result)
}

/// PyO3 module definition.
#[pymodule]
fn greatest(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_greatest_query, m)?)?;
    Ok(())
}
