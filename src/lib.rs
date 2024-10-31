use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::arrow::array::*;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::type_coercion::binary::get_wider_type;
use datafusion_functions_nested::greatest::greatest_inner;
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDate, PyDateAccess, PyDateTime, PyList, PyTimeAccess};
use std::sync::Arc;

/// Infer the data type from a PyAny object.
fn infer_single_data_type(_py: Python, item: &PyAny) -> PyResult<Option<DataType>> {
    if item.is_instance_of::<pyo3::types::PyLong>() {
        Ok(Some(DataType::Int64))
    } else if item.is_instance_of::<pyo3::types::PyFloat>() {
        Ok(Some(DataType::Float64))
    } else if item.is_instance_of::<pyo3::types::PyBool>() {
        Ok(Some(DataType::Boolean))
    } else if item.is_instance_of::<pyo3::types::PyString>() {
        Ok(Some(DataType::Utf8))
    } else if item.is_instance_of::<PyDate>() {
        Ok(Some(DataType::Date32))
    } else if item.is_instance_of::<PyDateTime>() {
        Ok(Some(DataType::Timestamp(TimeUnit::Nanosecond, None)))
    } else {
        Ok(None)
    }
}

/// Infer the common data type among a list of data types.
fn infer_common_data_type(types: &[DataType]) -> Result<DataType, PyErr> {
    if types.is_empty() {
        return Err(PyErr::new::<exceptions::PyTypeError, _>(
            "Cannot infer data type from empty input.",
        ));
    }

    let mut common_type = types[0].clone();
    for t in &types[1..] {
        match (&common_type, t) {
            (DataType::Float64, _) | (_, DataType::Float64) => {
                common_type = DataType::Float64;
            }
            _ => {
                common_type = get_wider_type(&common_type, t).map_err(|_e| {
                    PyErr::new::<exceptions::PyTypeError, _>(format!(
                        "data type mismatch: cannot resolve 'greatest' due to data type mismatch between {:?} and {:?}",
                        common_type, t
                    ))
                })?;
            }
        }
    }
    Ok(common_type)
}

/// PyO3 wrapper function to execute the greatest query generically.
#[pyfunction]
fn run_greatest(py: Python, input: &Bound<'_, PyList>) -> PyResult<PyObject> {
    let num_columns = input.len();
    if num_columns < 2 {
        return Err(PyErr::new::<exceptions::PyValueError, _>(
            "The 'greatest' function requires at least two arguments.",
        ));
    }

    // Collect data types from all columns
    let mut types = Vec::new();
    for col in input.iter() {
        let inner_list = col.downcast::<PyList>().map_err(|_| {
            PyErr::new::<exceptions::PyTypeError, _>("Each argument to 'greatest' must be a list.")
        })?;
        for item in inner_list.iter() {
            if !item.is_none() {
                if let Some(dt) = infer_single_data_type(py, item.into_gil_ref())? {
                    types.push(dt);
                } else {
                    return Err(PyErr::new::<exceptions::PyTypeError, _>(
                        "Unsupported data type in input.",
                    ));
                }
            }
        }
    }

    if types.is_empty() {
        // All inputs are null
        let num_rows = input.get_item(0)?.downcast::<PyList>()?.len();
        let rust_result: Vec<Option<PyObject>> = vec![None; num_rows];
        return Ok(PyList::new_bound(py, rust_result).into());
    }

    // Infer the common data type
    let data_type = infer_common_data_type(&types)?;

    // Convert input columns to Arrow ArrayRefs based on the inferred data type
    let arrays: Vec<ArrayRef> = input
        .iter()
        .map(|col| {
            let inner_list = col.downcast::<PyList>().map_err(|_| {
                PyErr::new::<exceptions::PyTypeError, _>(
                    "Each argument to 'greatest' must be a list.",
                )
            })?;
            match data_type {
                DataType::Int8 => {
                    let vec: Vec<Option<i8>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<i8>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(Int8Array::from(vec)) as ArrayRef)
                }
                DataType::Int16 => {
                    let vec: Vec<Option<i16>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<i16>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(Int16Array::from(vec)) as ArrayRef)
                }
                DataType::Int32 => {
                    let vec: Vec<Option<i32>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<i32>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(Int32Array::from(vec)) as ArrayRef)
                }
                DataType::Int64 => {
                    let vec: Vec<Option<i64>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<i64>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(Int64Array::from(vec)) as ArrayRef)
                }
                DataType::Float32 => {
                    let vec: Vec<Option<f32>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<f32>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(Float32Array::from(vec)) as ArrayRef)
                }
                DataType::Float64 => {
                    let vec: Vec<Option<f64>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<f64>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(Float64Array::from(vec)) as ArrayRef)
                }
                DataType::Boolean => {
                    let vec: Vec<Option<bool>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<bool>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(BooleanArray::from(vec)) as ArrayRef)
                }
                DataType::Utf8 => {
                    let vec: Vec<Option<String>> = inner_list
                        .iter()
                        .map(|item| item.extract::<Option<String>>().ok().flatten())
                        .collect();
                    Ok(Arc::new(StringArray::from(vec)) as ArrayRef)
                }
                DataType::Date32 => {
                    let vec: Vec<Option<i32>> = inner_list
                        .iter()
                        .map(|item| {
                            item.extract::<Option<&PyDate>>()
                                .ok()
                                .flatten()
                                .map(|d| {
                                    NaiveDate::from_ymd_opt(
                                        d.get_year(),
                                        d.get_month() as u32,
                                        d.get_day() as u32,
                                    )
                                    .ok_or_else(|| {
                                        PyErr::new::<exceptions::PyValueError, _>(
                                            "Invalid date value.",
                                        )
                                    })
                                    .map(|date| {
                                        date.num_days_from_ce()
                                            - NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<Option<i32>>, _>>()?;
                    Ok(Arc::new(Date32Array::from(vec)) as ArrayRef)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let vec: Vec<Option<i64>> = inner_list
                        .iter()
                        .map(|item| {
                            item.extract::<Option<&PyDateTime>>()
                                .ok()
                                .flatten()
                                .map(|dt| {
                                    NaiveDate::from_ymd_opt(
                                        dt.get_year(),
                                        dt.get_month() as u32,
                                        dt.get_day() as u32,
                                    )
                                    .and_then(|date| {
                                        NaiveTime::from_hms_micro_opt(
                                            dt.get_hour() as u32,
                                            dt.get_minute() as u32,
                                            dt.get_second() as u32,
                                            dt.get_microsecond(),
                                        )
                                        .map(|time| NaiveDateTime::new(date, time))
                                    })
                                    .ok_or_else(|| {
                                        PyErr::new::<exceptions::PyValueError, _>(
                                            "Invalid datetime value.",
                                        )
                                    })
                                    .and_then(|datetime| {
                                        Ok(datetime.timestamp() * 1_000_000_000
                                            + datetime.timestamp_subsec_nanos() as i64)
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<Option<i64>>, _>>()?;
                    Ok(Arc::new(TimestampNanosecondArray::from(vec)) as ArrayRef)
                }

                _ => Err(PyErr::new::<exceptions::PyTypeError, _>(
                    "Unsupported data type for 'greatest'.",
                )),
            }
        })
        .collect::<PyResult<Vec<ArrayRef>>>()?;

    // Cast arrays to common type
    let arrays = arrays
        .into_iter()
        .map(|array| {
            compute::cast(&array, &data_type).map_err(|e| {
                PyErr::new::<exceptions::PyRuntimeError, _>(format!("Error casting array: {}", e))
            })
        })
        .collect::<PyResult<Vec<ArrayRef>>>()?;

    // Compute greatest
    let greatest_array = greatest_inner(&arrays).map_err(|e| {
        PyErr::new::<exceptions::PyRuntimeError, _>(format!("Error computing greatest: {}", e))
    })?;

    // Extract the greatest values
    let result = match data_type {
        DataType::Int8 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>("Failed to downcast to Int8Array.")
                })?;
            let rust_result = array.iter().collect::<Vec<Option<i8>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Int16 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>("Failed to downcast to Int16Array.")
                })?;
            let rust_result = array.iter().collect::<Vec<Option<i16>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Int32 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>("Failed to downcast to Int32Array.")
                })?;
            let rust_result = array.iter().collect::<Vec<Option<i32>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Int64 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>("Failed to downcast to Int64Array.")
                })?;
            let rust_result = array.iter().collect::<Vec<Option<i64>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Float32 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>(
                        "Failed to downcast to Float32Array.",
                    )
                })?;
            let rust_result = array.iter().collect::<Vec<Option<f32>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Float64 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>(
                        "Failed to downcast to Float64Array.",
                    )
                })?;
            let rust_result = array.iter().collect::<Vec<Option<f64>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Boolean => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>(
                        "Failed to downcast to BooleanArray.",
                    )
                })?;
            let rust_result = array.iter().collect::<Vec<Option<bool>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Utf8 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>(
                        "Failed to downcast to StringArray.",
                    )
                })?;
            let rust_result = array
                .iter()
                .map(|opt| opt.map(|s| s.to_string()))
                .collect::<Vec<Option<String>>>();
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Date32 => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>(
                        "Failed to downcast to Date32Array.",
                    )
                })?;
            let rust_result = array
                .iter()
                .map(|opt| {
                    opt.map(|days| {
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .and_then(|d| d.checked_add(chrono::Duration::days(days as i64)))
                            .ok_or_else(|| {
                                PyErr::new::<exceptions::PyValueError, _>("Invalid date value.")
                            })
                            .and_then(|naive_date| {
                                Ok(PyDate::new_bound(
                                    py,
                                    naive_date.year(),
                                    naive_date.month() as u8,
                                    naive_date.day() as u8,
                                )?
                                .into_py(py))
                            })
                    })
                    .transpose()
                })
                .collect::<PyResult<Vec<Option<PyObject>>>>()?;
            Ok(PyList::new_bound(py, rust_result).into())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = greatest_array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    PyErr::new::<exceptions::PyRuntimeError, _>(
                        "Failed to downcast to TimestampNanosecondArray.",
                    )
                })?;
            let rust_result = array
                .iter()
                .map(|opt| {
                    opt.map(|nanos| {
                        PyDateTime::from_timestamp_bound(py, nanos as f64 / 1_000_000_000.0, None)
                            .map(|py_datetime| py_datetime.into_py(py))
                    })
                    .transpose()
                })
                .collect::<PyResult<Vec<Option<PyObject>>>>()?;
            Ok(PyList::new_bound(py, &rust_result).into())
        }

        _ => Err(PyErr::new::<exceptions::PyTypeError, _>(
            "Unsupported data type for 'greatest'.",
        )),
    };

    result
}

#[pymodule]
fn greatest(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_greatest, m)?)?;
    Ok(())
}
