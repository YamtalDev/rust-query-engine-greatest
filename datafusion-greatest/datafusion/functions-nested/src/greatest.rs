// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! [ScalarUDFImpl] definitions for the greatest function.

use crate::utils::make_scalar_function;
use arrow::array::*;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_schema::TimeUnit;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    type_coercion::binary::get_wider_type, ColumnarValue, Documentation, ScalarUDFImpl,
    Signature, Volatility,
};
use num_traits::Float;
use std::any::Any;
use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};
use types::{
    Date32Type, Date64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};

make_udf_expr_and_func!(
    Greatest,
    greatest,
    "Returns the greatest value among the arguments, skipping nulls.",
    greatest_udf
);

#[derive(Debug)]
pub struct Greatest {
    signature: Signature,
    aliases: Vec<String>,
}

impl Greatest {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for Greatest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return Err(DataFusionError::Plan(
                "The 'greatest' function requires at least two arguments".to_string(),
            ));
        }

        let mut common_type = arg_types[0].clone();
        for arg_type in &arg_types[1..] {
            let coerced_type =
                if is_date_type(&common_type) && is_timestamp_type(arg_type) {
                    arg_type.clone()
                } else if is_timestamp_type(&common_type) && is_date_type(arg_type) {
                    common_type.clone()
                } else if (common_type == DataType::Boolean && arg_type.is_numeric())
                    || (arg_type == &DataType::Boolean && common_type.is_numeric())
                {
                    DataType::Int64
                } else {
                    get_wider_type(&common_type, arg_type)?
                };
            common_type = coerced_type;
        }

        Ok(common_type)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(greatest_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_greatest_doc())
    }
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn is_timestamp_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _))
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_greatest_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_description(
                "Returns the greatest value among the arguments, skipping null values.",
            )
            .with_syntax_example("greatest(value1[, value2[, ...]])")
            .with_sql_example(
                r#"
sql
> SELECT greatest(10, 20, 30);
+----------------------+
| greatest(10, 20, 30) |
+----------------------+
| 30                   |
+----------------------+
"#,
            )
            .build()
            .unwrap()
    })
}

pub fn greatest_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 {
        return Err(DataFusionError::Plan(
            "The 'greatest' function requires at least two arguments".to_string(),
        ));
    }

    // Determine the common supertype of all arguments with Float64 coercion
    let arg_types: Vec<DataType> =
        args.iter().map(|arg| arg.data_type().clone()).collect();
    let data_type = {
        let mut common_type = arg_types[0].clone();
        for arg_type in &arg_types[1..] {
            match (&common_type, arg_type) {
                // If either type is Float64, coerce to Float64
                (DataType::Float64, _) | (_, DataType::Float64) => {
                    common_type = DataType::Float64;
                }
                _ => {
                    common_type = get_wider_type(&common_type, arg_type)?;
                }
            }
        }
        common_type
    };

    // **Handle DataType::Null**
    if data_type == DataType::Null {
        // All inputs are Null
        let num_rows = args[0].len();
        // Create a NullArray of length num_rows
        let null_array = NullArray::new(num_rows);
        return Ok(Arc::new(null_array) as ArrayRef);
    }

    // **Proceed with casting and the rest of the function**
    // Cast all arrays to the common type
    let arrays = args
        .iter()
        .map(|array| {
            arrow::compute::cast(array, &data_type).map_err(DataFusionError::from)
        })
        .collect::<Result<Vec<_>>>()?;

    if arrays.is_empty() || arrays[0].len() == 0 {
        return Err(DataFusionError::Execution(
            "The input arrays are empty".to_string(),
        ));
    }

    match data_type {
        DataType::Int8 => compute_greatest_numeric::<Int8Type>(&arrays),
        DataType::Int16 => compute_greatest_numeric::<Int16Type>(&arrays),
        DataType::Int32 => compute_greatest_numeric::<Int32Type>(&arrays),
        DataType::Int64 => compute_greatest_numeric::<Int64Type>(&arrays),
        DataType::UInt8 => compute_greatest_numeric::<UInt8Type>(&arrays),
        DataType::UInt16 => compute_greatest_numeric::<UInt16Type>(&arrays),
        DataType::UInt32 => compute_greatest_numeric::<UInt32Type>(&arrays),
        DataType::UInt64 => compute_greatest_numeric::<UInt64Type>(&arrays),
        DataType::Decimal128(_, _) => compute_greatest_decimal(&arrays),
        DataType::Date32 => compute_greatest_numeric::<Date32Type>(&arrays),
        DataType::Date64 => compute_greatest_numeric::<Date64Type>(&arrays),
        DataType::Time32(TimeUnit::Second) => {
            compute_greatest_numeric::<Time32SecondType>(&arrays)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            compute_greatest_numeric::<Time32MillisecondType>(&arrays)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            compute_greatest_numeric::<Time64MicrosecondType>(&arrays)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            compute_greatest_numeric::<Time64NanosecondType>(&arrays)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            compute_greatest_numeric::<TimestampSecondType>(&arrays)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            compute_greatest_numeric::<TimestampMillisecondType>(&arrays)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            compute_greatest_numeric::<TimestampMicrosecondType>(&arrays)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            compute_greatest_numeric::<TimestampNanosecondType>(&arrays)
        }
        DataType::Float32 => compute_greatest_float::<Float32Type>(&arrays),
        DataType::Float64 => compute_greatest_float::<Float64Type>(&arrays),
        DataType::Boolean => compute_greatest_boolean(&arrays),
        DataType::Utf8 => compute_greatest_utf8(&arrays),
        DataType::LargeUtf8 => compute_greatest_large_utf8(&arrays),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Greatest function not implemented for data type {:?}",
            data_type
        ))),
    }
}

// Function for integer and non-floating-point numeric types
fn compute_greatest_numeric<T>(arrays: &[ArrayRef]) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd + Copy,
{
    let arrays = arrays
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast array".to_string())
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_rows = arrays[0].len();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(num_rows);

    for row in 0..num_rows {
        let max_value = arrays
            .iter()
            .filter_map(|array| {
                if array.is_valid(row) {
                    Some(array.value(row))
                } else {
                    None
                }
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        if let Some(value) = max_value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn compute_greatest_decimal(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    // Ensure all arrays are Decimal128Array
    let arrays = arrays
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Failed to downcast array to Decimal128Array".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_rows = arrays[0].len();
    let data_type = arrays[0].data_type().clone();

    // Extract precision and scale from data_type
    let (_precision, _scale) = match &data_type {
        DataType::Decimal128(p, s) => (*p, *s),
        _ => {
            return Err(DataFusionError::Execution(
                "Data type is not Decimal128".to_string(),
            ))
        }
    };

    // Initialize Decimal128Builder with precision and scale
    let mut builder = Decimal128Builder::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut max_value: Option<i128> = None;
        for array in &arrays {
            if array.is_valid(row) {
                let value = array.value(row);
                max_value = Some(match max_value {
                    None => value,
                    Some(current_max) => {
                        if value > current_max {
                            value
                        } else {
                            current_max
                        }
                    }
                });
            }
        }
        if let Some(value) = max_value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// Function for floating-point types with NaN handling
fn compute_greatest_float<T>(arrays: &[ArrayRef]) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Float + PartialOrd + Copy,
{
    let arrays = arrays
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast array".to_string())
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_rows = arrays[0].len();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(num_rows);

    for row in 0..num_rows {
        let values = arrays.iter().filter_map(|array| {
            if array.is_valid(row) {
                Some(array.value(row))
            } else {
                None
            }
        });

        let mut max_value: Option<T::Native> = None;
        for value in values {
            max_value = Some(match max_value {
                None => value,
                Some(current_max) => {
                    if current_max.is_nan() {
                        current_max
                    } else if value.is_nan() {
                        value
                    } else if value > current_max {
                        value
                    } else {
                        current_max
                    }
                }
            });
        }

        if let Some(value) = max_value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn compute_greatest_boolean(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let arrays = arrays
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Failed to downcast array to BooleanArray".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_rows = arrays[0].len();
    let mut builder = BooleanBuilder::with_capacity(num_rows);

    for row in 0..num_rows {
        let max_value = arrays
            .iter()
            .filter_map(|array| {
                if array.is_valid(row) {
                    Some(array.value(row))
                } else {
                    None
                }
            })
            .max();

        if let Some(value) = max_value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn compute_greatest_utf8(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let arrays = arrays
        .iter()
        .map(|array| {
            array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution("Failed to downcast array".to_string())
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_rows = arrays[0].len();
    let mut builder = StringBuilder::new();

    for row in 0..num_rows {
        let mut max_value: Option<&str> = None;
        for array in &arrays {
            if array.is_valid(row) {
                let value = array.value(row);
                max_value = Some(match max_value {
                    None => value,
                    Some(current_max) => {
                        if value > current_max {
                            value
                        } else {
                            current_max
                        }
                    }
                });
            }
        }
        if let Some(value) = max_value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn compute_greatest_large_utf8(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let arrays = arrays
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast array".to_string())
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_rows = arrays[0].len();
    let mut builder = LargeStringBuilder::new();

    for row in 0..num_rows {
        let mut max_value: Option<&str> = None;
        for array in &arrays {
            if array.is_valid(row) {
                let value = array.value(row);
                max_value = Some(match max_value {
                    None => value,
                    Some(current_max) => {
                        if value > current_max {
                            value
                        } else {
                            current_max
                        }
                    }
                });
            }
        }
        if let Some(value) = max_value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate, NaiveDateTime};
    use datafusion_common::Result;
    use std::sync::Arc;

    /// Helper function to create a Date32Array from date strings
    fn date32_array_from_dates(dates: &[Option<&str>]) -> Date32Array {
        dates
            .iter()
            .map(|opt| {
                opt.map(|s| {
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                    date.num_days_from_ce()
                        - NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .num_days_from_ce()
                })
            })
            .collect()
    }

    /// Helper function to create a Date64Array from date strings
    fn date64_array_from_dates(dates: &[Option<&str>]) -> Date64Array {
        dates
            .iter()
            .map(|opt| {
                opt.map(|s| {
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                    date.and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_utc()
                        .timestamp_millis()
                })
            })
            .collect()
    }

    /// Helper function to create a TimestampNanosecondArray from datetime strings
    fn timestamp_ns_array_from_datetimes(
        datetimes: &[Option<&str>],
    ) -> TimestampNanosecondArray {
        datetimes
            .iter()
            .map(|opt| {
                opt.map(|s| {
                    let datetime =
                        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap();
                    datetime.and_utc().timestamp_nanos_opt().unwrap()
                })
            })
            .collect()
    }

    // --- Basic Numeric Types ---

    #[test]
    /// Tests the greatest function with Int8 arrays including null values.
    fn test_greatest_int8() -> Result<()> {
        let input_int = vec![
            Arc::new(Int8Array::from(vec![Some(1), Some(4), Some(3), None])) as ArrayRef,
            Arc::new(Int8Array::from(vec![Some(2), None, Some(6), Some(8)])) as ArrayRef,
            Arc::new(Int8Array::from(vec![Some(7), Some(5), None, Some(9)])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_int)?;
        let greatest_int = greatest
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Failed to downcast to Int8Array");

        let expected = Int8Array::from(vec![Some(7), Some(5), Some(6), Some(9)]);
        assert_eq!(greatest_int, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with Int32 arrays including null values.
    fn test_greatest_int32() -> Result<()> {
        let input_int = vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(4), Some(3), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(2), None, Some(6), Some(8)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(7), Some(5), None, Some(9)])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_int)?;
        let greatest_int = greatest
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        let expected = Int32Array::from(vec![Some(7), Some(5), Some(6), Some(9)]);
        assert_eq!(greatest_int, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with Float64 arrays including NaN values.
    /// In Spark, NaN is considered greater than any other numeric value.
    fn test_greatest_float64_with_nan() -> Result<()> {
        let input_float = vec![
            Arc::new(Float64Array::from(vec![
                Some(1.1),
                None,
                Some(3.3),
                Some(f64::NAN),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(4.4),
                Some(5.5),
                None,
                Some(2.2),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(7.7),
                Some(8.8),
                Some(9.9),
                Some(f64::NAN),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_float)?;
        let greatest_float = greatest
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        // Expected results:
        // Row 0: 1.1, 4.4, 7.7 → Greatest: 7.7
        // Row 1: None, 5.5, 8.8 → Greatest: 8.8
        // Row 2: 3.3, None, 9.9 → Greatest: 9.9
        // Row 3: NaN, 2.2, NaN → Greatest: NaN
        let expected =
            Float64Array::from(vec![Some(7.7), Some(8.8), Some(9.9), Some(f64::NAN)]);
        assert!(
            greatest_float
                .iter()
                .zip(expected.iter())
                .all(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) if a.is_nan() && b.is_nan() => true,
                    (Some(a), Some(b)) => (a - b).abs() < f64::EPSILON,
                    (None, None) => true,
                    _ => false,
                }),
            "Arrays are not equal"
        );

        Ok(())
    }

    #[test]
    /// Tests the greatest function with Float64 arrays including Infinity values.
    fn test_greatest_with_infinities() -> Result<()> {
        let input_float = vec![
            Arc::new(Float64Array::from(vec![
                Some(f64::NEG_INFINITY),
                Some(1.0),
                Some(2.0),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(f64::INFINITY),
                Some(1.5),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_float)?;
        let greatest_float = greatest
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        let expected =
            Float64Array::from(vec![Some(0.0), Some(f64::INFINITY), Some(2.0)]);
        assert!(
            greatest_float
                .iter()
                .zip(expected.iter())
                .all(|(a, b)| match (a, b) {
                    // Both are NaN
                    (Some(a), Some(b)) if a.is_nan() && b.is_nan() => true,
                    // Both are exactly equal (covers Infinity and -Infinity)
                    (Some(a), Some(b)) if a == b => true,
                    // Both are finite and approximately equal
                    (Some(a), Some(b)) => (a - b).abs() < f64::EPSILON,
                    // Both are None
                    (None, None) => true,
                    // Any other case is a mismatch
                    _ => false,
                }),
            "Arrays are not equal"
        );

        Ok(())
    }

    // --- Date and Time Types ---

    #[test]
    /// Tests the greatest function with Date32 arrays including null values.
    fn test_greatest_date32() -> Result<()> {
        let input_dates = vec![
            Arc::new(date32_array_from_dates(&[
                Some("2020-01-01"),
                Some("2020-06-01"),
                None,
            ])) as ArrayRef,
            Arc::new(date32_array_from_dates(&[
                Some("2020-03-01"),
                Some("2020-05-01"),
                Some("2020-07-01"),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_dates)?;
        let greatest_date = greatest
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Failed to downcast to Date32Array");

        let expected = date32_array_from_dates(&[
            Some("2020-03-01"),
            Some("2020-06-01"),
            Some("2020-07-01"),
        ]);
        assert_eq!(greatest_date, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with Date64 arrays including null values.
    fn test_greatest_date64() -> Result<()> {
        let input_dates = vec![
            Arc::new(date64_array_from_dates(&[
                Some("2020-01-01"),
                Some("2020-06-01"),
                None,
            ])) as ArrayRef,
            Arc::new(date64_array_from_dates(&[
                Some("2020-03-01"),
                Some("2020-05-01"),
                Some("2020-07-01"),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_dates)?;
        let greatest_date = greatest
            .as_any()
            .downcast_ref::<Date64Array>()
            .expect("Failed to downcast to Date64Array");

        let expected = date64_array_from_dates(&[
            Some("2020-03-01"),
            Some("2020-06-01"),
            Some("2020-07-01"),
        ]);
        assert_eq!(greatest_date, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with TimestampNanosecond arrays including null values.
    fn test_greatest_timestamp_ns() -> Result<()> {
        let input_timestamps = vec![
            Arc::new(timestamp_ns_array_from_datetimes(&[
                Some("2020-01-01 12:00:00"),
                Some("2020-06-01 08:00:00"),
                None,
            ])) as ArrayRef,
            Arc::new(timestamp_ns_array_from_datetimes(&[
                Some("2020-03-01 15:30:00"),
                Some("2020-05-01 20:45:00"),
                Some("2020-07-01 10:15:00"),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_timestamps)?;
        let greatest_timestamp = greatest
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Failed to downcast to TimestampNanosecondArray");

        let expected = timestamp_ns_array_from_datetimes(&[
            Some("2020-03-01 15:30:00"),
            Some("2020-06-01 08:00:00"),
            Some("2020-07-01 10:15:00"),
        ]);
        assert_eq!(greatest_timestamp, &expected);

        Ok(())
    }

    // --- String Types ---

    #[test]
    /// Tests the greatest function with Utf8 arrays (StringArray).
    fn test_greatest_utf8() -> Result<()> {
        let input_utf8 = vec![
            Arc::new(StringArray::from(vec![
                Some("apple"),
                Some("banana"),
                Some("cherry"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("apricot"),
                Some("blueberry"),
                Some("citrus"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("avocado"),
                Some("blackberry"),
                Some("cranberry"),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_utf8)?;
        let greatest_utf8 = greatest
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");

        let expected = StringArray::from(vec![
            Some("avocado"),
            Some("blueberry"),
            Some("cranberry"),
        ]);
        assert_eq!(greatest_utf8, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with LargeUtf8 arrays (LargeStringArray).
    fn test_greatest_large_utf8() -> Result<()> {
        let input_large_utf8 = vec![
            Arc::new(LargeStringArray::from(vec![
                Some("apple"),
                Some("banana"),
                Some("cherry"),
            ])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some("apricot"),
                Some("blueberry"),
                Some("citrus"),
            ])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some("avocado"),
                Some("blackberry"),
                Some("cranberry"),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_large_utf8)?;
        let greatest_utf8 = greatest
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("Failed to downcast to LargeStringArray");

        let expected = LargeStringArray::from(vec![
            Some("avocado"),
            Some("blueberry"),
            Some("cranberry"),
        ]);
        assert_eq!(greatest_utf8, &expected);

        Ok(())
    }

    // --- Boolean Type ---

    #[test]
    /// Tests the greatest function with Boolean arrays.
    fn test_greatest_boolean() -> Result<()> {
        let input_bool = vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(true),
            ])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_bool)?;
        let greatest_bool = greatest
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Failed to downcast to BooleanArray");

        let expected = BooleanArray::from(vec![Some(true), Some(true), Some(true)]);
        assert_eq!(greatest_bool, &expected);

        Ok(())
    }

    // --- Null Handling ---

    #[test]
    /// Tests the greatest function when arrays contain null values.
    fn test_greatest_with_nulls() -> Result<()> {
        let input_int = vec![
            Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None, Some(2), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None, None, Some(3)])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_int)?;
        let greatest_int = greatest
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        let expected = Int32Array::from(vec![None, Some(2), Some(3)]);
        assert_eq!(greatest_int, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function when all values are null.
    fn test_greatest_all_nulls() -> Result<()> {
        let input_nulls = vec![
            Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
        ];

        let greatest = greatest_inner(&input_nulls)?;
        let greatest_int = greatest
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(greatest_int, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function when all values are NaN.
    fn test_greatest_all_nan() -> Result<()> {
        let input_float = vec![
            Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(f64::NAN)]))
                as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(f64::NAN)]))
                as ArrayRef,
        ];

        let greatest = greatest_inner(&input_float)?;
        let greatest_float = greatest
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        let expected = Float64Array::from(vec![Some(f64::NAN), Some(f64::NAN)]);
        assert!(
            greatest_float
                .iter()
                .zip(expected.iter())
                .all(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) if a.is_nan() && b.is_nan() => true,
                    (None, None) => true,
                    _ => false,
                }),
            "Arrays are not equal"
        );

        Ok(())
    }

    // --- Type Coercion and Mixed Types ---

    #[test]
    /// Tests the greatest function with mixed numeric types (Int32 and Float64).
    fn test_greatest_mixed_numeric_types() -> Result<()> {
        let input_mixed = vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(4.0), Some(5.0), Some(6.0)]))
                as ArrayRef,
        ];

        let greatest = greatest_inner(&input_mixed)?;
        let greatest_float64 = greatest
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        let expected = Float64Array::from(vec![Some(4.0), Some(5.0), Some(6.0)]);
        assert_eq!(greatest_float64, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with explicit type coercion.
    fn test_greatest_type_coercion() -> Result<()> {
        let input_coercion = vec![
            arrow::compute::cast(
                &(Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))
                    as ArrayRef),
                &DataType::Float64,
            )?,
            Arc::new(Float64Array::from(vec![Some(4.4), Some(5.5), Some(6.6)]))
                as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(7.7), Some(8.8), Some(9.9)]))
                as ArrayRef,
        ];

        let greatest = greatest_inner(&input_coercion)?;
        let greatest_float64 = greatest
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");

        let expected = Float64Array::from(vec![Some(7.7), Some(8.8), Some(9.9)]);
        assert_eq!(greatest_float64, &expected);

        Ok(())
    }

    // --- Error Cases ---

    #[test]
    /// Tests the greatest function with incompatible mixed types (Int32 and Utf8).
    /// Expects an error due to type coercion failure.
    fn test_greatest_mixed_types_error() -> Result<()> {
        let input_mixed = vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]))
                as ArrayRef,
        ];

        let result = greatest_inner(&input_mixed);
        assert!(result.is_err(), "Expected an error but function succeeded.");
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("There is no wider type that can represent both Int32 and Utf8") ||
            error_message.contains("data type mismatch") ||
            error_message.contains("common supertype") ||
            error_message.contains("cannot resolve 'greatest"),
            "Expected error message to contain 'common supertype', 'data type mismatch', or 'cannot resolve 'greatest', but got '{}'",
            error_message
        );

        Ok(())
    }

    #[test]
    /// Tests the greatest function with insufficient number of arguments (only one array).
    fn test_greatest_invalid_number_of_arguments() {
        let input_single =
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef];

        let result = greatest_inner(&input_single);
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("The 'greatest' function requires at least two arguments"),
            "Expected error message to contain 'The 'greatest' function requires at least two arguments', but got '{}'",
            error_message
        );
    }

    #[test]
    /// Tests the greatest function with zero arguments.
    fn test_greatest_zero_arguments() {
        let input_empty: Vec<ArrayRef> = vec![];

        let result = greatest_inner(&input_empty);
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("The 'greatest' function requires at least two arguments"),
            "Expected error message to contain 'The 'greatest' function requires at least two arguments', but got '{}'",
            error_message
        );
    }

    // --- Stress Tests ---

    #[test]
    /// Tests the greatest function with a large number of arguments to check performance.
    fn test_greatest_large_number_of_arguments() -> Result<()> {
        // Generate a large number of arrays
        let num_arrays = 1000;
        let mut input_arrays: Vec<ArrayRef> = Vec::with_capacity(num_arrays);
        for i in 0..num_arrays {
            let array = Arc::new(Int32Array::from(vec![Some(i as i32); 3])) as ArrayRef;
            input_arrays.push(array);
        }

        let greatest = greatest_inner(&input_arrays)?;
        let greatest_int = greatest
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        let expected_value = (num_arrays - 1) as i32;
        let expected = Int32Array::from(vec![Some(expected_value); 3]);
        assert_eq!(greatest_int, &expected);

        Ok(())
    }

    #[test]
    /// Tests the greatest function with a very large number of arguments to check code generation limits.
    fn test_greatest_code_generation_limit() -> Result<()> {
        // Test with a large number of arguments to check for code generation limits
        let num_arrays = 2000;
        let mut input_arrays: Vec<ArrayRef> = Vec::with_capacity(num_arrays);
        for i in 0..num_arrays {
            let array = Arc::new(Int32Array::from(vec![Some(i as i32)])) as ArrayRef;
            input_arrays.push(array);
        }

        let greatest = greatest_inner(&input_arrays)?;
        let greatest_int = greatest
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");

        let expected_value = (num_arrays - 1) as i32;
        let expected = Int32Array::from(vec![Some(expected_value)]);
        assert_eq!(greatest_int, &expected);

        Ok(())
    }
}
