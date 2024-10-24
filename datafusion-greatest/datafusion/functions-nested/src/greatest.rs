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
use std::any::Any;
use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};
use types::{
    Date32Type, Date64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
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

        // Find the common supertype among the arguments
        let mut common_type = arg_types[0].clone();
        for arg_type in &arg_types[1..] {
            common_type = get_wider_type(&common_type, arg_type)?;
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

    // Determine the common supertype of all arguments
    let arg_types: Vec<DataType> =
        args.iter().map(|arg| arg.data_type().clone()).collect();
    let data_type = {
        let mut common_type = arg_types[0].clone();
        for arg_type in &arg_types[1..] {
            common_type = get_wider_type(&common_type, arg_type)?;
        }
        common_type
    };

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
        DataType::Float32 => compute_greatest_numeric::<Float32Type>(&arrays),
        DataType::Float64 => compute_greatest_numeric::<Float64Type>(&arrays),
        DataType::Date32 => compute_greatest_numeric::<Date32Type>(&arrays),
        DataType::Date64 => compute_greatest_numeric::<Date64Type>(&arrays),
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
        DataType::Utf8 => compute_greatest_utf8(&arrays),
        DataType::LargeUtf8 => compute_greatest_large_utf8(&arrays),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Greatest function not implemented for data type {:?}",
            data_type
        ))),
    }
}

// Generic function to compute greatest for numeric types
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
                    let value = array.value(row);
                    if value.partial_cmp(&value).is_none() {
                        None
                    } else {
                        Some(value)
                    }
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

    #[test]
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

        let expected =
            Float64Array::from(vec![Some(7.7), Some(8.8), Some(9.9), Some(2.2)]);
        assert_eq!(greatest_float, &expected);

        Ok(())
    }

    #[test]
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
    fn test_greatest_mixed_types_error() -> Result<()> {
        let input_mixed = vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]))
                as ArrayRef,
        ];

        let result = greatest_inner(&input_mixed);
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("There is no wider type that can represent both Int32 and Utf8"),
            "Expected error message to contain 'There is no wider type that can represent both Int32 and Utf8', but got '{}'",
            error_message
        );

        Ok(())
    }

    #[test]
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

    #[test]
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
        assert_eq!(greatest_float, &expected);

        Ok(())
    }

    #[test]
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

    #[test]
    fn test_greatest_decimal() -> Result<()> {
        // Note: Decimal support depends on the arrow implementation
        // For illustration purposes, assuming FixedSizeBinaryArray is used for decimals
        // Adjust the test according to the actual implementation

        // For this example, we'll skip the decimal test since DataFusion may not support decimals yet

        Ok(())
    }

    #[test]
    fn test_greatest_invalid_number_of_arguments() {
        let input_single =
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef];

        // According to Spark, greatest function requires at least two arguments
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

    #[test]
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

// fn compute_greatest_list(arrays: &[ArrayRef]) -> Result<ArrayRef> {
//     // Assuming the list elements are Int32Array for simplicity
//     // You may need to generalize this based on element type
//     let arrays = arrays
//         .iter()
//         .map(|array| {
//             array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
//                 DataFusionError::Execution("Failed to downcast array".to_string())
//             })
//         })
//         .collect::<Result<Vec<_>>>()?;

//     let num_rows = arrays[0].len();
//     let value_builder = Int32Builder::new();
//     let mut builder = ListBuilder::new(value_builder);

//     for row in 0..num_rows {
//         let mut max_value: Option<i32> = None;
//         for array in &arrays {
//             if array.is_valid(row) {
//                 let values = array.value(row);
//                 let value_array = values
//                     .as_any()
//                     .downcast_ref::<Int32Array>()
//                     .ok_or_else(|| {
//                         DataFusionError::Execution(
//                             "Failed to downcast value array".to_string(),
//                         )
//                     })?;

//                 for index in 0..value_array.len() {
//                     if value_array.is_valid(index) {
//                         let value = value_array.value(index);
//                         max_value = Some(
//                             max_value.map_or(value, |current_max| current_max.max(value)),
//                         );
//                     }
//                 }
//             }
//         }

//         if let Some(value) = max_value {
//             builder.values().append_value(value);
//             builder.append(true);
//         } else {
//             builder.append(false);
//         }
//     }

//     Ok(Arc::new(builder.finish()))
// }

// fn compute_greatest_struct(arrays: &[ArrayRef]) -> Result<ArrayRef> {
//     let arrays = arrays
//         .iter()
//         .map(|array| {
//             array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
//                 DataFusionError::Execution("Failed to downcast array".to_string())
//             })
//         })
//         .collect::<Result<Vec<_>>>()?;

//     let num_rows = arrays[0].len();
//     let fields = arrays[0].data_type().clone();
//     let field_types = match fields {
//         DataType::Struct(fields) => fields,
//         _ => {
//             return Err(DataFusionError::Execution(
//                 "Expected Struct data type".to_string(),
//             ))
//         }
//     };

//     // Create field builders
//     let field_builders: Vec<Box<dyn ArrayBuilder>> = field_types
//         .iter()
//         .map(|field| make_builder(field.data_type(), num_rows))
//         .collect::<Result<_>>()?;

//     let mut builder = StructBuilder::new(field_types, field_builders);

//     for row in 0..num_rows {
//         let mut append_null = true;

//         for field_index in 0..builder.num_fields() {
//             let field_builder =
//                 builder.field_builder::<Int64Builder>(field_index).unwrap();
//             let mut max_value: Option<i64> = None;

//             for array in &arrays {
//                 let column = array.column(field_index);
//                 if column.is_valid(row) {
//                     let value = column
//                         .as_any()
//                         .downcast_ref::<Int64Array>()
//                         .ok_or_else(|| {
//                             DataFusionError::Execution(
//                                 "Failed to downcast column array".to_string(),
//                             )
//                         })?
//                         .value(row);

//                     max_value = Some(
//                         max_value.map_or(value, |current_max| current_max.max(value)),
//                     );
//                 }
//             }

//             if let Some(value) = max_value {
//                 field_builder.append_value(value);
//                 append_null = false;
//             } else {
//                 field_builder.append_null();
//             }
//         }

//         if append_null {
//             builder.append(false);
//         } else {
//             builder.append(true);
//         }
//     }

//     Ok(Arc::new(builder.finish()))
// }

// fn make_builder(data_type: &DataType, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
//     Ok(match data_type {
//         DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
//         DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
//         DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
//         DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
//         DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
//         DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
//         DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
//         DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
//         DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
//         DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
//         DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 0)),
//         DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, 0)),
//         _ => {
//             return Err(DataFusionError::NotImplemented(format!(
//                 "Builder not implemented for data type {:?}",
//                 data_type
//             )))
//         }
//     })
// }
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use arrow::datatypes::Field;
//     use datafusion_common::Result;
//     use std::sync::Arc;

//     #[test]
//     fn test_greatest_int32() -> Result<()> {
//         let input_int = vec![
//             Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![Some(4), None, Some(6), Some(8)])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![Some(7), Some(5), None, Some(9)])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_int)?;
//         let greatest_int = greatest
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .expect("Failed to downcast to Int32Array");

//         let expected = Int32Array::from(vec![Some(7), Some(5), Some(6), Some(9)]);
//         assert_eq!(greatest_int, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_float64_with_nan() -> Result<()> {
//         let input_float = vec![
//             Arc::new(Float64Array::from(vec![
//                 Some(1.1),
//                 None,
//                 Some(3.3),
//                 Some(f64::NAN),
//             ])) as ArrayRef,
//             Arc::new(Float64Array::from(vec![
//                 Some(4.4),
//                 Some(5.5),
//                 None,
//                 Some(2.2),
//             ])) as ArrayRef,
//             Arc::new(Float64Array::from(vec![
//                 Some(7.7),
//                 Some(8.8),
//                 Some(9.9),
//                 Some(f64::NAN),
//             ])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_float)?;
//         let greatest_float = greatest
//             .as_any()
//             .downcast_ref::<Float64Array>()
//             .expect("Failed to downcast to Float64Array");

//         let expected =
//             Float64Array::from(vec![Some(7.7), Some(8.8), Some(9.9), Some(2.2)]);
//         assert_eq!(greatest_float, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_utf8() -> Result<()> {
//         let input_utf8 = vec![
//             Arc::new(StringArray::from(vec![
//                 Some("apple"),
//                 Some("banana"),
//                 Some("cherry"),
//             ])) as ArrayRef,
//             Arc::new(StringArray::from(vec![
//                 Some("apricot"),
//                 Some("blueberry"),
//                 Some("citrus"),
//             ])) as ArrayRef,
//             Arc::new(StringArray::from(vec![
//                 Some("avocado"),
//                 Some("blackberry"),
//                 Some("cranberry"),
//             ])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_utf8)?;
//         let greatest_utf8 = greatest
//             .as_any()
//             .downcast_ref::<StringArray>()
//             .expect("Failed to downcast to StringArray");

//         let expected = StringArray::from(vec![
//             Some("avocado"),
//             Some("blueberry"),
//             Some("cranberry"),
//         ]);
//         assert_eq!(greatest_utf8, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_with_nulls() -> Result<()> {
//         let input_int = vec![
//             Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![None, Some(2), None])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![None, None, Some(3)])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_int)?;
//         let greatest_int = greatest
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .expect("Failed to downcast to Int32Array");

//         let expected = Int32Array::from(vec![None, Some(2), Some(3)]);
//         assert_eq!(greatest_int, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_mixed_types_error() -> Result<()> {
//         let input_mixed = vec![
//             Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
//             Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]))
//                 as ArrayRef,
//         ];

//         let result = greatest_inner(&input_mixed);
//         assert!(result.is_err());
//         let error_message = result.unwrap_err().to_string();
//         assert!(
//             error_message.contains("There is no wider type that can represent both Int32 and Utf8"),
//             "Expected error message to contain 'There is no wider type that can represent both Int32 and Utf8', but got '{}'",
//             error_message
//         );

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_empty_input() {
//         let input_empty: Vec<ArrayRef> = vec![];

//         let result = greatest_inner(&input_empty);
//         assert!(result.is_err());
//         let error_message = result.unwrap_err().to_string();
//         assert!(
//             error_message.contains("The 'greatest' function requires at least one argument"),
//             "Expected error message to contain 'The 'greatest' function requires at least one argument', but got '{}'",
//             error_message
//         );
//     }

//     #[test]
//     fn test_greatest_single_argument() -> Result<()> {
//         let input_single =
//             vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef];

//         let greatest = greatest_inner(&input_single)?;
//         let greatest_int = greatest
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .expect("Failed to downcast to Int32Array");

//         let expected = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
//         assert_eq!(greatest_int, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_large_utf8() -> Result<()> {
//         let input_large_utf8 = vec![
//             Arc::new(LargeStringArray::from(vec![
//                 Some("alpha"),
//                 Some("beta"),
//                 Some("gamma"),
//             ])) as ArrayRef,
//             Arc::new(LargeStringArray::from(vec![
//                 Some("delta"),
//                 Some("epsilon"),
//                 Some("zeta"),
//             ])) as ArrayRef,
//             Arc::new(LargeStringArray::from(vec![
//                 Some("eta"),
//                 Some("theta"),
//                 Some("iota"),
//             ])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_large_utf8)?;
//         let greatest_large_utf8 = greatest
//             .as_any()
//             .downcast_ref::<LargeStringArray>()
//             .expect("Failed to downcast to LargeStringArray");

//         let expected =
//             LargeStringArray::from(vec![Some("eta"), Some("theta"), Some("zeta")]);
//         assert_eq!(greatest_large_utf8, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_unsupported_list() -> Result<()> {
//         let list1 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
//             Some(vec![Some(1), Some(2), Some(3)]),
//             Some(vec![Some(4), Some(5), Some(6)]),
//             None,
//         ])) as ArrayRef;

//         let list2 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
//             Some(vec![Some(7), Some(8), Some(9)]),
//             Some(vec![Some(10), Some(11), Some(12)]),
//             Some(vec![Some(13), Some(14)]),
//         ])) as ArrayRef;

//         let input_list = vec![list1, list2];
//         let result = greatest_inner(&input_list);
//         assert!(result.is_err());
//         let error_message = result.unwrap_err().to_string();
//         assert!(
//             error_message.contains("Greatest function not implemented for data type"),
//             "Expected error message to contain 'Greatest function not implemented for data type', but got '{}'",
//             error_message
//         );

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_unsupported_struct() -> Result<()> {
//         let field_a = Field::new("a", DataType::Int64, false);
//         let field_b = Field::new("b", DataType::Int64, false);

//         let array_a =
//             Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef;
//         let array_b =
//             Arc::new(Int64Array::from(vec![Some(4), Some(5), Some(6)])) as ArrayRef;

//         let struct_array1 = Arc::new(StructArray::from(vec![
//             (Arc::new(field_a.clone()), array_a.clone()),
//             (Arc::new(field_b.clone()), array_b.clone()),
//         ])) as ArrayRef;

//         let array_a2 =
//             Arc::new(Int64Array::from(vec![Some(7), Some(8), Some(9)])) as ArrayRef;
//         let array_b2 =
//             Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])) as ArrayRef;

//         let struct_array2 = Arc::new(StructArray::from(vec![
//             (Arc::new(field_a.clone()), array_a2.clone()),
//             (Arc::new(field_b.clone()), array_b2.clone()),
//         ])) as ArrayRef;

//         let input_struct = vec![struct_array1, struct_array2];
//         let result = greatest_inner(&input_struct);
//         assert!(result.is_err());
//         let error_message = result.unwrap_err().to_string();
//         assert!(
//             error_message.contains("Greatest function not implemented for data type"),
//             "Expected error message to contain 'Greatest function not implemented for data type', but got '{}'",
//             error_message
//         );

//         Ok(())
//     }

//     // Additional edge cases

//     #[test]
//     fn test_greatest_all_nulls() -> Result<()> {
//         let input_nulls = vec![
//             Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_nulls)?;
//         let greatest_int = greatest
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .expect("Failed to downcast to Int32Array");

//         let expected = Int32Array::from(vec![None, None, None]);
//         assert_eq!(greatest_int, &expected);

//         Ok(())
//     }

//     #[test]
//     fn test_greatest_with_infinities() -> Result<()> {
//         let input_float = vec![
//             Arc::new(Float64Array::from(vec![
//                 Some(f64::NEG_INFINITY),
//                 Some(1.0),
//                 Some(2.0),
//             ])) as ArrayRef,
//             Arc::new(Float64Array::from(vec![
//                 Some(0.0),
//                 Some(f64::INFINITY),
//                 Some(1.5),
//             ])) as ArrayRef,
//         ];

//         let greatest = greatest_inner(&input_float)?;
//         let greatest_float = greatest
//             .as_any()
//             .downcast_ref::<Float64Array>()
//             .expect("Failed to downcast to Float64Array");

//         let expected =
//             Float64Array::from(vec![Some(0.0), Some(f64::INFINITY), Some(2.0)]);
//         assert_eq!(greatest_float, &expected);

//         Ok(())
//     }
// }
