use crate::array_accessor::ArrayAccessor;
use arrow::array::{Array, ArrayRef, PrimitiveArray, PrimitiveBuilder, StringArray};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;

/// Implements `ArrayAccessor` for Arrow's `PrimitiveArray`.
///
/// This implementation is generic over Arrow's primitive types, allowing the function
/// to work with types such as `Int32`, `Float32`, etc.
impl<T> ArrayAccessor for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd + Copy,
{
    type Item = T::Native;

    fn value(&self, row: usize) -> Option<Self::Item> {
        if self.is_null(row) {
            None
        } else {
            Some(self.value(row))
        }
    }

    fn len(&self) -> usize {
        Array::len(self)
    }
}

/// Implements `ArrayAccessor` for Arrow's `StringArray`.
///
/// This implementation allows working with `Utf8` string arrays in the `greatest` function.
impl ArrayAccessor for StringArray {
    type Item = String;

    fn value(&self, row: usize) -> Option<Self::Item> {
        if self.is_null(row) {
            None
        } else {
            Some(self.value(row).to_string())
        }
    }

    fn len(&self) -> usize {
        Array::len(self)
    }
}

/// The `greatest` function returns a column of the greatest values from each row, skipping nulls.
/// It works for both primitive types (e.g., `Int32`, `Float32`) and strings (`Utf8`).
///
/// # Arguments
/// * `args` - A slice of Arrow arrays of the same type.
///
/// # Returns
/// * A new Arrow array of the greatest values from each row, or an error if the input is invalid.
///
/// # Errors
/// * If fewer than two columns are passed.
/// * If the columns are of different types or unsupported types.
pub fn greatest(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Ensure there are at least two columns
    if args.len() < 2 {
        return Err(DataFusionError::Execution(
            "The greatest function requires at least 2 columns.".to_string(),
        ));
    }

    // Determine the data type of the first column
    let data_type = args[0].data_type();
    match data_type {
        DataType::Int32 => greatest_primitive::<arrow::datatypes::Int32Type>(args),
        DataType::Float32 => greatest_primitive::<arrow::datatypes::Float32Type>(args),
        DataType::Utf8 => greatest_string(args),
        _ => Err(DataFusionError::Execution(format!(
            "Data type {:?} not supported",
            data_type
        ))),
    }
}

/// Helper function to compute the greatest value for primitive types using generics.
///
/// This function works with primitive types like `Int32`, `Float32`, etc., and selects
/// the greatest value for each row, ignoring nulls.
///
/// # Arguments
/// * `args` - A slice of Arrow arrays of the same primitive type.
///
/// # Returns
/// * A new Arrow array with the greatest value for each row.
fn greatest_primitive<T>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd + Copy,
{
    let num_rows = args[0].len();
    let mut builder = PrimitiveBuilder::<T>::new();

    // Iterate through each row
    for row_idx in 0..num_rows {
        let mut max_value: Option<T::Native> = None;

        // Compare values in each column for this row
        for column in args {
            let array = column.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            if !array.is_null(row_idx) {
                let value = array.value(row_idx);
                max_value = Some(match max_value {
                    None => value,
                    Some(greatest) => {
                        if value > greatest {
                            value
                        } else {
                            greatest
                        }
                    }
                });
            }
        }

        // Append the greatest value or null to the builder
        match max_value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        };
    }

    // Finish building the result array
    Ok(Arc::new(builder.finish()))
}

/// Helper function to compute the greatest value for string types (`Utf8`).
///
/// This function compares the string values row by row and selects the greatest,
/// ignoring nulls.
///
/// # Arguments
/// * `args` - A slice of Arrow `StringArray`s.
///
/// # Returns
/// * A new Arrow `StringArray` with the greatest value for each row.
fn greatest_string(args: &[ArrayRef]) -> Result<ArrayRef> {
    let num_rows = args[0].len();
    let mut greatest_values: Vec<Option<String>> = Vec::with_capacity(num_rows);

    // Iterate through each row
    for row_idx in 0..num_rows {
        let mut max_value: Option<&str> = None;

        // Compare string values in each column for this row
        for column in args {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            if !array.is_null(row_idx) {
                let value = array.value(row_idx);
                max_value = Some(match max_value {
                    None => value,
                    Some(greatest) => {
                        if value > greatest {
                            value
                        } else {
                            greatest
                        }
                    }
                });
            }
        }

        // Store the result for this row
        greatest_values.push(max_value.map(|v| v.to_string()));
    }

    // Convert the vector to a `StringArray`
    let result = StringArray::from(greatest_values);
    Ok(Arc::new(result))
}
