// tests/integration.rs

use arrow::array::*;
use datafusion_common::Result;
use datafusion_greatest::greatest::greatest_inner;
use std::sync::Arc;

#[test]
fn test_greatest_with_matching_types() -> Result<()> {
    let array1 = Int32Array::from(vec![1, 2, 3]);
    let array2 = Int32Array::from(vec![4, 5, 6]);

    let result = greatest_inner(&[Arc::new(array1), Arc::new(array2)])?;
    let result_int32 = result
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Failed to downcast to Int32Array");

    let expected = Int32Array::from(vec![4, 5, 6]);
    assert_eq!(result_int32, &expected);

    Ok(())
}

#[test]
fn test_greatest_with_incorrect_types() -> Result<()> {
    let array1 = Int32Array::from(vec![1, 2, 3]);
    let array2 = Float32Array::from(vec![4.0, 5.0, 6.0]);

    let result = greatest_inner(&[Arc::new(array1), Arc::new(array2)]);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Error during type coercion: Failed to get wider type between Int32 and Float32"
    );

    Ok(())
}
