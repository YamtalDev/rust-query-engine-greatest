use arrow::array::{Float32Array, Int32Array, StringArray};
use rust_query_engine_greatest::greatest::greatest;
use std::sync::Arc;

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_integration_greatest_mixed_arrays() {
        let array1 = Int32Array::from(vec![Some(1), None, Some(3)]);
        let array2 = Float32Array::from(vec![Some(2.5), Some(4.4), None]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]);
        assert!(result.is_err()); // Should fail due to type mismatch
    }

    #[test]
    fn test_integration_greatest_empty() {
        let array1: Int32Array = Int32Array::from(vec![Option::<i32>::None; 0]);
        let array2: Int32Array = Int32Array::from(vec![Option::<i32>::None; 0]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.len(), 0); // Should be empty
    }

    #[test]
    fn test_integration_greatest_strings() {
        let array1 = StringArray::from(vec![Some("apple"), Some("banana"), None]);
        let array2 = StringArray::from(vec![Some("orange"), None, Some("grape")]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "orange");
        assert_eq!(result_array.value(1), "banana");
        assert_eq!(result_array.value(2), "grape");
    }
}
