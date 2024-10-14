use arrow::array::Array;
use arrow::array::{Float32Array, Int32Array, StringArray};
use rust_query_engine_greatest::greatest::greatest;
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_greatest_int32_basic() {
        let array1 = Int32Array::from(vec![Some(1), None, Some(3)]);
        let array2 = Int32Array::from(vec![Some(2), Some(4), None]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.value(0), 2);
        assert_eq!(result_array.value(1), 4);
        assert_eq!(result_array.value(2), 3);
    }

    #[test]
    fn test_greatest_float32_basic() {
        let array1 = Float32Array::from(vec![Some(1.1), None, Some(3.3)]);
        let array2 = Float32Array::from(vec![Some(2.2), Some(4.4), None]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(result_array.value(0), 2.2);
        assert_eq!(result_array.value(1), 4.4);
        assert_eq!(result_array.value(2), 3.3);
    }

    #[test]
    fn test_greatest_strings_basic() {
        let array1 = StringArray::from(vec![Some("a"), Some("b"), None]);
        let array2 = StringArray::from(vec![Some("b"), None, Some("c")]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "b");
        assert_eq!(result_array.value(1), "b");
        assert_eq!(result_array.value(2), "c");
    }

    #[test]
    fn test_edge_case_null_columns() {
        let array1 = Int32Array::from(vec![None, None, None]);
        let array2 = Int32Array::from(vec![None, None, None]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(result_array.is_null(0));
        assert!(result_array.is_null(1));
        assert!(result_array.is_null(2));
    }

    #[test]
    fn test_edge_case_empty_arrays() {
        let array1 = Int32Array::from(vec![Option::<i32>::None; 0]);
        let array2 = Int32Array::from(vec![Option::<i32>::None; 0]);

        let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.len(), 0);
    }
}
