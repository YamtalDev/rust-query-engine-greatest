// // tests/greatest.rs

// use arrow::array::*;
// use arrow::datatypes::{DataType, Field, Schema};
// use arrow::record_batch::RecordBatch;
// use datafusion_common::Result;
// use datafusion_functions_nested::greatest::greatest_inner;
// use std::sync::Arc;

// #[tokio::test]
// async fn test_greatest_function() -> Result<()> {
//     // Define schema
//     let schema = Arc::new(Schema::new(vec![
//         Field::new("col1", DataType::Int32, true),
//         Field::new("col2", DataType::Int32, true),
//     ]));

//     // Create RecordBatch
//     let batch = RecordBatch::try_new(
//         schema.clone(),
//         vec![
//             Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
//         ],
//     )?;

//     // Call greatest_inner
//     let result = greatest_inner(&[batch.column(0).clone(), batch.column(1).clone()])?;
//     let result_int32 = result
//         .as_any()
//         .downcast_ref::<Int32Array>()
//         .expect("Failed to downcast to Int32Array");

//     let expected = Int32Array::from(vec![4, 5, 6]);
//     assert_eq!(result_int32, &expected);

//     Ok(())
// }

// #[tokio::test]
// async fn test_greatest_function_with_nulls() -> Result<()> {
//     // Define schema
//     let schema = Arc::new(Schema::new(vec![
//         Field::new("col1", DataType::Int32, true),
//         Field::new("col2", DataType::Int32, true),
//     ]));

//     // Create RecordBatch with nulls
//     let batch = RecordBatch::try_new(
//         schema.clone(),
//         vec![
//             Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
//             Arc::new(Int32Array::from(vec![Some(4), Some(5), None])) as ArrayRef,
//         ],
//     )?;

//     // Call greatest_inner
//     let result = greatest_inner(&[batch.column(0).clone(), batch.column(1).clone()])?;
//     let result_int32 = result
//         .as_any()
//         .downcast_ref::<Int32Array>()
//         .expect("Failed to downcast to Int32Array");

//     let expected = Int32Array::from(vec![4, 5, 3]);
//     assert_eq!(result_int32, &expected);

//     Ok(())
// }
