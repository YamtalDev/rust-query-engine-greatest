use datafusion::arrow::array::*;
use datafusion_common::{DataFusionError, Result};
use datafusion_functions_nested::greatest::greatest_inner;
use std::sync::Arc;
use types::Int32Type;

fn main() -> Result<()> {
    // Test with Int32Array
    let input_int = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
        Arc::new(Int32Array::from(vec![7, 1, 2])) as ArrayRef,
    ];

    let greatest_int =
        greatest_inner(&input_int).map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let greatest_int_array = greatest_int
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Failed to downcast to Int32Array");
    println!("Greatest Int: {:?}", greatest_int_array);

    // Test with StringArray
    let input_utf8 = vec![
        Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["d", "e", "f"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["g", "h", "i"])) as ArrayRef,
    ];

    let greatest_utf8 =
        greatest_inner(&input_utf8).map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let greatest_utf8_array = greatest_utf8
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Failed to downcast to StringArray");
    println!("Greatest UTF8: {:?}", greatest_utf8_array);

    // Test with ListArray
    let list1 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5), Some(6)]),
        None,
    ])) as ArrayRef;

    let list2 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(7), Some(8), Some(9)]),
        Some(vec![Some(10), Some(11), Some(12)]),
        Some(vec![Some(13), Some(14)]),
    ])) as ArrayRef;

    let input_list = vec![list1, list2];
    let greatest_list =
        greatest_inner(&input_list).map_err(|e| DataFusionError::Execution(e.to_string()))?;
    println!("Greatest List: {:?}", greatest_list);

    Ok(())
}
