use datafusion::common::Result;
use datafusion::prelude::*;
use datafusion_functions_nested::greatest::{greatest, greatest_udf};
use std::sync::Arc;

// Import necessary arrow types
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::DataFusionError;

#[tokio::main]
async fn main() -> std::result::Result<(), DataFusionError> {
    // Create an execution context
    let mut ctx = SessionContext::new();

    // Register the UDF
    ctx.register_udf(greatest_udf().as_ref().clone());

    // Define a schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Int32, true),
        Field::new("col2", DataType::Int32, true),
        Field::new("col3", DataType::Int32, true),
    ]));

    // Create a record batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(10), Some(50), Some(1)])),
            Arc::new(Int32Array::from(vec![Some(20), Some(40), Some(2)])),
            Arc::new(Int32Array::from(vec![Some(30), Some(30), Some(3)])),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(e, None))?;

    // Register data as a table
    ctx.register_batch("my_table", batch)?;

    // Build a DataFrame using the `greatest` function
    let df = ctx.table("my_table").await?;

    let df = df.select(vec![
        col("col1"),
        col("col2"),
        col("col3"),
        greatest(vec![col("col1"), col("col2"), col("col3")]).alias("max_value"),
    ])?;

    // Collect the results
    let results = df.collect().await?;

    // Display the results
    for batch in results {
        println!("{:?}", batch);
    }

    Ok(())
}
