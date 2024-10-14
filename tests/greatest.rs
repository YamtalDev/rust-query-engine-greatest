#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::prelude::*;
    use datafusion_expr::expr_fn::{col, greatest};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_greatest_function() -> datafusion_core::error::Result<()> {
        let mut ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, true),
            Field::new("col2", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(3), None])),
                Arc::new(Int32Array::from(vec![Some(2), None, Some(4)])),
            ],
        )?;

        ctx.register_batch("test_table", batch)?;

        let df = ctx.table("test_table").await?;
        let df = df.select(vec![
            col("col1"),
            col("col2"),
            greatest(vec![col("col1"), col("col2")]).alias("max_value"),
        ])?;

        let results = df.collect().await?;

        // Verify the results
        let batch = &results[0];
        let array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(array.value(0), 2);
        assert_eq!(array.value(1), 3);
        assert_eq!(array.value(2), 4);

        Ok(())
    }
}
