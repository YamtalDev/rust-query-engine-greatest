use arrow::array::{Float32Array, Int32Array, StringArray};
use rust_query_engine_greatest::greatest::greatest;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyModule;

fn run_spark_greatest() -> PyResult<()> {
    Python::with_gil(|py| {
        // Load Python module and execute the PySpark code
        let sys = PyModule::import(py, "sys")?;
        sys.get("path")?.call_method1("append", ("./",))?;

        // Import the Python script (pyspark_greatest.py)
        let pyspark_greatest = PyModule::import(py, "pyspark_greatest")?;

        // Call the Spark session function from the Python script
        let result = pyspark_greatest.call_function0("main")?;

        println!("Spark result: {:?}", result);
        Ok(())
    })
}

fn main() {
    // Example 1: Int32
    let array1 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let array2 = Int32Array::from(vec![Some(2), Some(4), None]);
    let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
    println!("Greatest Int32: {:?}", result);

    // Example 2: Float32
    let array1 = Float32Array::from(vec![Some(1.1), None, Some(3.3)]);
    let array2 = Float32Array::from(vec![Some(2.2), Some(4.4), None]);
    let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
    println!("Greatest Float32: {:?}", result);

    // Example 3: Strings
    let array1 = StringArray::from(vec![Some("a"), Some("b"), None]);
    let array2 = StringArray::from(vec![Some("b"), None, Some("c")]);
    let result = greatest(&[Arc::new(array1), Arc::new(array2)]).unwrap();
    println!("Greatest String: {:?}", result);

    println!("\nTesting completed. For more detailed tests, please check the integration tests.");
}
