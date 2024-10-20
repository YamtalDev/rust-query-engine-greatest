import greatest
from pyspark.sql import SparkSession
from pyspark.sql.functions import greatest as spark_greatest
import sys
print(sys.executable)

# Rest of the script...

def test_greatest():
    # Initialize Spark
    spark = SparkSession.builder.appName("GreatestTest").getOrCreate()

    # Create a sample DataFrame
    data = [
        (1, 4, 7),
        (2, 5, 1),
        (3, 6, 2)
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    # Apply Spark's greatest function
    spark_df = df.withColumn("spark_greatest", spark_greatest("col1", "col2", "col3"))
    spark_result = spark_df.select("spark_greatest").collect()

    # Prepare input for Rust function (transpose columns to rows)
    input_data = [
        [1, 2, 3],  # col1
        [4, 5, 6],  # col2
        [7, 1, 2]   # col3
    ]

    # Apply Rust's greatest function via Python bindings
    rust_result = greatest.run_greatest_query(input_data)

    # Extract Spark results
    spark_greatest_values = [row['spark_greatest'] for row in spark_result]

    # Compare results
    assert spark_greatest_values == rust_result, f"Mismatch:\nSpark: {spark_greatest_values}\nRust: {rust_result}"

    spark.stop()
    print("Test completed successfully and results match.")

if __name__ == "__main__":
    test_greatest()
