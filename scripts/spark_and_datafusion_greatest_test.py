# from pyspark.sql import SparkSession
# from pyspark.sql.functions import greatest as spark_greatest, col
# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     IntegerType,
#     DoubleType,
#     DateType,
#     TimestampType,
# )
# from datetime import datetime, date

# # Import the Rust module
# import greatest

# # Initialize SparkSession
# spark = SparkSession.builder.appName("GreatestFunctionTests").getOrCreate()

# def compare_results(rust_result, spark_result):
#     assert rust_result == spark_result, f"Rust and Spark results differ:\nRust: {rust_result}\nSpark: {spark_result}"
#     print("Results match between Rust and Spark.")

# def test_greatest_int32():
#     print("Running test_greatest_int32")
#     data = [
#         (1, 2, 7),
#         (4, None, 5),
#         (3, 6, None),
#         (None, 8, 9),
#     ]
#     columns = ["col1", "col2", "col3"]
#     df = spark.createDataFrame(data, columns)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     spark_result = df.select("greatest_value_spark").collect()
#     spark_values = [row["greatest_value_spark"] for row in spark_result]
#     df.show()

#     # Prepare data for Rust function: list of lists (columns)
#     input_columns = [
#         [row[0] for row in data],  # col1
#         [row[1] for row in data],  # col2
#         [row[2] for row in data],  # col3
#     ]

#     # Call Rust function
#     rust_result = greatest.run_greatest_query(input_columns)
#     print("Rust result:", rust_result)
#     print("Spark result:", spark_values)

#     # Compare results
#     compare_results(rust_result, spark_values)
#     print("Test greatest_int32 passed.\n")

# def test_greatest_with_nulls():
#     print("Running test_greatest_with_nulls")
#     data = [
#         (None, None, None),
#         (None, 2, None),
#         (None, None, 3),
#     ]
#     columns = ["col1", "col2", "col3"]
#     schema = StructType([
#         StructField("col1", IntegerType(), True),
#         StructField("col2", IntegerType(), True),
#         StructField("col3", IntegerType(), True),
#     ])
#     df = spark.createDataFrame(data, schema=schema)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     spark_result = df.select("greatest_value_spark").collect()
#     spark_values = [row["greatest_value_spark"] for row in spark_result]
#     df.show()

#     # Prepare data for Rust function: list of lists (columns)
#     input_columns = [
#         [row[0] for row in data],  # col1
#         [row[1] for row in data],  # col2
#         [row[2] for row in data],  # col3
#     ]

#     # Call Rust function
#     rust_result = greatest.run_greatest_query(input_columns)
#     print("Rust result:", rust_result)
#     print("Spark result:", spark_values)

#     # Compare results
#     compare_results(rust_result, spark_values)
#     print("Test greatest_with_nulls passed.\n")

# def test_greatest_all_nulls():
#     print("Running test_greatest_all_nulls")
#     data = [
#         (None, None),
#         (None, None),
#         (None, None),
#     ]
#     columns = ["col1", "col2"]
#     schema = StructType([
#         StructField("col1", IntegerType(), True),
#         StructField("col2", IntegerType(), True),
#     ])
#     df = spark.createDataFrame(data, schema=schema)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     spark_result = df.select("greatest_value_spark").collect()
#     spark_values = [row["greatest_value_spark"] for row in spark_result]
#     df.show()

#     # Prepare data for Rust function: list of lists (columns)
#     input_columns = [
#         [row[0] for row in data],  # col1
#         [row[1] for row in data],  # col2
#     ]

#     # Call Rust function
#     rust_result = greatest.run_greatest_query(input_columns)
#     print("Rust result:", rust_result)
#     print("Spark result:", spark_values)

#     # Compare results
#     compare_results(rust_result, spark_values)
#     print("Test greatest_all_nulls passed.\n")

# def test_greatest_type_coercion():
#     print("Running test_greatest_type_coercion")
#     data = [
#         (1, 4.4, 7.7),
#         (2, 5.5, 8.8),
#         (3, 6.6, 9.9),
#     ]
#     columns = ["col1", "col2", "col3"]
#     schema = StructType([
#         StructField("col1", IntegerType(), True),
#         StructField("col2", DoubleType(), True),
#         StructField("col3", DoubleType(), True),
#     ])
#     df = spark.createDataFrame(data, schema=schema)
#     # Spark should coerce integers to floats
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     spark_result = df.select("greatest_value_spark").collect()
#     spark_values = [row["greatest_value_spark"] for row in spark_result]
#     df.show()

#     # Note: Rust function currently only handles Int32. Type coercion to Float is handled by Spark, but Rust does not support it.
#     # Thus, we skip comparison for this test.
#     print("Skipping comparison for test_greatest_type_coercion as Rust function only handles Int32.\n")

# def test_greatest_large_number_of_arguments():
#     print("Running test_greatest_large_number_of_arguments")
#     num_columns = 1000
#     data = [tuple(i for _ in range(num_columns)) for i in range(3)]
#     columns = [f"col{i}" for i in range(num_columns)]
#     df = spark.createDataFrame(data, columns)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     spark_result = df.select("greatest_value_spark").collect()
#     spark_values = [row["greatest_value_spark"] for row in spark_result]
#     df.select("greatest_value_spark").show()

#     # Prepare data for Rust function: list of lists (columns)
#     input_columns = [
#         [row[i] for row in data] for i in range(num_columns)
#     ]

#     # Call Rust function
#     rust_result = greatest.run_greatest_query(input_columns)
#     print("Rust result:", rust_result)
#     print("Spark result:", spark_values)

#     # Compare results
#     compare_results(rust_result, spark_values)
#     print("Test greatest_large_number_of_arguments passed.\n")

# def test_greatest_with_nulls():
#     # Already handled above as test_greatest_with_nulls
#     pass

# def test_greatest_mixed_types_error():
#     print("Running test_greatest_mixed_types_error")
#     data = [
#         (1, "a"),
#         (2, "b"),
#         (3, "c"),
#     ]
#     columns = ["col1", "col2"]
#     df = spark.createDataFrame(data, columns)
#     try:
#         df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#         df.show()
#     except Exception as e:
#         print("Expected error:", e)
#     print("Test greatest_mixed_types_error passed.\n")

# def test_greatest_with_infinities():
#     print("Running test_greatest_with_infinities")
#     data = [
#         (float('-inf'), 0.0),
#         (1.0, float('inf')),
#         (2.0, 1.5),
#     ]
#     columns = ["col1", "col2"]
#     schema = StructType([
#         StructField("col1", DoubleType(), True),
#         StructField("col2", DoubleType(), True),
#     ])
#     df = spark.createDataFrame(data, schema=schema)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     spark_result = df.select("greatest_value_spark").collect()
#     spark_values = [row["greatest_value_spark"] for row in spark_result]
#     df.show()

#     # Note: Rust function currently only handles Int32. Handling infinities would require support for Float types.
#     # Thus, we skip comparison for this test.
#     print("Skipping comparison for test_greatest_with_infinities as Rust function only handles Int32.\n")

# def test_greatest_date():
#     print("Running test_greatest_date")
#     data = [
#         (date(2020, 1, 1), date(2020, 3, 1)),
#         (date(2020, 6, 1), date(2020, 5, 1)),
#         (None, date(2020, 7, 1)),
#     ]
#     columns = ["col1", "col2"]
#     schema = StructType([
#         StructField("col1", DateType(), True),
#         StructField("col2", DateType(), True),
#     ])
#     df = spark.createDataFrame(data, schema=schema)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     df.show()

#     # Note: Rust function currently only handles Int32. Handling dates would require extending the Rust function.
#     # Thus, we skip comparison for this test.
#     print("Skipping comparison for test_greatest_date as Rust function only handles Int32.\n")

# def test_greatest_timestamp():
#     print("Running test_greatest_timestamp")
#     data = [
#         (datetime(2020, 1, 1, 12, 0, 0), datetime(2020, 3, 1, 15, 30, 0)),
#         (datetime(2020, 6, 1, 8, 0, 0), datetime(2020, 5, 1, 20, 45, 0)),
#         (None, datetime(2020, 7, 1, 10, 15, 0)),
#     ]
#     columns = ["col1", "col2"]
#     schema = StructType([
#         StructField("col1", TimestampType(), True),
#         StructField("col2", TimestampType(), True),
#     ])
#     df = spark.createDataFrame(data, schema=schema)
#     df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#     df.show(truncate=False)

#     # Note: Rust function currently only handles Int32. Handling timestamps would require extending the Rust function.
#     # Thus, we skip comparison for this test.
#     print("Skipping comparison for test_greatest_timestamp as Rust function only handles Int32.\n")

# def test_greatest_invalid_number_of_arguments():
#     print("Running test_greatest_invalid_number_of_arguments")
#     data = [
#         (1,),
#         (2,),
#         (3,),
#     ]
#     columns = ["col1"]
#     df = spark.createDataFrame(data, columns)
#     try:
#         df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#         df.show()
#     except Exception as e:
#         print("Expected error:", e)
#     print("Test greatest_invalid_number_of_arguments passed.\n")

# def test_greatest_zero_arguments():
#     print("Running test_greatest_zero_arguments")
#     df = spark.createDataFrame([(1,)], ["col1"])
#     try:
#         df = df.withColumn("greatest_value_spark", spark_greatest())
#         df.show()
#     except Exception as e:
#         print("Expected error:", e)
#     print("Test greatest_zero_arguments passed.\n")

# def test_greatest_code_generation_limit():
#     print("Running test_greatest_code_generation_limit")
#     num_columns = 2000
#     data = [tuple(i for _ in range(num_columns)) for i in range(1)]
#     columns = [f"col{i}" for i in range(num_columns)]
#     df = spark.createDataFrame(data, columns)
#     try:
#         df = df.withColumn("greatest_value_spark", spark_greatest(*columns))
#         spark_result = df.select("greatest_value_spark").collect()
#         spark_values = [row["greatest_value_spark"] for row in spark_result]
#         df.select("greatest_value_spark").show()
#     except Exception as e:
#         print("Expected error:", e)

#     # Prepare data for Rust function: list of lists (columns)
#     input_columns = [
#         [row[i] for row in data] for i in range(num_columns)
#     ]

#     # Call Rust function
#     rust_result = greatest.run_greatest_query(input_columns)
#     print("Rust result:", rust_result)
#     print("Spark result:", spark_values)

#     # Compare results
#     compare_results(rust_result, spark_values)
#     print("Test greatest_code_generation_limit passed.\n")

# def main():
#     test_greatest_int32()
#     test_greatest_with_nulls()
#     test_greatest_all_nulls()
#     test_greatest_mixed_types_error()
#     test_greatest_type_coercion()
#     test_greatest_large_number_of_arguments()
#     test_greatest_with_infinities()
#     test_greatest_date()
#     test_greatest_timestamp()
#     test_greatest_invalid_number_of_arguments()
#     test_greatest_zero_arguments()
#     test_greatest_code_generation_limit()

#     # Stop the SparkSession
#     spark.stop()

# if __name__ == "__main__":
#     main()

import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import greatest as spark_greatest, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException
from datetime import datetime, date

from pyspark.errors.exceptions.base import PySparkValueError

# Import the Rust module
import greatest  # Ensure that the Rust module is correctly installed and accessible

def initialize_spark():
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .appName("GreatestFunctionTests") \
        .master("local[*]") \
        .getOrCreate()
    return spark

def run_test(test_name, spark, df, expected_column, float_comparison=False, tolerance=1e-6, error_substrings=None):
    """
    Runs a test by comparing Spark's greatest function output with Rust's greatest function output.

    Parameters:
    - test_name: Name of the test.
    - spark: Spark session.
    - df: Input DataFrame.
    - expected_column: List of expected results (optional, primarily for reference).
    - float_comparison: Boolean indicating if approximate float comparison is needed.
    - tolerance: Allowed difference for floating-point comparisons.
    - error_substrings: List of substrings expected in the error message (for error tests).

    Returns:
    - None
    """
    try:
        if error_substrings is None:
            # Compute Spark's greatest
            spark_result_df = df.withColumn("spark_greatest", spark_greatest(*[col(c) for c in df.columns]))
            spark_result = spark_result_df.select("spark_greatest").collect()
            spark_actual = [row['spark_greatest'] for row in spark_result]

            # Compute Rust's greatest
            # Assuming Rust's greatest function can be called as greatest.greatest(*values)
            # Iterate over each row and apply Rust's greatest
            rust_actual = []
            for row in df.collect():
                # Extract the values for the greatest computation
                row_values = list(row)
                # Replace Spark's nulls with Python's None
                row_values = [v if v is not None else None for v in row_values]
                # Call Rust's greatest function
                rust_result = greatest.run_greatest([row_values])
                rust_actual.append(rust_result)

            # Compare Spark's and Rust's results
            if float_comparison:
                passed = True
                for i, (s_val, r_val) in enumerate(zip(spark_actual, rust_actual)):
                    if s_val is None and r_val is None:
                        continue
                    if (s_val is None) != (r_val is None):
                        print(f"Test '{test_name}' FAILED at index {i}. Spark Result: {s_val}, Rust Result: {r_val}")
                        passed = False
                        continue
                    if s_val is not None:
                        if isinstance(s_val, float) and math.isnan(s_val):
                            if not (isinstance(r_val, float) and math.isnan(r_val)):
                                print(f"Test '{test_name}' FAILED at index {i}. Spark Result: NaN, Rust Result: {r_val}")
                                passed = False
                        elif isinstance(s_val, float) and math.isinf(s_val):
                            if not (isinstance(r_val, float) and math.isinf(r_val) and (s_val > 0) == (r_val > 0)):
                                print(f"Test '{test_name}' FAILED at index {i}. Spark Result: {s_val}, Rust Result: {r_val}")
                                passed = False
                        else:
                            if not math.isclose(s_val, r_val, rel_tol=tolerance, abs_tol=tolerance):
                                print(f"Test '{test_name}' FAILED at index {i}. Spark Result: {s_val}, Rust Result: {r_val}")
                                passed = False
                if passed:
                    print(f"Test '{test_name}': PASSED.")
            else:
                assert spark_actual == rust_actual, f"Test '{test_name}' FAILED.\nSpark Result: {spark_actual}\nRust Result: {rust_actual}"
                print(f"Test '{test_name}': PASSED.")
        else:
            # Test expects an error
            # Attempt to compute Spark's greatest and Rust's greatest, expecting both to raise errors
            # Compute Spark's greatest
            try:
                spark_result_df = df.withColumn("spark_greatest", spark_greatest(*[col(c) for c in df.columns]))
                spark_result_df.collect()
                print(f"Test '{test_name}': FAILED. Expected an error but Spark's greatest did not raise one.")
            except (AnalysisException, PySparkValueError) as e:
                if any(sub in str(e) for sub in error_substrings):
                    print(f"Test '{test_name}': PASSED. Spark's greatest raised the expected error.")
                else:
                    print(f"Test '{test_name}': FAILED. Spark's greatest raised an unexpected error message: {str(e)}")

            # Attempt to compute Rust's greatest, expecting an error
            try:
                rust_actual = []
                for row in df.collect():
                    row_values = list(row)
                    row_values = [v if v is not None else None for v in row_values]
                    rust_result = greatest.run_greatest([row_values])
                    rust_actual.append(rust_result)
                print(f"Test '{test_name}': FAILED. Expected an error but Rust's greatest did not raise one.")
            except Exception as e:
                if any(sub in str(e) for sub in error_substrings):
                    print(f"Test '{test_name}': PASSED. Rust's greatest raised the expected error.")
                else:
                    print(f"Test '{test_name}': FAILED. Rust's greatest raised an unexpected error message: {str(e)}")
    except AssertionError as e:
        print(str(e))
    except Exception as e:
        print(f"Test '{test_name}' encountered an unexpected error: {str(e)}")

def test_greatest_int8(spark):
    """
    Tests the greatest function with Int8 arrays including null values.
    """
    data = [
        (1, 2, 7),
        (4, None, 5),
        (3, 6, None),
        (None, 8, 9),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", IntegerType(), True),
        StructField("col3", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [7, 5, 6, 9]
    run_test("test_greatest_int8", spark, df, expected)

def test_greatest_int32(spark):
    """
    Tests the greatest function with Int32 arrays including null values.
    """
    data = [
        (1, 2, 7),
        (4, None, 5),
        (3, 6, None),
        (None, 8, 9),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", IntegerType(), True),
        StructField("col3", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [7, 5, 6, 9]
    run_test("test_greatest_int32", spark, df, expected)

def test_greatest_float64_with_nan(spark):
    """
    Tests the greatest function with Float64 arrays including NaN values.
    In Spark, NaN is considered greater than any other numeric value.
    """
    data = [
        (1.1, None, 7.7),
        (None, 5.5, 8.8),
        (3.3, None, 9.9),
        (math.nan, 2.2, math.nan),
    ]
    schema = StructType([
        StructField("col1", FloatType(), True),
        StructField("col2", FloatType(), True),
        StructField("col3", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [7.7, 8.8, 9.9, math.nan]
    run_test("test_greatest_float64_with_nan", spark, df, expected, float_comparison=True)

def test_greatest_with_infinities(spark):
    """
    Tests the greatest function with Float64 arrays including Infinity and -Infinity values.
    """
    data = [
        (-math.inf, 0.0),
        (1.0, math.inf),
        (2.0, 1.5),
    ]
    schema = StructType([
        StructField("col1", FloatType(), True),
        StructField("col2", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [0.0, math.inf, 2.0]
    run_test("test_greatest_with_infinities", spark, df, expected, float_comparison=True)

def test_greatest_date32(spark):
    """
    Tests the greatest function with Date32 arrays including null values.
    """
    # Spark uses DateType for both Date32 and Date64
    data = [
        (date(2020, 1, 1), date(2020, 3, 1)),
        (date(2020, 6, 1), date(2020, 5, 1)),
        (None, date(2020, 7, 1)),
    ]
    schema = StructType([
        StructField("col1", DateType(), True),
        StructField("col2", DateType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [date(2020, 3, 1), date(2020, 6, 1), date(2020, 7, 1)]
    run_test("test_greatest_date32", spark, df, expected)

def test_greatest_date64(spark):
    """
    Tests the greatest function with Date64 arrays including null values.
    """
    # Spark uses DateType for both Date32 and Date64
    data = [
        (date(2020, 1, 1), date(2020, 3, 1)),
        (date(2020, 6, 1), date(2020, 5, 1)),
        (None, date(2020, 7, 1)),
    ]
    schema = StructType([
        StructField("col1", DateType(), True),
        StructField("col2", DateType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [date(2020, 3, 1), date(2020, 6, 1), date(2020, 7, 1)]
    run_test("test_greatest_date64", spark, df, expected)

def test_greatest_timestamp_ns(spark):
    """
    Tests the greatest function with TimestampNanosecond arrays including null values.
    """
    data = [
        (datetime.strptime("2020-01-01 12:00:00", "%Y-%m-%d %H:%M:%S"),
         datetime.strptime("2020-03-01 15:30:00", "%Y-%m-%d %H:%M:%S")),
        (datetime.strptime("2020-06-01 08:00:00", "%Y-%m-%d %H:%M:%S"),
         datetime.strptime("2020-05-01 20:45:00", "%Y-%m-%d %H:%M:%S")),
        (None, datetime.strptime("2020-07-01 10:15:00", "%Y-%m-%d %H:%M:%S")),
    ]
    schema = StructType([
        StructField("col1", TimestampType(), True),
        StructField("col2", TimestampType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [
        datetime.strptime("2020-03-01 15:30:00", "%Y-%m-%d %H:%M:%S"),
        datetime.strptime("2020-06-01 08:00:00", "%Y-%m-%d %H:%M:%S"),
        datetime.strptime("2020-07-01 10:15:00", "%Y-%m-%d %H:%M:%S"),
    ]
    run_test("test_greatest_timestamp_ns", spark, df, expected)

def test_greatest_utf8(spark):
    """
    Tests the greatest function with Utf8 arrays (StringType).
    """
    data = [
        ("apple", "apricot", "avocado"),
        ("banana", "blueberry", "blackberry"),
        ("cherry", "citrus", "cranberry"),
    ]
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = ["avocado", "blueberry", "cranberry"]
    run_test("test_greatest_utf8", spark, df, expected)

def test_greatest_large_utf8(spark):
    """
    Tests the greatest function with LargeUtf8 arrays (StringType in Spark).
    Spark does not differentiate between Utf8 and LargeUtf8, both use StringType.
    """
    data = [
        ("apple", "apricot", "avocado"),
        ("banana", "blueberry", "blackberry"),
        ("cherry", "citrus", "cranberry"),
    ]
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = ["avocado", "blueberry", "cranberry"]
    run_test("test_greatest_large_utf8", spark, df, expected)

def test_greatest_boolean(spark):
    """
    Tests the greatest function with Boolean arrays.
    In Spark, True > False.
    """
    data = [
        (True, False),
        (False, True),
        (None, True),
    ]
    schema = StructType([
        StructField("col1", BooleanType(), True),
        StructField("col2", BooleanType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [True, True, True]
    run_test("test_greatest_boolean", spark, df, expected)

def test_greatest_with_nulls(spark):
    """
    Tests the greatest function when arrays contain null values.
    """
    data = [
        (None, None, None),
        (None, 2, None),
        (None, None, 3),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", IntegerType(), True),
        StructField("col3", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [None, 2, 3]
    run_test("test_greatest_with_nulls", spark, df, expected)

def test_greatest_all_nulls(spark):
    """
    Tests the greatest function when all values are null.
    """
    data = [
        (None, None),
        (None, None),
        (None, None),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [None, None, None]
    run_test("test_greatest_all_nulls", spark, df, expected)

def test_greatest_all_nan(spark):
    """
    Tests the greatest function when all values are NaN.
    """
    data = [
        (math.nan, math.nan),
        (math.nan, math.nan),
    ]
    schema = StructType([
        StructField("col1", FloatType(), True),
        StructField("col2", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [math.nan, math.nan]
    run_test("test_greatest_all_nan", spark, df, expected, float_comparison=True)

def test_greatest_mixed_numeric_types(spark):
    """
    Tests the greatest function with mixed numeric types (Int32 and Float64).
    Spark coerces Int32 to Float64 for comparison.
    """
    data = [
        (1, 4.0),
        (2, 5.0),
        (3, 6.0),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), False),
        StructField("col2", FloatType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [4.0, 5.0, 6.0]
    run_test("test_greatest_mixed_numeric_types", spark, df, expected, float_comparison=True)

def test_greatest_type_coercion(spark):
    """
    Tests the greatest function with explicit type coercion.
    """
    data = [
        (1.0, 4.4, 7.7),
        (2.0, 5.5, 8.8),
        (3.0, 6.6, 9.9),
    ]
    schema = StructType([
        StructField("col1", FloatType(), False),
        StructField("col2", FloatType(), False),
        StructField("col3", FloatType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    expected = [7.7, 8.8, 9.9]
    run_test("test_greatest_type_coercion", spark, df, expected, float_comparison=True)

def test_greatest_mixed_types_error(spark):
    """
    Tests the greatest function with incompatible mixed types (Int32 and Utf8).
    Expecting an error due to type coercion failure.
    """
    data = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), False),
        StructField("col2", StringType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    expected_error_substrings = ["data type mismatch", "cannot resolve 'greatest"]

    # Modify run_test to handle expected errors by passing error_substrings
    run_test("test_greatest_mixed_types_error", spark, df, expected_column=None, error_substrings=expected_error_substrings)

def test_greatest_invalid_number_of_arguments(spark):
    """
    Tests the greatest function with insufficient number of arguments (only one column).
    Expecting an error.
    """
    data = [
        (1,),
        (2,),
        (3,),
    ]
    schema = StructType([
        StructField("col1", IntegerType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    expected_error_substrings = ["should take at least 2 columns", "requires at least"]

    run_test("test_greatest_invalid_number_of_arguments", spark, df, expected_column=None, error_substrings=expected_error_substrings)

def test_greatest_zero_arguments(spark):
    """
    Tests the greatest function with zero arguments.
    Expecting an error.
    """
    try:
        # Attempting to call greatest() with zero arguments using expr
        # Since 'greatest' requires at least two arguments, this should raise an error
        from pyspark.sql.functions import expr
        result_df = spark.createDataFrame([()], StructType([])).select(expr("greatest() as greatest"))
        result_df.collect()
        print("Test 'test_greatest_zero_arguments': FAILED. Expected an error due to zero arguments.")
    except (AnalysisException, PySparkValueError) as e:
        expected_error_substrings = ["should take at least 2 columns", "requires at least"]
        if any(sub in str(e) for sub in expected_error_substrings):
            print("Test 'test_greatest_zero_arguments': PASSED.")
        else:
            print(f"Test 'test_greatest_zero_arguments': FAILED. Unexpected error message: {str(e)}")

def test_greatest_large_number_of_arguments(spark):
    """
    Tests the greatest function with a large number of arguments to check performance.
    """
    num_arrays = 1000
    data = []
    for _ in range(3):  # Assuming 3 rows
        row = tuple([i for i in range(num_arrays)])
        data.append(row)
    schema_fields = [StructField(f"col{i}", IntegerType(), False) for i in range(num_arrays)]
    schema = StructType(schema_fields)
    df = spark.createDataFrame(data, schema)
    # The greatest value in each row is (num_arrays - 1)
    expected = [num_arrays - 1] * 3
    run_test("test_greatest_large_number_of_arguments", spark, df, expected)

def test_greatest_code_generation_limit(spark):
    """
    Tests the greatest function with a very large number of arguments to check code generation limits.
    """
    num_arrays = 2000
    data = [
        tuple([i for i in range(num_arrays)])
    ]  # Single row
    schema_fields = [StructField(f"col{i}", IntegerType(), False) for i in range(num_arrays)]
    schema = StructType(schema_fields)
    df = spark.createDataFrame(data, schema)
    expected = [num_arrays - 1]
    run_test("test_greatest_code_generation_limit", spark, df, expected)

def main():
    """Main function to run all tests."""
    spark = initialize_spark()

    print("Starting Greatest Function Tests...\n")

    # Basic Numeric Types
    test_greatest_int8(spark)
    test_greatest_int32(spark)
    test_greatest_float64_with_nan(spark)
    test_greatest_with_infinities(spark)

    # Date and Time Types
    test_greatest_date32(spark)
    test_greatest_date64(spark)
    test_greatest_timestamp_ns(spark)

    # String Types
    test_greatest_utf8(spark)
    test_greatest_large_utf8(spark)

    # Boolean Type
    test_greatest_boolean(spark)

    # Null Handling
    test_greatest_with_nulls(spark)
    test_greatest_all_nulls(spark)
    test_greatest_all_nan(spark)

    # Type Coercion and Mixed Types
    test_greatest_mixed_numeric_types(spark)
    test_greatest_type_coercion(spark)

    # Error Cases
    test_greatest_mixed_types_error(spark)
    test_greatest_invalid_number_of_arguments(spark)
    test_greatest_zero_arguments(spark)

    # Stress Tests
    test_greatest_large_number_of_arguments(spark)
    test_greatest_code_generation_limit(spark)

    print("\nAll tests completed.")
    spark.stop()

if __name__ == "__main__":
    main()
