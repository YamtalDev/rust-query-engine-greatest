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

def run_test(test_name, spark, df, expected_column=None, float_comparison=False, tolerance=1e-6, error_substrings=None):
    try:
        columns = [df.select(c).rdd.flatMap(lambda x: x).collect() for c in df.columns]
        if error_substrings is None:
            # Get Spark result
            spark_result_df = df.withColumn("spark_greatest", spark_greatest(*[col(c) for c in df.columns]))
            spark_result = spark_result_df.select("spark_greatest").collect()
            spark_actual = [row['spark_greatest'] for row in spark_result]

            # Get Rust result
            rust_result = greatest.run_greatest(columns)

            # Comparison
            if float_comparison:
                passed = True
                for i, (s_val, r_val) in enumerate(zip(spark_actual, rust_result)):
                    if (s_val is None) != (r_val is None):
                        print(f"Test '{test_name}' FAILED at index {i}. Spark Result: {s_val}, Rust Result: {r_val}")
                        passed = False
                    elif s_val is not None and r_val is not None:
                        if math.isnan(s_val) and math.isnan(r_val):
                            continue  # Both are NaN, consider as equal
                        if not math.isclose(s_val, r_val, rel_tol=tolerance, abs_tol=tolerance):
                            print(f"Test '{test_name}' FAILED at index {i}. Spark Result: {s_val}, Rust Result: {r_val}")
                            passed = False
                if passed:
                    print(f"Test '{test_name}': PASSED.")
            else:
                if spark_actual == rust_result:
                    print(f"Test '{test_name}': PASSED.")
                else:
                    print(f"Test '{test_name}' FAILED.\nSpark Result: {spark_actual}\nRust Result: {rust_result}")
        else:
            # Error handling
            spark_error = None
            rust_error = None
            try:
                spark_result_df = df.withColumn("spark_greatest", spark_greatest(*[col(c) for c in df.columns]))
                spark_result_df.collect()
            except (AnalysisException, PySparkValueError) as e:
                spark_error = str(e)

            try:
                rust_result = greatest.run_greatest(columns)
            except Exception as e:
                rust_error = str(e)

            # Check if errors contain expected substrings
            spark_error_matched = spark_error and any(sub in spark_error for sub in error_substrings)
            rust_error_matched = rust_error and any(sub in rust_error for sub in error_substrings)

            if spark_error_matched and rust_error_matched:
                print(f"Test '{test_name}': PASSED. Both implementations raised the expected error.")
            else:
                if not spark_error_matched:
                    print(f"Test '{test_name}': FAILED. Spark did not raise the expected error.")
                if not rust_error_matched:
                    print(f"Test '{test_name}': FAILED. Rust did not raise the expected error.")
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
    expected_error_substrings = ["should take at least 2 columns", "requires at least", "requires > 1 parameters"]

    # Testing Spark implementation
    try:
        from pyspark.sql.functions import expr
        result_df = spark.createDataFrame([()], StructType([])).select(expr("greatest() as greatest"))
        result_df.collect()
        spark_error = None
        print("Test 'test_greatest_zero_arguments': FAILED. Expected an error due to zero arguments in Spark.")
    except Exception as e:
        spark_error = str(e)

    # Testing Rust implementation
    try:
        rust_result = greatest.run_greatest([])
        rust_error = None
        print("Test 'test_greatest_zero_arguments': FAILED. Expected an error due to zero arguments in Rust.")
    except Exception as e:
        rust_error = str(e)

    # Checking if errors contain expected substrings
    spark_error_matched = spark_error and any(sub in spark_error for sub in expected_error_substrings)
    rust_error_matched = rust_error and any(sub in rust_error for sub in expected_error_substrings)

    if spark_error_matched and rust_error_matched:
        print(f"Test 'test_greatest_zero_arguments': PASSED. Both implementations raised the expected error.")
    else:
        if not spark_error_matched:
            print(f"Test 'test_greatest_zero_arguments': FAILED. Spark did not raise the expected error.")
            print(f"Spark Error: {spark_error}")
        if not rust_error_matched:
            print(f"Test 'test_greatest_zero_arguments': FAILED. Rust did not raise the expected error.")
            print(f"Rust Error: {rust_error}")


def test_greatest_large_number_of_arguments(spark):
    """
    Tests the greatest function with a large number of arguments to check performance.
    """
    num_arrays = 20
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
    num_arrays = 20
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
