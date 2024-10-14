from pyspark.sql import SparkSession
from pyspark.sql.functions import greatest

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("GreatestFunction").getOrCreate()

# Create a sample dataframe with similar data to your DataFusion tests
data = [(1, 4, 3), (None, 5, 2), (6, None, None)]
df = spark.createDataFrame(data, ['a', 'b', 'c'])

# Apply Spark's greatest function
spark_result = df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()

# Print Spark results
print("Spark Results:")
for row in spark_result:
    print(f"Greatest: {row.greatest}")

# Add a function to compare the results with your DataFusion implementation
def compare_results(datafusion_result, spark_result):
    if datafusion_result == spark_result:
        print("The results match!")
    else:
        print("The results differ.")

# Now call your DataFusion greatest function in Rust and pass the results to compare_results.
# You will need to call your Rust function externally or compare the results manually in your Rust code.
