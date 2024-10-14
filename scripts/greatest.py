import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import greatest

# Set SPARK_HOME to the correct path where Spark is located
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/home/talas/dev/rust/rust-query-engine-greatest/spark'  # Set this to your actual Spark path

# Set the PYTHONPATH to include Spark Python directories
os.environ['PYTHONPATH'] = os.path.join(os.environ['SPARK_HOME'], 'python') + ":" + os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-0.10.9.7-src.zip')

# Create a Spark session
spark = SparkSession.builder.master("local").appName("GreatestFunction").getOrCreate()

# Create a sample dataframe
df = spark.createDataFrame([(1, 4, 3), (None, 5, 2), (6, None, None)], ['a', 'b', 'c'])

# Apply the greatest function
result = df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()

# Print results
for row in result:
    print(f"Greatest: {row.greatest}")
