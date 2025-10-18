from pyspark.sql import SparkSession

# task submit APP=delta-lake-test/delta_test.py

spark = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# Create a Delta table
data = spark.range(0, 5)
output= "/opt/spark/data/bronze/test_delta_table"

data.write.format("delta").mode("overwrite").save(output)

# Read data from the Delta table
df = spark.read.format("delta").load(output)
df.show()