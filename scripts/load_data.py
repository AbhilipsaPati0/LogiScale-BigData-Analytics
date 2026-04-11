from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("LogiScale Data Processing") \
    .getOrCreate()

df = spark.read.csv("data/logistics_data.csv", header=True, inferSchema=True)

# Remove null values
df = df.dropna()

# Create Delay column
df = df.withColumn("Delay", col("ActualTime") - col("ETA"))

# Show data
df.show(5)

# Schema
df.printSchema()

# Summary stats (optional but good)
df.describe().show()