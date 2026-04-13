# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder \
    .appName("LogiScale Data Processing") \
    .getOrCreate()

# Load dataset
df = spark.read.csv("data/logistics_data.csv", header=True, inferSchema=True)

# Remove null values
df = df.dropna()

# Create Delay column
df = df.withColumn("Delay", col("ActualTime") - col("ETA"))

# Display sample data
df.show(5)

# Print schema
df.printSchema()

# Show summary statistics
df.describe().show()

# Calculate average delay per route
route_delay = df.groupBy("RouteID").agg(avg("Delay").alias("Avg_Delay"))
route_delay.show(10)

# Calculate average delay per driver
driver_delay = df.groupBy("DriverID").agg(avg("Delay").alias("Avg_Delay"))
driver_delay.show(10)

# Calculate average delay per warehouse
warehouse_delay = df.groupBy("Warehouse").agg(avg("Delay").alias("Avg_Delay"))
warehouse_delay.show(10)

# Define window for each route ordered by date
windowSpec = Window.partitionBy("RouteID").orderBy("Date")

# Calculate running average delay
df = df.withColumn("Running_Avg_Delay", avg("Delay").over(windowSpec))

# Show window function result
df.select("RouteID", "Date", "Delay", "Running_Avg_Delay").show(10)

# Export selected columns to CSV
df.select(
    "RouteID",
    "DriverID",
    "Warehouse",
    "Date",
    "Delay",
    "Running_Avg_Delay"
).toPandas().to_csv("output/final_data.csv", index=False)