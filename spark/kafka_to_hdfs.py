from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Kafka_AQI_To_HDFS") \
    .getOrCreate()

# AQI JSON schema
schema = StructType([
    StructField("city", StringType()),
    StructField("aqi", DoubleType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("co", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.0.0.76:9092") \
    .option("subscribe", "airquality.raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write to HDFS
query = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/data/air/raw") \
    .option("checkpointLocation", "/data/air/checkpoints/raw") \
    .start()

query.awaitTermination()
