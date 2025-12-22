from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Kafka_AQI_To_HDFS_Batch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

# BATCH READ FROM KAFKA
kafka_df = (
    spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.0.0.76:9092")
        .option("subscribe", "airquality.raw")
        .option("startingOffsets", "earliest")
        .load()
)

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_df.write \
    .mode("append") \
    .parquet("/data/air/raw")

spark.stop()
