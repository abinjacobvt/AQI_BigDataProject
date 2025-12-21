from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg
from influxdb import InfluxDBClient

spark = SparkSession.builder \
    .appName("Daily_AQI_To_InfluxDB") \
    .getOrCreate()

# Read raw AQI data from HDFS
df = spark.read.parquet("/data/air/raw")

# Convert timestamp to date
df = df.withColumn("date", to_date(col("timestamp")))

# Compute daily average AQI per city
daily_df = (
    df.groupBy("date", "city")
      .agg(avg("aqi").alias("avg_aqi"))
)

# Collect results
results = daily_df.collect()

# Connect to InfluxDB
client = InfluxDBClient(host="localhost", port=8086, database="aqi_db")

points = []
for row in results:
    points.append({
        "measurement": "daily_aqi",
        "tags": {
            "city": row["city"]
        },
        "time": str(row["date"]),
        "fields": {
            "avg_aqi": float(row["avg_aqi"])
        }
    })

# Write to InfluxDB
client.write_points(points)

spark.stop()
