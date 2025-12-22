from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg
from influxdb import InfluxDBClient
from datetime import datetime

spark = SparkSession.builder \
    .appName("Daily_AQI_To_InfluxDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Read raw data
df = spark.read.parquet("/data/air/raw")

# 2. Extract date
df = df.withColumn("event_date", to_date(col("timestamp")))

# 3. Daily aggregation
daily_df = (
    df.groupBy("event_date", "city")
      .agg(avg("aqi").alias("avg_aqi"))
)

results = daily_df.collect()

# 4. InfluxDB connection
client = InfluxDBClient(
    host="localhost",
    port=8086,
    database="aqi_db"
)

# 5. Write idempotent points (1 per city per day)
points = []

for row in results:
    ts = datetime.strptime(str(row["event_date"]), "%Y-%m-%d")

    points.append({
        "measurement": "daily_aqi",
        "tags": {
            "city": row["city"]
        },
        "time": ts.strftime("%Y-%m-%dT00:00:00Z"),
        "fields": {
            "avg_aqi": float(row["avg_aqi"])
        }
    })

if points:
    client.write_points(points)

spark.stop()
client.close()

