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
      .agg(
          avg("aqi").alias("avg_aqi"),
          avg("pm25").alias("avg_pm25")
      )
)

results = daily_df.collect()

# 4. InfluxDB connection
client = InfluxDBClient(
    host="localhost",
    port=8086,
    database="aqi_db"
)

points = []

for row in results:
    ts = datetime.strptime(str(row["event_date"]), "%Y-%m-%d")

    avg_aqi = float(row["avg_aqi"]) if row["avg_aqi"] is not None else 0.0
    avg_pm25 = float(row["avg_pm25"]) if row["avg_pm25"] is not None else 0.0

    points.append({
        "measurement": "daily_aqi",
        "tags": {
            "city": row["city"]
        },
        "time": ts.strftime("%Y-%m-%dT00:00:00Z"),
        "fields": {
            "avg_aqi": avg_aqi,
            "avg_pm25": avg_pm25
        }
    })

if points:
    client.write_points(points)

spark.stop()
client.close()

