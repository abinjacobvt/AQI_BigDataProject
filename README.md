# Air Quality Monitoring and Pollution Analytics  
Using Distributed Big Data Architecture

---

## Project Overview

This project implements a **fully distributed Big Data pipeline** to monitor, analyze, and visualize **Air Quality Index (AQI)** and air pollutant metrics across major French cities.

The system demonstrates an **end-to-end Big Data architecture** using **Apache Kafka, Apache Spark, HDFS, InfluxDB, and Grafana**, deployed on a distributed cluster.  
Air quality data is ingested from a public REST API, processed using both **streaming and batch analytics**, and visualized through interactive dashboards.

This project validates core Big Data concepts such as **scalability, fault tolerance, decoupling, distributed storage, and time-series analytics**.

---

## Cities Analyzed

- Paris  
- Lyon  
- Marseille  
- Lille  
- Toulouse  

**Observation Period:**  
30 December 2025 – 05 January 2026 (7 days)

---

## System Architecture

WAQI REST API
↓
Python Kafka Producer
↓
Apache Kafka (airquality.raw)
↓
Spark Structured Streaming
↓
HDFS (Raw AQI Data - Parquet)
↓
Spark Batch Processing
↓
InfluxDB (Daily AQI KPIs)
↓
Grafana Dashboards


---

## Technologies Used

- **Apache Kafka** – Distributed data ingestion  
- **Apache Spark** – Structured Streaming and Batch processing  
- **Apache Hadoop HDFS** – Distributed storage  
- **InfluxDB** – Time-series database  
- **Grafana** – Data visualization  
- **Python** – Producer and Spark jobs  
- **Linux** – Distributed cluster environment  

---

## Cluster Deployment

| Component | Node |
|---------|------|
| ZooKeeper | Master |
| Kafka Broker | Master |
| Spark Master | Master |
| Spark Workers | Worker |
| HDFS NameNode | Master |
| HDFS DataNode | Worker |
| YARN ResourceManager | Master |
| InfluxDB | Master |
| Grafana | Master |

---

## Data Source

- **Provider:** World Air Quality Index (WAQI)
- **API Type:** REST (JSON)
- **Authentication:** Token-based
- **Granularity:** City-level
- **Update Frequency:** Near real-time

### API Example

https://api.waqi.info/feed/{city}/?token=YOUR_API_TOKEN



---

## Pollutants and Metrics

- AQI (Air Quality Index)
- PM2.5
- PM10
- NO₂
- O₃
- CO
- SO₂
- Dominant Pollutant
- Timestamp (UTC)

---

## Repository Structure

BigDataProjects/
├── configs/
│ ├── hdfs/
│ ├── kafka/
│ ├── spark/
│ ├── influxdb/
│ └── zookeeper/
├── producer/
│ └── aqi_producer.py
├── spark/
│ ├── kafka_to_hdfs.py
│ └── daily_kpi_to_influx.py
├── dashboards/
│ └── grafana/
├── docs/
│ └── influxdb_schema.md
├── .gitignore
└── README.md

---

## Pipeline Execution

### 1. Start Services (Master Node)

sudo systemctl start zookeeper
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
start-dfs.sh
start-yarn.sh
sudo systemctl start influxdb
sudo systemctl start grafana-server


---

### 2. Create Kafka Topic

$KAFKA_HOME/bin/kafka-topics.sh
--create
--topic airquality.raw
--bootstrap-server master:9092
--replication-factor 1
--partitions 1



---

### 3. Spark Structured Streaming (Kafka → HDFS)

cd ~/aqi_project/spark
spark-submit kafka_to_hdfs.py



This job continuously consumes AQI data from Kafka, parses JSON records, and stores raw data in HDFS using Parquet format.

---

### 4. AQI Producer (API → Kafka)

cd ~/aqi_project/producer
source ~/aqi_project/venv/bin/activate
python aqi_producer.py



This script fetches AQI data for each city and publishes messages to Kafka.

---

### 5. Spark Batch Job (HDFS → InfluxDB)

cd ~/aqi_project/spark
spark-submit daily_kpi_to_influx.py



This job computes daily average AQI and PM2.5 values per city and writes aggregated results to InfluxDB.

---

## Grafana Dashboard

### Access Grafana via SSH Tunnel

ssh -L 3001:localhost:3000 user@master



### Open in Browser

http://localhost:3001


The dashboards display daily AQI and PM2.5 trends with city-wise comparisons.

---

## Key Observations

- Paris and Lyon consistently showed higher AQI and PM2.5 levels
- Marseille exhibited comparatively lower pollution levels
- PM2.5 emerged as the dominant pollutant
- Day-to-day pollution variability was observed

---

## Distributed System Validation

- Kafka decouples ingestion from processing
- Spark jobs require active worker nodes
- HDFS enforces block replication policies
- ZooKeeper coordinates distributed services
- Fault tolerance and data replay were validated

---

## Conclusion

This project demonstrates a **production-grade distributed Big Data system** for environmental monitoring.  
The architecture is scalable and extensible to support longer observation periods, additional cities, and predictive pollution analytics.

---

## Authors

- Abin Jacob   

---

## Supervising Professors

- Dr. Yiru Zhang  
- Dr. Li Yue  

---

## GitHub Repository

https://github.com/abinjacobvt/BigDataProjects
