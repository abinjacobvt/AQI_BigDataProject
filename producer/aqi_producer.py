import json
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone

# =========================
# CONFIGURATION
# =========================

KAFKA_BROKER = "10.0.0.76:9092"
TOPIC = "airquality.raw"

API_TOKEN = "70ecec84dfc1b5f475745888b0ca12d143cbeae5"

# Cities from different regions of France
CITIES = {
    "Paris": "paris",
    "Lyon": "lyon",
    "Marseille": "marseille",
    "Lille": "lille",
    "Toulouse": "toulouse"
}

# =========================
# KAFKA PRODUCER
# =========================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# =========================
# FETCH AQI DATA
# =========================

def fetch_aqi(city_name, city_query):
    url = f"https://api.waqi.info/feed/{city_query}/?token={API_TOKEN}"
    response = requests.get(url, timeout=10)
    data = response.json()

    if data.get("status") != "ok":
        print(f"Failed to fetch AQI for {city_name}")
        return None

    iaqi = data["data"].get("iaqi", {})

    return {
        "city": city_name,
        "aqi": data["data"].get("aqi"),
        "pm25": iaqi.get("pm25", {}).get("v"),
        "pm10": iaqi.get("pm10", {}).get("v"),
        "no2": iaqi.get("no2", {}).get("v"),
        "o3": iaqi.get("o3", {}).get("v"),
        "co": iaqi.get("co", {}).get("v"),
        "so2": iaqi.get("so2", {}).get("v"),
        "dominant_pol": data["data"].get("dominentpol"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# =========================
# MAIN
# =========================

def main():
    print("Starting AQI Producer")

    for city, query in CITIES.items():
        record = fetch_aqi(city, query)
        if record:
            producer.send(TOPIC, record)
            print(f"Sent to Kafka: {record}")

    producer.flush()
    print("AQI data sent successfully")

if __name__ == "__main__":
    main()

