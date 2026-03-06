import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from config import env

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
DLQ_TOPIC = env("DLQ_TOPIC", "dead_letter")

INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
INFLUX_BUCKET = env("INFLUX_BUCKET", "iot_bucket")
INFLUX_ORG = env("INFLUX_ORG", "ukma")
INFLUX_TOKEN = env("INFLUX_TOKEN")

consumer = KafkaConsumer(
    DLQ_TOPIC,
    group_id="dlq_writer_v3",
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    api_version=(0, 10, 2),

)

client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(write_options=SYNCHRONOUS)

print("Моніторинг DLQ запущено...")

for message in consumer:
    try:
        raw_payload = message.value.decode("utf-8")
        data = json.loads(raw_payload)

        if isinstance(data, str):
            data = json.loads(data)

        reason = data.get("error", "unknown")

        p = (
            Point("dlq_metrics")
            .tag("reason", reason)
            .tag("stage", "flink_decoder")
            .field("dlq_count", 1)
            .time(datetime.now(timezone.utc))
        )

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
        print(f"Записано помилку в InfluxDB: {reason}")

    except Exception as e:
        print(f"Помилка обробки DLQ повідомлення: {e}")