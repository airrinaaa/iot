# ВІДПОВІДАЄ ЗА ЗАПИС ЗАПІЗНІЛИХ ДАНИХ ДО БАЗИ ДАНИХ
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from config import env

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
LATE_DATA_TOPIC = env("LATE_DATA_TOPIC", "late_data_topic2")

INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
INFLUX_BUCKET = env("INFLUX_BUCKET", "iot_bucket")
INFLUX_ORG = env("INFLUX_ORG", "ukma")
INFLUX_TOKEN = env("INFLUX_TOKEN")

consumer = KafkaConsumer(
    LATE_DATA_TOPIC,
    group_id="late_data",
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    api_version=(0, 10, 2),
    enable_auto_commit=False,
    max_partition_fetch_bytes=20 * 1024 * 1024,
    fetch_max_bytes=20 * 1024 * 1024,
)

client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(write_options=SYNCHRONOUS)

print("Моніторинг запізнілих даних запущено...")

for message in consumer:
    try:
        raw_payload = message.value.decode("utf-8")
        data = json.loads(raw_payload)

        if isinstance(data, str):
            data = json.loads(data)


        datastream_id = data.get("datastream_id", "unknown")
        metric= data.get("metric", "unknown")
        late_sec = float(data.get("late_by_sec", 0.0))
        seq = str(data.get("seq", "NA"))
        event_datetime = datetime.fromisoformat(data["event_time"])
        p = (Point("late_events")
             .tag("datastream_id", datastream_id)
             .tag("metric", metric)
             .tag("seq", seq)
             .field("late_by_seconds", late_sec)
             .field("late_count", 1)
             .time(event_datetime))

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
        print(f"Записано {datastream_id[:8]} ({metric}) запізнився на {late_sec} сек.")
        consumer.commit()

    except Exception as e:
        print(f"Помилка обробки повідомлення: {e}")