import json
from datetime import datetime, timezone
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer
from config import env

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
PROCESSED_TOPIC = env("PROCESSED_TOPIC", "processed_data_topic1")
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "processed_writer_topic1")

INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
INFLUX_BUCKET = env("INFLUX_BUCKET", "iot_bucket")
INFLUX_ORG = env("INFLUX_ORG", "ukma")
INFLUX_TOKEN = env("INFLUX_TOKEN")
INFLUX_MEASUREMENT = env("INFLUX_MEASUREMENT", "processed_data")

consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)

write_api = client.write_api(write_options=SYNCHRONOUS)

def ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

print("Очікування даних з Kafka для запису в InfluxDB...")

for message in consumer:
    try:
        raw_text = message.value.decode("utf-8")
        data = json.loads(raw_text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        continue

    print(f"Готово до запису: {data.get('metric')} | Sensor: {data.get('sensor_type')}")

    try:
        metric = str(data["metric"])
        sensor_type = str(data["sensor_type"])
        datastream_id = str(data["datastream_id"])
        window_start = int(data["window_start"])
        window_end = int(data["window_end"])
        count = int(data["count"])
        latency = int(data["latency"])
        result_time = int(data["result_time"])
        max_event_time = int(data["max_event_time"])
        window_duration_sec = (window_end - window_start) / 1000.0
        throughput = count / window_duration_sec if window_duration_sec > 0 else 0.0
    except (KeyError, TypeError, ValueError) as e:
        print(f"Відсутнє поле {e}, пропускаємо запис.")
        continue

    p = None

    if sensor_type == "analog":
        try:
            min_value = float(data["min_value"])
            max_value = float(data["max_value"])
            average = float(data["average"])
            p = (Point(INFLUX_MEASUREMENT)
                .tag("sensor_type", sensor_type)
                .tag("metric", metric)
                .tag("datastream_id", datastream_id)
                .time(ms_to_datetime(window_end))
                .field("window_start", window_start)
                .field("window_end", window_end)
                .field("count", count)
                .field("min_value", min_value)
                .field("max_value", max_value)
                .field("average", average)
                .field("result_time", result_time)
                .field("max_event_time", max_event_time)
                .field("window_duration_sec", window_duration_sec)
                .field("latency", latency)
                .field("throughput", throughput))
        except (KeyError, TypeError, ValueError):
            pass

    elif sensor_type == "counter":
        try:
            delta = float(data["delta"])
            first_value = float(data["first_value"])
            last_value = float(data["last_value"])
            p = (Point(INFLUX_MEASUREMENT)
                .tag("sensor_type", sensor_type)
                .tag("metric", metric)
                .tag("datastream_id", datastream_id)
                .time(ms_to_datetime(window_end))
                .field("window_start", window_start)
                .field("window_end", window_end)
                .field("count", count)
                .field("first_value", first_value)
                .field("last_value", last_value)
                .field("delta", delta)
                 .field("result_time", result_time)
                 .field("max_event_time", max_event_time)
                 .field("window_duration_sec", window_duration_sec)
                 .field("latency", latency)
                 .field("throughput", throughput))

        except (KeyError, TypeError, ValueError):
            pass

    elif sensor_type == "state":
        try:
            on_count = int(data["on_count"])
            off_count = int(data["off_count"])
            distinct_counts = int(data["distinct_counts"])
            p = (Point(INFLUX_MEASUREMENT)
                .tag("sensor_type", sensor_type)
                .tag("metric", metric)
                .tag("datastream_id", datastream_id)
                .time(ms_to_datetime(window_end))
                .field("window_start", window_start)
                .field("window_end", window_end)
                .field("count", count)
                .field("on_count", on_count)
                .field("off_count", off_count)
                .field("distinct_counts", distinct_counts)
                .field("result_time", result_time)
                .field("max_event_time", max_event_time)
                .field("window_duration_sec", window_duration_sec)
                .field("latency", latency)
                .field("throughput", throughput))

        except (KeyError, TypeError, ValueError):
            pass

    elif sensor_type == "alarm":
        try:
            true_ratio = float(data["true_ratio"])
            false_count = int(data["false_count"])
            true_count = int(data["true_count"])
            p = (Point(INFLUX_MEASUREMENT)
                .tag("sensor_type", sensor_type)
                .tag("metric", metric)
                .tag("datastream_id", datastream_id)
                .time(ms_to_datetime(window_end))
                .field("window_start", window_start)
                .field("window_end", window_end)
                .field("count", count)
                .field("true_ratio", true_ratio)
                .field("false_count", false_count)
                .field("true_count", true_count)
                .field("result_time", result_time)
                .field("max_event_time", max_event_time)
                .field("window_duration_sec", window_duration_sec)
                .field("latency", latency)
                .field("throughput", throughput))
        except (KeyError, TypeError, ValueError):
            pass

    if p is not None:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)