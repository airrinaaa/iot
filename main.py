import json
import time
import io
import random
from config import env

from fastavro import schemaless_writer, parse_schema
import paho.mqtt.client as mqtt

from Device import Device, DeviceType

SCHEMA_PATH = env("SCHEMA_PATH", "observation.avsc")

MQTT_HOST = env("MQTT_HOST", "localhost")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = int(env("MQTT_KEEPALIVE", "60"))
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "sensors")
NUMBER_OF_DEVICES = int(env("NUMBER_OF_DEVICES", "10"))
SLEEP_TIME = float(env("SLEEP_TIME", "1"))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)


def run_simulation():
    client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_start()
    with open(SCHEMA_PATH, "rb") as f:
        raw_schema = json.load(f)
        schema = parse_schema(raw_schema)

    types_of_devices = [DeviceType.CLIMATE, DeviceType.UTILITY, DeviceType.KITCHEN, DeviceType.SECURITY]
    devices: list[Device] = []
    for i in range(NUMBER_OF_DEVICES):
        dev_type = types_of_devices[i % len(types_of_devices)]
        devices.append(Device.create_by_type(dev_type, i))


    ANOMALY_CYCLE_SEC = 600
    device_phase_offset = {
        device.thing_id: random.uniform(0, ANOMALY_CYCLE_SEC)
        for device in devices
    }

    start_time = time.time()
    while True:
        elapsed_time = time.time() - start_time
        for device in devices:
            current_observations = device.read_all()
            current_offset = device_phase_offset[device.thing_id]

            for observation in current_observations:
                data = observation.to_dict()

                personal_time = elapsed_time + current_offset
                cycle_time = personal_time % ANOMALY_CYCLE_SEC
                metric = data["metric"]

                if random.random() < 0.00001:
                    if metric in ["temperature", "voltage", "co2", "fridge"]:
                        data['value'] = random.choice([-999.0, 9999.0])
                    elif metric in ["humidity"]:
                        data['value'] = 500.0
                else:
                    if 240 < cycle_time < 250:
                        if metric == "temperature":
                            data['value'] = random.uniform(60.0, 85.0)
                        elif metric == "co2":
                            data['value'] = random.uniform(1500.0, 3500.0)
                        elif metric == "fridge":
                            data['value'] = random.uniform(15.0, 22.0)
                        elif metric == "voltage":
                            data['value'] = random.uniform(280.0, 310.0)
                        elif metric == "water":
                            data['value'] += random.uniform(100.0, 300.0)
                        elif metric == "door":
                            data['value'] = "ON" if int(personal_time * 5) % 2 == 0 else "OFF"
                        elif metric == "move":
                            data['value'] = True

                data['thing_id'] = str(data['thing_id'])
                data['datastream_id'] = str(data['datastream_id'])
                data['event_time'] = data['event_time'].isoformat()
                data['ingestion_time'] = data['ingestion_time'].isoformat()

                is_dlq = False
                avro_bytes = b""

                if random.random() < 0.00001:
                    is_dlq = True
                    error_type = random.choice(["bad_metric", "bad_time", "empty_id", "bad_bytes"])

                    if error_type == "bad_metric":
                        data['metric'] = "   "
                    elif error_type == "bad_time":
                        data['event_time'] = "2026-99-99 broken-time"
                    elif error_type == "empty_id":
                        data['datastream_id'] = "   "

                    if error_type == "bad_bytes":
                        avro_bytes = b"totally_invalid_avro_garbage_123456789"
                    else:
                        try:
                            bytes_io = io.BytesIO()
                            schemaless_writer(bytes_io, schema, data)
                            avro_bytes = bytes_io.getvalue()
                        except Exception:
                            avro_bytes = b""
                if not is_dlq:
                    bytes_io = io.BytesIO()
                    schemaless_writer(bytes_io, schema, data)
                    avro_bytes = bytes_io.getvalue()

                topic = f"{MQTT_TOPIC_PREFIX}/{device.device_type.value}"
                client.publish(topic, payload=avro_bytes)

        time.sleep(SLEEP_TIME)


if __name__ == '__main__':
    run_simulation()