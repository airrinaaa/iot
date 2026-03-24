import json
import time
import io
import random
from collections import deque
from datetime import datetime, timezone
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


def publish_avro(topic, data, schema):
    """
    серіалізація повідомлення в Avro і відправка до MQTT
    :param topic:
    :param data:
    :param schema:
    :return:
    """
    data["publish_time"] = datetime.now(timezone.utc).isoformat()

    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, schema, data)
    avro_bytes = bytes_io.getvalue()

    client.publish(topic, payload=avro_bytes)


def run_simulation():
    """
    основний цикл симуляції (створення пристроїв, генерація спостережень і їх публікація до MQTT)
    """
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

    ANOMALY_CYCLE_SEC = 500
    device_phase_offset = {
        device.thing_id: random.uniform(0, ANOMALY_CYCLE_SEC)
        for device in devices
    }

    start_time = time.time()
    #для запізнілих подій
    WITHIN_ALLOWED_DELAY_SEC = 8
    TOO_LATE_DELAY_SEC = 30
    WITHIN_ALLOWED_DELAY_PROB = 0.00001
    TOO_LATE_DELAY_PROB = 0.00001

    delayed_buffer: deque = deque()
    too_late_buffer: deque = deque()

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
                topic = f"{MQTT_TOPIC_PREFIX}/{device.device_type.value}"


                #аномалії(неможливі значення)
                if random.random() < 0.0001:
                    if metric in ["temperature", "voltage", "co2", "fridge"]:
                        data["value"] = random.choice([-999.0, 9999.0])
                    elif metric in ["humidity"]:
                        data["value"] = 500.0
                else:
                    #період аномалій у циклі
                    if 360 < cycle_time < 370 and random.random() < 0.5:
                        if metric == "temperature":
                            data["value"] = random.uniform(70.0, 95.0)
                        elif metric == "humidity":
                            data["value"] = random.uniform(95.0, 100.0)
                        elif metric == "co2":
                            data["value"] = random.uniform(2500.0, 5000.0)
                        elif metric == "fridge":
                            data["value"] = random.uniform(18.0, 25.0)
                        elif metric == "voltage":
                            data["value"] = random.uniform(290.0, 320.0)
                        elif metric == "water":
                            data["value"] += random.uniform(150.0, 400.0)
                        elif metric == "electricity":
                            data["value"] += random.uniform(200.0, 600.0)
                data["thing_id"] = str(data["thing_id"])
                data["datastream_id"] = str(data["datastream_id"])
                data["event_time"] = data["event_time"].isoformat()

                is_dlq = False
                bad_bytes_payload = None
                #генерація невалідних повідомлень
                if random.random() < 0.00001:
                    is_dlq = True
                    error_type = random.choice(["bad_metric", "bad_time", "empty_id", "bad_bytes"])
                    if error_type == "bad_metric":
                        data["metric"] = "   "
                    elif error_type == "bad_time":
                        data["event_time"] = "2026-99-99 broken-time"
                    elif error_type == "empty_id":
                        data["datastream_id"] = "   "
                    elif error_type == "bad_bytes":
                        bad_bytes_payload = b"totally_invalid_avro_garbage_123456789"
                    print(f"[INVALID MESSAGE] topic={topic} error_type={error_type}")


                if bad_bytes_payload is not None:
                    client.publish(topic, payload=bad_bytes_payload)
                #генерація дуже запізнілих подій(поза межами дозволеної затримки)
                elif not is_dlq and random.random() < TOO_LATE_DELAY_PROB:
                    too_late_buffer.append((time.time() + TOO_LATE_DELAY_SEC, topic, data.copy()))
                #генерація запізнілих подій у межах допустимої затримки
                elif not is_dlq and random.random() < WITHIN_ALLOWED_DELAY_PROB:
                    delayed_buffer.append((time.time() + WITHIN_ALLOWED_DELAY_SEC, topic, data.copy()))
                else:
                    publish_avro(topic, data, schema)

        now = time.time()

        while delayed_buffer and delayed_buffer[0][0] <= now:
            _, t, data = delayed_buffer.popleft()
            publish_avro(t, data, schema)
            print(f"[DELAYED PUBLISH - WITHIN ALLOWED] topic={t}")

        while too_late_buffer and too_late_buffer[0][0] <= now:
            _, t, data = too_late_buffer.popleft()
            publish_avro(t, data, schema)
            print(f"[DELAYED PUBLISH - TOO LATE] topic={t}")

        time.sleep(SLEEP_TIME)


if __name__ == '__main__':
    run_simulation()