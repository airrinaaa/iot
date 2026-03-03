import json
import time
import io
from config import env


from fastavro import schemaless_writer, parse_schema
import paho.mqtt.client as mqtt

from Device import Device, DeviceType

SCHEMA_PATH = env("SCHEMA_PATH", "observation.avsc")

MQTT_HOST = env("MQTT_HOST", "localhost")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = int(env("MQTT_KEEPALIVE", "60"))
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "sensors")
NUMBER_OF_DEVICES = int(env("NUMBER_OF_DEVICES", "1000"))
SLEEP_TIME = float(env("SLEEP_TIME", "2"))

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
        dev_type = types_of_devices[i%len(types_of_devices)]
        devices.append(Device.create_by_type(dev_type))
    while True:
        for device in devices:
            current_observations = device.read_all()
            for observation in current_observations:
                data = observation.to_dict()
                bytes_io = io.BytesIO()
                data['thing_id'] = str()
                data['datastream_id'] = str(data['datastream_id'])
                data['event_time'] = data['event_time'].isoformat()
                data['ingestion_time'] = data['ingestion_time'].isoformat()
                schemaless_writer(bytes_io, schema, data)
                avro_bytes = bytes_io.getvalue()
                topic = f"{MQTT_TOPIC_PREFIX}/{device.device_type.value}"
                client.publish(topic, payload=avro_bytes)
            # print(f"Sent to {topic}: {data['metric']} = {data['value']}")

        time.sleep(SLEEP_TIME)

if __name__ == '__main__':
    run_simulation()
