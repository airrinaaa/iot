import json
import time
import io
from fastavro import schemaless_writer, parse_schema
import paho.mqtt.client as mqtt

from Device import Device, DeviceType


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect("localhost", 1883, 60)

def run_simulation():
    client.loop_start()
    with open("observation.avsc", "rb") as f:
        raw_schema = json.load(f)
        schema = parse_schema(raw_schema)

    devices: list[Device] = []
    devices.append(Device.create_by_type(DeviceType.CLIMATE))
    devices.append(Device.create_by_type(DeviceType.SECURITY))
    devices.append(Device.create_by_type(DeviceType.UTILITY))
    devices.append(Device.create_by_type(DeviceType.KITCHEN))
    while True:
        for device in devices:
            current_observations = device.read_all()
            for observation in current_observations:
                data = observation.to_dict()
                bytes_io = io.BytesIO()
                data['thing_id'] = str(data['thing_id'])
                data['datastream_id'] = str(data['datastream_id'])
                data['event_time'] = data['event_time'].isoformat()
                data['ingestion_time'] = data['ingestion_time'].isoformat()
                schemaless_writer(bytes_io, schema, data)
                avro_bytes = bytes_io.getvalue()
                topic = f"sensors/{device.device_type.value}"
                client.publish(topic, payload=avro_bytes)
                print(f"Sent to {topic}: {data['metric']} = {data['value']}")

        time.sleep(2)

if __name__ == '__main__':
    run_simulation()
