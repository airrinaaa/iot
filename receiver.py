import json
import io
import paho.mqtt.client as mqtt
from fastavro import parse_schema, schemaless_reader
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "iot_bucket"
org = "ukma"
token = "udmGEVpuKmdT3FQjomj-ujjPeuFEz38BQZcJ4shDGeST2cjWw13XhSXhAuPMx8r7swrLN45r7Z-3odJaQjEw7g=="
url="http://localhost:8086"
db_client = influxdb_client.InfluxDBClient(
   url=url,
   token=token,
   org=org
)
write_api = db_client.write_api(write_options=SYNCHRONOUS)

with open("observation.avsc", "rb") as f:
    raw_schema = json.load(f)
    schema = parse_schema(raw_schema)

def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe("sensors/#")

def on_message(client, userdata, message):
    payload = message.payload
    bytes_io = io.BytesIO(payload)
    try:
        read_data = schemaless_reader(bytes_io, schema)

        raw_value = read_data['value']
        try:
            final_value = float(raw_value)
        except ValueError:
            final_value = raw_value
        p = (
            Point(read_data['metric'])
            .tag("sensor_id", read_data['thing_id'])
            .tag("topic", message.topic)
            .field("value", final_value)
            .time(read_data['event_time'])
        )
        write_api.write(bucket=bucket, org=org, record=p)
        print(f"Saved: {read_data['metric']} = {final_value}")
    except Exception as e:
        print(e)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

print("Connecting to broker...")
client.connect("127.0.0.1", 1883, 60)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("Receiver stopped.")
    client.disconnect()
    db_client.close()