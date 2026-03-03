import json
import io
import paho.mqtt.client as mqtt
from fastavro import parse_schema, schemaless_reader



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