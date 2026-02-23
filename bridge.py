import paho.mqtt.client as mqtt
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
KAFKA_TOPIC = "sensors_data"


def on_connect(client, userdata, flags, reason_code, properties):
    print("Bridge підключився до MQTT!")
    client.subscribe("sensors/#")


def on_message(client, userdata, message):
    try:
        producer.send(KAFKA_TOPIC, value=message.payload)
        producer.flush()
    except Exception as e:
        print(e)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect("127.0.0.1", 1883, 60)

print("Bridge працює...")
mqtt_client.loop_forever()

