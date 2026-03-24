import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from config import env

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = env("SOURCE_TOPIC", "sensors_data_topic2")

MQTT_HOST = env("MQTT_HOST", "localhost")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = int(env("MQTT_KEEPALIVE", "60"))
MQTT_SUBSCRIBE_TOPIC = env("MQTT_SUBSCRIBE_TOPIC", "sensors/#")

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP],api_version=(0, 10, 2),retries=5)

def on_connect(client, userdata, flags, reason_code, properties):
    """
    викликається після підключення до MQTT та підписується на потрібний топік
    :param client:
    :param userdata:
    :param flags:
    :param reason_code:
    :param properties:
    :return:
    """
    print("Bridge підключився до MQTT!")
    client.subscribe(MQTT_SUBSCRIBE_TOPIC)


def on_message(client, userdata, message):
    """
    викликається при отриманні повідомлення та пересилає його до kafka
    :param client:
    :param userdata:
    :param message:
    :return:
    """
    try:
        producer.send(SOURCE_TOPIC, value=message.payload)
    except Exception as e:
        print("kafka send error:", e)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

print("Bridge працює...")
mqtt_client.loop_forever()
