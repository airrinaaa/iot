from confluent_kafka import Producer
import socket

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'broker.address.family': 'v4'
}


producer = Producer(conf)

def delivery_report(err, msg):
    """ Функція звіту: скаже, чи дійшло повідомлення """
    if err is not None:
        print(f"❌ Помилка: {err}")
    else:
        print(f"✅ Успіх! Повідомлення доставлено в топік: {msg.topic()}")

print("Пробую підключитися до Kafka...")

producer.produce('test_topic', key="test", value="Hello Kafka", callback=delivery_report)

producer.flush()