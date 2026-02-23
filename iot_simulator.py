from confluent_kafka import Producer
import socket

# Налаштування
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'broker.address.family': 'v4'  # <--- ЦЕ ГОЛОВНЕ: примушуємо використовувати IPv4
}

# Створення продюсера
producer = Producer(conf)

def delivery_report(err, msg):
    """ Функція звіту: скаже, чи дійшло повідомлення """
    if err is not None:
        print(f"❌ Помилка: {err}")
    else:
        print(f"✅ Успіх! Повідомлення доставлено в топік: {msg.topic()}")

print("📡 Пробую підключитися до Kafka...")

# Відправляємо одне тестове повідомлення
producer.produce('test_topic', key="test", value="Hello Kafka", callback=delivery_report)

# Чекаємо доставки
producer.flush()