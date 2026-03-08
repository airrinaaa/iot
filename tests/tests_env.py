from config import env


def test_schema_path_from_env():
    assert env("SCHEMA_PATH") == "observation.avsc"


def test_mqtt_config_from_env():
    assert env("MQTT_HOST") == "localhost"
    assert env("MQTT_PORT") == "1883"
    assert env("MQTT_KEEPALIVE") == "60"
    assert env("MQTT_TOPIC_PREFIX") == "sensors"
    assert env("MQTT_SUBSCRIBE_TOPIC") == "sensors/#"


def test_simulation_config_from_env():
    assert env("NUMBER_OF_DEVICES") == "1000"
    assert env("SLEEP_TIME") == "2"


def test_kafka_config_from_env():
    assert env("KAFKA_BOOTSTRAP") == "localhost:9092"
    assert env("SOURCE_TOPIC") == "sensors_data"
    assert env("PROCESSED_TOPIC") == "processed_data"
    assert env("ALERTS_TOPIC") == "alerts"
    assert env("DLQ_TOPIC") == "dead_letter"
    assert env("KAFKA_GROUP_ID") == "iot_processor_v1"


def test_influx_config_from_env():
    assert env("INFLUX_URL") == "http://localhost:8086"
    assert env("INFLUX_BUCKET") == "iot_bucket"
    assert env("INFLUX_ORG") == "ukma"
    assert env("INFLUX_TOKEN") == "7AQvoOVUi3fsj1onjj6p9lw4nkCWzrxhwHqpDFZGA-1TnivANIdvp-pjFEbPjtF9BQbb9QfELB8Nh5ZXUzX1g=="
    assert env("INFLUX_MEASUREMENT") == "processed_data"


def test_env_missing_value():
    assert env("TEST_DEFAULT_VAR", "default_value") == "default_value"