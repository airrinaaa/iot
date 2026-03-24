from datetime import datetime, timezone
import json
import os
import io

from fastavro import parse_schema, schemaless_reader
from pyflink.common import Duration, Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema, ByteArraySchema
from pyflink.datastream.externalized_checkpoint_retention import ExternalizedCheckpointRetention

from CollectAll import CollectAll
from SensorTimestampAssigner import SensorTimestampAssigner
from config import env


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = env("SOURCE_TOPIC", "sensors_data_topic2")
PROCESSED_TOPIC = env("PROCESSED_TOPIC", "processed_data_topic2")
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "iot_processor2")
LATE_DATA_TOPIC = env("LATE_DATA_TOPIC", "late_data_topic2")
DLQ_TOPIC = env("DLQ_TOPIC", "dead_letter_topic3")
FLINK_PARALLELISM = int(env("FLINK_PARALLELISM", "1"))

IDLENESS_SEC = 20
MAX_OUT_OF_ORDER_SEC = 2
WINDOW_SIZE_SEC = 10
ALLOWED_LATENESS_SEC = 15

SCHEMA_PATH = env("SCHEMA_PATH", "observation.avsc")

VALID_SENSOR_TYPES = {"analog", "counter", "state", "alarm"}

config = Configuration()
config.set_string("execution.buffer-timeout", "0")
config.set_string("state.checkpoints.dir", f"file://{os.getcwd()}/flink-checkpoints")

flink_env = StreamExecutionEnvironment.get_execution_environment(config)
flink_env.set_parallelism(FLINK_PARALLELISM)
flink_env.enable_checkpointing(5000)
checkpoint_config = flink_env.get_checkpoint_config()
checkpoint_config.set_checkpoint_timeout(60000)
checkpoint_config.set_min_pause_between_checkpoints(3000)
checkpoint_config.set_max_concurrent_checkpoints(1)
checkpoint_config.set_externalized_checkpoint_retention(
    ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
)

jar_path = f"file://{os.getcwd()}/jars/flink-sql-connector-kafka-4.0.1-2.0.jar"
flink_env.add_jars(jar_path)

print(f"Середовище налаштовано. JAR завантажено: {jar_path}")


source = KafkaSource.builder() \
    .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
    .set_topics(SOURCE_TOPIC) \
    .set_group_id(KAFKA_GROUP_ID) \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(ByteArraySchema()) \
    .build()

ds = flink_env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka bridge")


def is_iso_datetime(date: str) -> bool:
    """
    перевіряє дату на коректність відповідно до ISO
    """
    try:
        datetime.fromisoformat(date)
        return True
    except Exception:
        return False


def validate_record(record: dict) -> tuple[bool, str]:
    """
    перевірка декодованого запису на коректність
    """
    if "thing_id" not in record or not isinstance(record["thing_id"], str) or not record["thing_id"].strip():
        return False, "invalid_thing_id"

    if "datastream_id" not in record or not isinstance(record["datastream_id"], str) or not record["datastream_id"].strip():
        return False, "invalid_datastream_id"

    if "metric" not in record or not isinstance(record["metric"], str) or not record["metric"].strip():
        return False, "invalid_metric"

    if "sensor_type" not in record or not isinstance(record["sensor_type"], str) or record["sensor_type"] not in VALID_SENSOR_TYPES:
        return False, "invalid_sensor_type"

    if "seq" not in record or not isinstance(record["seq"], int) or isinstance(record["seq"], bool):
        return False, "invalid_seq"

    if "event_time" not in record or not isinstance(record["event_time"], str) or not record["event_time"].strip() or not is_iso_datetime(record["event_time"]):
        return False, "invalid_event_time"

    if "value" not in record or record["value"] is None:
        return False, "invalid_value"
    if "publish_time" not in record or not isinstance(record["publish_time"], str) or not record["publish_time"].strip() or not is_iso_datetime(record["publish_time"]):
        return False, "invalid_publish_time"
    value = record["value"]
    sensor_type = record["sensor_type"]
    if sensor_type == "analog":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return False, "invalid_analog_value"
    elif sensor_type == "counter":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return False, "invalid_counter_value"
    elif sensor_type == "state":
        if value not in ("ON", "OFF"):
            return False, "invalid_state_value"
    elif sensor_type == "alarm":
        if not isinstance(value, bool):
            return False, "invalid_alarm_value"
    return True, "ok"


def safe_decode_validate(avro_bytes: bytes, schema) -> tuple[str, dict]:
    """
    декодування та перевірка Avro-повідомлення
    :param avro_bytes:
    :param schema:
    :return:
    """
    try:
        record = schemaless_reader(io.BytesIO(avro_bytes), schema)
    except Exception as e:
        return ("dlq", {
            "error": "decode_failed",
            "error_details": str(e)[:500],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "size_bytes": len(avro_bytes),
        })

    ok, reason = validate_record(record)
    if ok:
        return ("ok", record)
    #у разі невалідності формується запис до dlq
    return ("dlq", {
        "error": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "thing_id": str(record.get("thing_id", ""))[:100],
        "datastream_id": str(record.get("datastream_id", ""))[:100],
        "metric": str(record.get("metric", ""))[:100],
        "sensor_type": str(record.get("sensor_type", ""))[:50],
        "seq": record.get("seq"),
    })

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    raw_schema = json.load(f)
    schema = parse_schema(raw_schema)

decoded = ds.map(lambda b: safe_decode_validate(b, schema))

ok_stream = decoded.filter(lambda t: t[0] == "ok").map(lambda t: t[1])
dlq_stream = decoded.filter(lambda t: t[0] == "dlq").map(lambda t: t[1])

dlq_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(KAFKA_BOOTSTRAP)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(DLQ_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()
)

dlq_stream_serialized = dlq_stream.map(lambda d: json.dumps(d), output_type=Types.STRING())
dlq_stream_serialized.sink_to(dlq_sink)

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(MAX_OUT_OF_ORDER_SEC)
).with_timestamp_assigner(
    SensorTimestampAssigner()
).with_idleness(
    Duration.of_seconds(IDLENESS_SEC)
)

parsed_ds = ok_stream.assign_timestamps_and_watermarks(watermark_strategy)

late_tag = OutputTag("late_data", Types.PICKLED_BYTE_ARRAY())

late_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(KAFKA_BOOTSTRAP)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(LATE_DATA_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()
)

metric_ds_with_windowing = parsed_ds \
    .key_by(lambda x: x["datastream_id"]) \
    .process(
        CollectAll(WINDOW_SIZE_SEC, ALLOWED_LATENESS_SEC, late_tag),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

metric_ds_with_windowing.print()

late_stream = metric_ds_with_windowing.get_side_output(late_tag)

late_stream_serialized = late_stream.map(
    lambda d: json.dumps(d),
    output_type=Types.STRING()
)

late_stream_serialized.sink_to(late_sink)

metric_ds_serialized = metric_ds_with_windowing.map(
    lambda x: json.dumps(x),
    output_type=Types.STRING()
)

processed_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(KAFKA_BOOTSTRAP)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(PROCESSED_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .set_transactional_id_prefix("flink-iot-processed-")
    .set_property("transaction.timeout.ms", "900000")
    .build()
)

metric_ds_serialized.sink_to(processed_sink)
flink_env.execute("IoT Process")