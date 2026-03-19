from datetime import datetime, timezone
import json
import os
import io

from fastavro import parse_schema, schemaless_reader
from pyflink.common import Duration, Types
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema, ByteArraySchema

from CollectAll import CollectAll
from SensorTimestampAssigner import SensorTimestampAssigner
from config import env
from pyflink.common import Configuration
from pyflink.datastream.externalized_checkpoint_retention import ExternalizedCheckpointRetention

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = env("SOURCE_TOPIC", "sensors_data_topic2")
PROCESSED_TOPIC = env("PROCESSED_TOPIC", "processed_data_topic1")
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "iot_processor2")
LATE_DATA_TOPIC = env("LATE_DATA_TOPIC", "late_data_topic1")
FLINK_PARALLELISM = int(env("FLINK_PARALLELISM", "1"))
IDLENESS_SEC = 10
MAX_OUT_OF_ORDER_SEC = 1
WINDOW_SIZE_SEC = 8
ALLOWED_LATENESS_SEC = 10

SCHEMA_PATH = env("SCHEMA_PATH", "observation.avsc")
DLQ_TOPIC = env("DLQ_TOPIC", "dead_letter_topic")

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

current_dir = os.getcwd()
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


def is_iso_datetime(s: str) -> bool:
    try:
        datetime.fromisoformat(s)
        return True
    except Exception:
        return False


def validate_record(record: dict) -> tuple[bool, str]:
    if "thing_id" not in record or not isinstance(record["thing_id"], str) or not record["thing_id"].strip():
        return False, "invalid_thing_id"

    if "datastream_id" not in record or not isinstance(record["datastream_id"], str) or not record[
        "datastream_id"].strip():
        return False, "invalid_datastream_id"

    if "metric" not in record or not isinstance(record["metric"], str) or not record["metric"].strip():
        return False, "invalid_metric"

    if "seq" not in record or not isinstance(record["seq"], int):
        return False, "invalid_seq"

    if "event_time" not in record or not isinstance(record["event_time"], str) or not record[
        "event_time"].strip() or not is_iso_datetime(record["event_time"]):
        return False, "invalid_event_time"

    if "ingestion_time" not in record or not isinstance(record["ingestion_time"], str) or not record[
        "ingestion_time"].strip() or not is_iso_datetime(record["ingestion_time"]):
        return False, "invalid_ingestion_time"

    if "value" not in record or record["value"] is None:
        return False, "invalid_value"

    return True, "ok"


def safe_decode_validate(avro_bytes: bytes, schema) -> tuple[str, dict]:
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

    return ("dlq", {
        "error": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "decoded_record": record,
    })


def serialize_late_event(record: dict) -> str:
    event_ts = datetime.fromisoformat(record["event_time"]).timestamp()
    ingestion_ts = datetime.fromisoformat(record["ingestion_time"]).timestamp()

    window_start_ts = int(event_ts // WINDOW_SIZE_SEC) * WINDOW_SIZE_SEC
    window_end_ts = window_start_ts + WINDOW_SIZE_SEC
    allowed_until_ts = window_end_ts + ALLOWED_LATENESS_SEC

    enriched_record = {
        **record,
        "late_reason": "dropped_after_allowed_lateness",
        "late_by_sec": round(max(0.0, ingestion_ts - allowed_until_ts), 3)
    }

    return json.dumps(enriched_record)


with open(SCHEMA_PATH, "rb") as f:
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

windowed_stream = parsed_ds \
    .key_by(lambda x: x["datastream_id"]) \
    .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SEC))) \
    .allowed_lateness(ALLOWED_LATENESS_SEC * 1000) \
    .side_output_late_data(late_tag)

metric_ds_with_windowing = windowed_stream.apply(
    CollectAll(),
    output_type=Types.PICKLED_BYTE_ARRAY()
)

metric_ds_with_windowing.print()

late_stream = metric_ds_with_windowing.get_side_output(late_tag)

late_stream_serialized = late_stream.map(
    serialize_late_event,
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