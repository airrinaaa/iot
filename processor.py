import json
import os
import io
from datetime import datetime

from pyflink.common import Duration
from pyflink.common.time import Time
from fastavro import parse_schema, schemaless_reader
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import ByteArraySchema


from CollectAll import CollectAll
from SensorTimestampAssigner import SensorTimestampAssigner

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
current_dir = os.getcwd()
jar_path = f"file://{os.getcwd()}/jars/flink-sql-connector-kafka-4.0.1-2.0.jar"

env.add_jars(jar_path)

print(f"Середовище налаштовано. JAR завантажено: {jar_path}")


source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("sensors_data") \
    .set_group_id(f"group_{datetime.now().timestamp()}") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(ByteArraySchema()) \
    .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka bridge")


with open("observation.avsc", "rb") as f:
    raw_schema = json.load(f)
    schema = parse_schema(raw_schema)

def decode_avro(raw_bytes):
    byt = io.BytesIO(raw_bytes)
    return schemaless_reader(byt, schema)

def collect_all(key, window, inputs):
    return [{"metric": key, "list": list(inputs)}]
parsed_ds = ds.map(decode_avro)

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(SensorTimestampAssigner()).with_idleness(Duration.of_seconds(10))
parsed_ds = parsed_ds.assign_timestamps_and_watermarks(watermark_strategy)

metric_ds_with_windowing = parsed_ds \
    .key_by(lambda x: x['datastream_id']) \
    .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
    .apply(CollectAll())
metric_ds_with_windowing.print()
env.execute("IoT Process")

