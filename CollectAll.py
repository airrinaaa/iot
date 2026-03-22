from enum import Enum
from datetime import datetime
import time
import pickle

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor


class Type(Enum):
    ANALOG = "analog"
    ALARM = "alarm"
    COUNTER = "counter"
    STATE = "state"


METRIC_TYPE = {
    "temperature": Type.ANALOG,
    "humidity": Type.ANALOG,
    "co2": Type.ANALOG,
    "voltage": Type.ANALOG,
    "water": Type.COUNTER,
    "electricity": Type.COUNTER,
    "door": Type.STATE,
    "oven": Type.STATE,
    "move": Type.ALARM,
    "alarm": Type.STATE,
    "fridge": Type.ANALOG,
    "smoke": Type.ALARM,
}


def type_by_value(data: dict) -> Type:
    value = data.get("value")

    if isinstance(value, str):
        return Type.STATE
    elif isinstance(value, bool):
        return Type.ALARM
    elif isinstance(value, (int, float)):
        return Type.ANALOG
    else:
        return Type.ANALOG


def type_by_metric(data: dict) -> Type:
    metric = data["metric"]
    if metric in METRIC_TYPE:
        return METRIC_TYPE[metric]
    else:
        return type_by_value(data)


def type_by_sensor_type(data: dict) -> Type | None:
    sensor_type = data.get("sensor_type")

    if sensor_type == "analog":
        return Type.ANALOG
    elif sensor_type == "alarm":
        return Type.ALARM
    elif sensor_type == "counter":
        return Type.COUNTER
    elif sensor_type == "state":
        return Type.STATE
    else:
        return None


def identify_type(data: dict) -> Type:
    sensor_type = type_by_sensor_type(data)
    if sensor_type is not None:
        return sensor_type
    else:
        return type_by_metric(data)


class CollectAll(KeyedProcessFunction):

    def __init__(self, window_size_sec: int, allowed_lateness_sec: int, late_tag):
        self.window_size_ms = window_size_sec * 1000
        self.allowed_lateness_ms = allowed_lateness_sec * 1000
        self.late_tag = late_tag
        self.windows_state = None

    def open(self, runtime_context):
        self.windows_state = runtime_context.get_map_state(
            MapStateDescriptor(
                "windows_state",
                Types.LONG(),
                Types.PICKLED_BYTE_ARRAY()
            )
        )

    def load_bucket(self, window_start_ms: int):
        raw_bucket = self.windows_state.get(window_start_ms)
        if raw_bucket is None:
            return None
        return pickle.loads(raw_bucket)

    def save_bucket(self, window_start_ms: int, bucket: dict):
        self.windows_state.put(window_start_ms, pickle.dumps(bucket))

    def delete_bucket(self, window_start_ms: int):
        self.windows_state.remove(window_start_ms)

    def get_window_bounds(self, event_time_ms: int) -> tuple[int, int]:
        window_start = (event_time_ms // self.window_size_ms) * self.window_size_ms
        window_end = window_start + self.window_size_ms
        return window_start, window_end

    def build_late_record(self, record: dict, arrival_time_ms: int) -> dict:
        event_time_ms = int(datetime.fromisoformat(record["event_time"]).timestamp() * 1000)
        _, window_end_ms = self.get_window_bounds(event_time_ms)
        allowed_until_ms = window_end_ms + self.allowed_lateness_ms

        return {
            "thing_id": record.get("thing_id"),
            "datastream_id": record.get("datastream_id"),
            "metric": record.get("metric"),
            "sensor_type": record.get("sensor_type"),
            "seq": record.get("seq"),
            "event_time": record.get("event_time"),
            "publish_time": record.get("publish_time"),
            "late_by_sec": round(max(0.0, (arrival_time_ms - allowed_until_ms) / 1000.0), 3),
        }

    def create_bucket(self, value: dict, window_end_ms: int, cleanup_time_ms: int) -> dict:
        sensor_type = identify_type(value)

        bucket = {
            "sensor_type": sensor_type.value,
            "metric": value["metric"],
            "count": 0,
            "seen_seq": set(),
            "max_event_time": 0,
            "max_publish_time": 0,
            "fire_index": 0,
            "first_emitted": False,
            "main_timer": window_end_ms - 1,
            "cleanup_timer": cleanup_time_ms,
        }

        if sensor_type == Type.ANALOG:
            bucket["min_value"] = None
            bucket["max_value"] = None
            bucket["sum_value"] = 0.0

        elif sensor_type == Type.COUNTER:
            bucket["first_value"] = None
            bucket["last_value"] = None
            bucket["first_event_time"] = None
            bucket["last_event_time"] = None

        elif sensor_type == Type.STATE:
            bucket["on_count"] = 0
            bucket["off_count"] = 0
            bucket["last_state"] = None
            bucket["last_event_time"] = None
            bucket["distinct_values"] = []

        elif sensor_type == Type.ALARM:
            bucket["true_count"] = 0
            bucket["false_count"] = 0
            bucket["last_value"] = None
            bucket["last_event_time"] = None
            bucket["distinct_values"] = []

        return bucket

    def update_bucket(self, bucket: dict, value: dict, event_time_ms: int, publish_time_ms: int):
        bucket["count"] += 1

        if event_time_ms > bucket["max_event_time"]:
            bucket["max_event_time"] = event_time_ms

        if publish_time_ms > bucket["max_publish_time"]:
            bucket["max_publish_time"] = publish_time_ms

        sensor_type = bucket["sensor_type"]

        if sensor_type == "analog":
            current_value = float(value["value"])

            if bucket["min_value"] is None or current_value < bucket["min_value"]:
                bucket["min_value"] = current_value

            if bucket["max_value"] is None or current_value > bucket["max_value"]:
                bucket["max_value"] = current_value

            bucket["sum_value"] += current_value

        elif sensor_type == "counter":
            current_value = float(value["value"])

            if bucket["first_event_time"] is None or event_time_ms < bucket["first_event_time"]:
                bucket["first_event_time"] = event_time_ms
                bucket["first_value"] = current_value

            if bucket["last_event_time"] is None or event_time_ms >= bucket["last_event_time"]:
                bucket["last_event_time"] = event_time_ms
                bucket["last_value"] = current_value

        elif sensor_type == "state":
            current_value = str(value["value"])

            if current_value == "ON":
                bucket["on_count"] += 1
            elif current_value == "OFF":
                bucket["off_count"] += 1

            if current_value not in bucket["distinct_values"]:
                bucket["distinct_values"].append(current_value)

            if bucket["last_event_time"] is None or event_time_ms >= bucket["last_event_time"]:
                bucket["last_event_time"] = event_time_ms
                bucket["last_state"] = current_value

        elif sensor_type == "alarm":
            current_value = bool(value["value"])

            if current_value:
                bucket["true_count"] += 1
            else:
                bucket["false_count"] += 1

            if current_value not in bucket["distinct_values"]:
                bucket["distinct_values"].append(current_value)

            if bucket["last_event_time"] is None or event_time_ms >= bucket["last_event_time"]:
                bucket["last_event_time"] = event_time_ms
                bucket["last_value"] = current_value

    def build_result(self, key, window_start_ms: int, window_end_ms: int, bucket: dict) -> dict:
        window_start = datetime.fromtimestamp(window_start_ms / 1000.0).strftime('%H:%M:%S')
        window_end = datetime.fromtimestamp(window_end_ms / 1000.0).strftime('%H:%M:%S')
        result_time = int(time.time() * 1000)

        max_event_time = bucket["max_event_time"]
        latency = result_time - max_event_time if max_event_time > 0 else 0

        max_publish_time = bucket["max_publish_time"]
        end_to_end_latency = result_time - max_publish_time if max_publish_time > 0 else 0

        fire_index = bucket["fire_index"]
        is_late_firing = fire_index > 1
        sensor_type = bucket["sensor_type"]
        metric = bucket["metric"]

        if sensor_type == "counter":
            first_value = bucket["first_value"] if bucket["first_value"] is not None else 0.0
            last_value = bucket["last_value"] if bucket["last_value"] is not None else 0.0

            return {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": bucket["count"],
                "sensor_type": sensor_type,
                "metric": metric,
                "first_value": first_value,
                "last_value": last_value,
                "delta": last_value - first_value,
                "window_start": window_start_ms,
                "window_end": window_end_ms,
                "result_time": result_time,
                "max_event_time": max_event_time,
                "max_publish_time": max_publish_time,
                "latency": latency,
                "end_to_end_latency": end_to_end_latency,
                "fire_index": fire_index,
                "is_late_firing": is_late_firing,
            }

        elif sensor_type == "analog":
            average = bucket["sum_value"] / bucket["count"] if bucket["count"] > 0 else 0.0

            return {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": bucket["count"],
                "sensor_type": sensor_type,
                "metric": metric,
                "min_value": bucket["min_value"],
                "max_value": bucket["max_value"],
                "average": average,
                "window_start": window_start_ms,
                "window_end": window_end_ms,
                "result_time": result_time,
                "max_event_time": max_event_time,
                "max_publish_time": max_publish_time,
                "latency": latency,
                "end_to_end_latency": end_to_end_latency,
                "fire_index": fire_index,
                "is_late_firing": is_late_firing,
            }

        elif sensor_type == "state":
            return {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": bucket["count"],
                "sensor_type": sensor_type,
                "metric": metric,
                "distinct_counts": len(bucket["distinct_values"]),
                "on_count": bucket["on_count"],
                "off_count": bucket["off_count"],
                "last_state": bucket["last_state"],
                "window_start": window_start_ms,
                "window_end": window_end_ms,
                "result_time": result_time,
                "max_event_time": max_event_time,
                "max_publish_time": max_publish_time,
                "latency": latency,
                "end_to_end_latency": end_to_end_latency,
                "fire_index": fire_index,
                "is_late_firing": is_late_firing,
            }

        else:
            true_count = bucket["true_count"]
            false_count = bucket["false_count"]
            total_count = bucket["count"]
            true_ratio = true_count / total_count if total_count > 0 else 0.0

            return {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": total_count,
                "sensor_type": sensor_type,
                "metric": metric,
                "distinct_counts": len(bucket["distinct_values"]),
                "triggered": true_count > 0,
                "true_count": true_count,
                "false_count": false_count,
                "true_ratio": true_ratio,
                "last_value": bucket["last_value"],
                "window_start": window_start_ms,
                "window_end": window_end_ms,
                "result_time": result_time,
                "max_event_time": max_event_time,
                "max_publish_time": max_publish_time,
                "latency": latency,
                "end_to_end_latency": end_to_end_latency,
                "fire_index": fire_index,
                "is_late_firing": is_late_firing,
            }

    def process_element(self, value: dict, ctx: 'KeyedProcessFunction.Context'):
        event_time_ms = int(datetime.fromisoformat(value["event_time"]).timestamp() * 1000)
        publish_time_ms = int(datetime.fromisoformat(value["publish_time"]).timestamp() * 1000)
        arrival_time_ms = int(time.time() * 1000)

        window_start_ms, window_end_ms = self.get_window_bounds(event_time_ms)
        cleanup_time_ms = window_end_ms + self.allowed_lateness_ms
        current_watermark = ctx.timer_service().current_watermark()

        if current_watermark >= cleanup_time_ms:
            yield self.late_tag, self.build_late_record(value, arrival_time_ms)
            return

        bucket = self.load_bucket(window_start_ms)

        if bucket is None:
            bucket = self.create_bucket(value, window_end_ms, cleanup_time_ms)
            ctx.timer_service().register_event_time_timer(bucket["main_timer"])
            ctx.timer_service().register_event_time_timer(bucket["cleanup_timer"])
        current_seq = value["seq"]

        if current_seq in bucket["seen_seq"]:
            return
        bucket["seen_seq"].add(current_seq)

        self.update_bucket(bucket, value, event_time_ms, publish_time_ms)
        self.save_bucket(window_start_ms, bucket)

        if not bucket["first_emitted"] and current_watermark >= bucket["main_timer"]:
            bucket["first_emitted"] = True
            bucket["fire_index"] += 1
            self.save_bucket(window_start_ms, bucket)
            yield self.build_result(ctx.get_current_key(), window_start_ms, window_end_ms, bucket)
            return

        if bucket["first_emitted"]:
            bucket["fire_index"] += 1
            self.save_bucket(window_start_ms, bucket)

            window_start = datetime.fromtimestamp(window_start_ms / 1000.0).strftime('%H:%M:%S')
            window_end = datetime.fromtimestamp(window_end_ms / 1000.0).strftime('%H:%M:%S')

            print(
                f"[LATE FIRING] key={str(ctx.get_current_key())[:8]}... "
                f"window={window_start}-{window_end} "
                f"fire_index={bucket['fire_index']}"
            )

            yield self.build_result(ctx.get_current_key(), window_start_ms, window_end_ms, bucket)
            return

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        main_window_start_ms = timestamp - self.window_size_ms + 1
        main_bucket = self.load_bucket(main_window_start_ms)

        if main_bucket is not None:
            if timestamp == main_bucket["main_timer"] and not main_bucket["first_emitted"]:
                main_bucket["first_emitted"] = True
                main_bucket["fire_index"] += 1
                self.save_bucket(main_window_start_ms, main_bucket)

                yield self.build_result(
                    ctx.get_current_key(),
                    main_window_start_ms,
                    main_window_start_ms + self.window_size_ms,
                    main_bucket
                )

        cleanup_window_start_ms = timestamp - self.window_size_ms - self.allowed_lateness_ms
        cleanup_bucket = self.load_bucket(cleanup_window_start_ms)

        if cleanup_bucket is not None:
            if timestamp == cleanup_bucket["cleanup_timer"]:
                self.delete_bucket(cleanup_window_start_ms)