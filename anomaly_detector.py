import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pickle
import os
from river import anomaly
from river import preprocessing
from river import compose
from river import anomaly
from river.drift import ADWIN

from config import env


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
PROCESSED_TOPIC = env("PROCESSED_TOPIC", "processed_data_topic2")
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID_ANOMALY", "anomaly_detector_topic333")
KAFKA_OFFSET_RESET = env("KAFKA_OFFSET_RESET", "latest")

INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
INFLUX_BUCKET = env("INFLUX_BUCKET", "iot_bucket")
INFLUX_ORG = env("INFLUX_ORG", "ukma")
INFLUX_TOKEN = env("INFLUX_TOKEN")

INFLUX_MEASUREMENT = "anomalies"
DRIFT_MEASUREMENT = "concept_drift"

MODEL_STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "anomaly_models")
if not os.path.exists(MODEL_STATE_DIR):
    os.makedirs(MODEL_STATE_DIR)
dirty_models = set()

SAVE_EVERY = 2000
THRESHOLD = 0.6
MIN_SAMPLES_FOR_ANOMALY = 10
MIN_SAMPLES_FOR_DRIFT = 10
DRIFT_COOLDOWN_WINDOWS = 3

ADWIN_DELTA = 0.002
ADWIN_CLOCK = 8
ADWIN_GRACE_PERIOD = 5
ADWIN_MIN_WINDOW_LENGTH = 10


consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset=KAFKA_OFFSET_RESET,
    enable_auto_commit=True,
)

client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)


def create_model():
    return compose.Pipeline(
        preprocessing.StandardScaler(),
        anomaly.HalfSpaceTrees(
            n_trees=50,
            height=10,
            window_size=50,
            seed=42
        ))


def create_drift_detector():
    return ADWIN(
        delta=ADWIN_DELTA,
        clock=ADWIN_CLOCK,
        grace_period=ADWIN_GRACE_PERIOD,
        min_window_length=ADWIN_MIN_WINDOW_LENGTH
    )


def create_entry(reset_count=0):
    return {
        "model": create_model(),
        "count": 0,
        "drift_detector": create_drift_detector(),
        "reset_count": reset_count,
        "cooldown": 0
    }


def ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def model_file_path(datastream_id):
    safe_id = datastream_id.replace("/", "_").replace(":", "_")
    return os.path.join(MODEL_STATE_DIR, f"{safe_id}.pkl")


def load_model_entry(datastream_id):
    path = model_file_path(datastream_id)

    if not os.path.exists(path):
        return create_entry()

    try:
        with open(path, "rb") as f:
            entry = pickle.load(f)
        repaired = repair_loaded_models({datastream_id: entry})
        return repaired.get(datastream_id, create_entry())
    except Exception as e:
        print(f"MODEL LOAD ERROR | datastream_id={datastream_id} | error={e}")
        return create_entry()


def save_ml_models():
    for datastream_id in list(dirty_models):
        path = model_file_path(datastream_id)
        temp_path = path + ".tmp"

        try:
            with open(temp_path, "wb") as f:
                pickle.dump(ml_models[datastream_id], f)
                f.flush()
                os.fsync(f.fileno())

            os.replace(temp_path, path)
            dirty_models.discard(datastream_id)

        except Exception as e:
            print(f"MODEL SAVE ERROR | datastream_id={datastream_id} | error={e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
progress = {"processed_messages": 0}


def register_progress():
    progress["processed_messages"] += 1
    if progress["processed_messages"] % SAVE_EVERY == 0:
        save_ml_models()


def write_point(point, error_text):
    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as e:
        print(f"{error_text}: {e}")


def ensure_model(datastream_id):
    if datastream_id not in ml_models:
        ml_models[datastream_id] = load_model_entry(datastream_id)


def repair_loaded_models(models):
    if not isinstance(models, dict):
        return {}

    for datastream_id, entry in list(models.items()):
        if not isinstance(entry, dict):
            models[datastream_id] = create_entry()
            continue

        if "model" not in entry:
            entry["model"] = create_model()
        if "count" not in entry:
            entry["count"] = 0
        if "drift_detector" not in entry:
            entry["drift_detector"] = create_drift_detector()
        if "reset_count" not in entry:
            entry["reset_count"] = 0
        if "cooldown" not in entry:
            entry["cooldown"] = 0

        if "adaptive" in entry:
            del entry["adaptive"]

    return models


ml_models = {}

print("Модуль виявлення аномалій запущено.")


try:
    for message in consumer:
        try:
            if message.value is None or len(message.value) == 0:
                continue

            if message.value.startswith(b"\x00"):
                continue

            raw = message.value.decode("utf-8", errors="ignore").strip()
            if not raw:
                continue

            data = json.loads(raw)
        except Exception:
            continue

        try:
            datastream_id = str(data["datastream_id"])
            metric = str(data["metric"])
            sensor_type = str(data["sensor_type"])
            window_end = int(data["window_end"])
        except Exception:
            continue
        event_dt = ms_to_datetime(window_end)
        time_of_event = event_dt.hour
        day_of_week = event_dt.weekday()

        if sensor_type == "analog":
            try:
                min_value = float(data["min_value"])
                max_value = float(data["max_value"])
                average = float(data["average"])
            except (KeyError, TypeError, ValueError):
                continue

            is_sensor_fault = False
            anomaly_value = None

            if metric == "temperature":
                if min_value < -50:
                    is_sensor_fault = True
                    anomaly_value = min_value
                elif max_value > 120:
                    is_sensor_fault = True
                    anomaly_value = max_value
            elif metric == "humidity":
                if min_value < 0:
                    is_sensor_fault = True
                    anomaly_value = min_value
                elif max_value > 100:
                    is_sensor_fault = True
                    anomaly_value = max_value
            elif metric == "co2":
                if min_value < 0:
                    is_sensor_fault = True
                    anomaly_value = min_value
                elif max_value > 10000:
                    is_sensor_fault = True
                    anomaly_value = max_value
            elif metric == "voltage":
                if min_value < 0:
                    is_sensor_fault = True
                    anomaly_value = min_value
                elif max_value > 500:
                    is_sensor_fault = True
                    anomaly_value = max_value
            elif metric == "fridge":
                if min_value < -50:
                    is_sensor_fault = True
                    anomaly_value = min_value
                elif max_value > 100:
                    is_sensor_fault = True
                    anomaly_value = max_value

            if is_sensor_fault:
                print(
                    f"ЗБІЙ СЕНСОРА (Hardware Fault)! Датчик {datastream_id}({metric}) "
                    f"видав неможливе значення: {anomaly_value:.2f} "
                    f"(середнє вікна: {average:.2f})"
                )
                p = (
                    Point(INFLUX_MEASUREMENT)
                    .tag("sensor_type", "analog")
                    .tag("metric", metric)
                    .tag("datastream_id", datastream_id)
                    .tag("anomaly_type", "hardware_fault")
                    .time(ms_to_datetime(window_end))
                    .field("anomaly_value", float(anomaly_value))
                    .field("anomaly_score", 1.0)
                )
                write_point(p, "Помилка запису в Influx")
                continue

            spread = max_value - min_value

            ensure_model(datastream_id)

            features = {
                "average": average,
                "min_value": min_value,
                "max_value": max_value,
                "spread": spread,
                "time": time_of_event,
                "day_of_week": day_of_week
            }

            anomaly_score = ml_models[datastream_id]["model"].score_one(features)
            print(
                f"SCORE | datastream_id={datastream_id} | sensor_type={sensor_type} | "
                f"metric={metric} | score={anomaly_score:.3f} | "
                f"samples_before={ml_models[datastream_id]['count']} | "
                f"reset_count={ml_models[datastream_id]['reset_count']}"
            )
            ml_models[datastream_id]["model"].learn_one(features)
            ml_models[datastream_id]["count"] += 1
            dirty_models.add(datastream_id)
            register_progress()

            if ml_models[datastream_id]["cooldown"] > 0:
                ml_models[datastream_id]["cooldown"] -= 1
            elif ml_models[datastream_id]["count"] >= MIN_SAMPLES_FOR_DRIFT:
                ml_models[datastream_id]["drift_detector"].update(anomaly_score)

                if ml_models[datastream_id]["drift_detector"].drift_detected:
                    ml_models[datastream_id]["reset_count"] += 1
                    reset_count = ml_models[datastream_id]["reset_count"]

                    print(
                        f"ВИЯВЛЕНО CONCEPT DRIFT! Сенсор {datastream_id}({metric}). "
                        f"Score: {anomaly_score:.3f}. Модель буде скинута."
                    )

                    p = (
                        Point(DRIFT_MEASUREMENT)
                        .tag("sensor_type", "analog")
                        .tag("metric", metric)
                        .tag("datastream_id", datastream_id)
                        .tag("detector", "ADWIN")
                        .time(ms_to_datetime(window_end))
                        .field("drift_value", float(anomaly_score))
                        .field("count_before_reset", ml_models[datastream_id]["count"])
                        .field("reset_count", reset_count)
                    )
                    write_point(p, "Помилка запису drift в Influx")

                    ml_models[datastream_id] = create_entry(reset_count=reset_count)
                    ml_models[datastream_id]["cooldown"] = DRIFT_COOLDOWN_WINDOWS
                    dirty_models.add(datastream_id)
                    save_ml_models()

            if anomaly_score > THRESHOLD and ml_models[datastream_id]["count"] >= MIN_SAMPLES_FOR_ANOMALY:
                print(
                    f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). "
                    f"Значення {average:.2f}, Score: {anomaly_score:.3f}"
                )
                p = (
                    Point(INFLUX_MEASUREMENT)
                    .tag("sensor_type", "analog")
                    .tag("metric", metric)
                    .tag("datastream_id", datastream_id)
                    .tag("anomaly_type", "statistical")
                    .time(ms_to_datetime(window_end))
                    .field("average", float(average))
                    .field("anomaly_score", float(anomaly_score))
                )
                write_point(p, "Помилка запису в Influx")

        elif sensor_type == "counter":
            try:
                delta = float(data["delta"])
                first_value = float(data["first_value"])
                window_start = int(data["window_start"])
                window_end = int(data["window_end"])
            except (KeyError, TypeError, ValueError):
                continue

            window_duration_sec = max((window_end - window_start) / 1000.0, 1.0)
            abs_delta = abs(delta)
            rate = delta / window_duration_sec

            ensure_model(datastream_id)

            features = {
                "first_value": first_value,
                "delta": delta,
                "abs_delta": abs_delta,
                "rate": rate,
                "time": time_of_event,
                "day_of_week": day_of_week
            }

            anomaly_score = ml_models[datastream_id]["model"].score_one(features)
            print(
                f"SCORE | datastream_id={datastream_id} | sensor_type={sensor_type} | "
                f"metric={metric} | score={anomaly_score:.3f} | "
                f"samples_before={ml_models[datastream_id]['count']} | "
                f"reset_count={ml_models[datastream_id]['reset_count']}"
            )
            ml_models[datastream_id]["model"].learn_one(features)
            ml_models[datastream_id]["count"] += 1
            dirty_models.add(datastream_id)
            register_progress()

            if ml_models[datastream_id]["cooldown"] > 0:
                ml_models[datastream_id]["cooldown"] -= 1
            elif ml_models[datastream_id]["count"] >= MIN_SAMPLES_FOR_DRIFT:
                ml_models[datastream_id]["drift_detector"].update(anomaly_score)

                if ml_models[datastream_id]["drift_detector"].drift_detected:
                    ml_models[datastream_id]["reset_count"] += 1
                    reset_count = ml_models[datastream_id]["reset_count"]

                    print(
                        f"ВИЯВЛЕНО CONCEPT DRIFT! Сенсор {datastream_id}({metric}). "
                        f"Score: {anomaly_score:.3f}. Модель буде скинута."
                    )

                    p = (
                        Point(DRIFT_MEASUREMENT)
                        .tag("sensor_type", "counter")
                        .tag("metric", metric)
                        .tag("datastream_id", datastream_id)
                        .tag("detector", "ADWIN")
                        .time(ms_to_datetime(window_end))
                        .field("drift_value", float(anomaly_score))
                        .field("count_before_reset", ml_models[datastream_id]["count"])
                        .field("reset_count", reset_count)
                    )
                    write_point(p, "Помилка запису drift в Influx")

                    ml_models[datastream_id] = create_entry(reset_count=reset_count)
                    ml_models[datastream_id]["cooldown"] = DRIFT_COOLDOWN_WINDOWS
                    dirty_models.add(datastream_id)
                    save_ml_models()

            if anomaly_score > THRESHOLD and ml_models[datastream_id]["count"] >= MIN_SAMPLES_FOR_ANOMALY:
                print(
                    f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). "
                    f"Значення {delta:.2f}, Score: {anomaly_score:.3f}"
                )
                p = (
                    Point(INFLUX_MEASUREMENT)
                    .tag("sensor_type", "counter")
                    .tag("metric", metric)
                    .tag("datastream_id", datastream_id)
                    .tag("anomaly_type", "statistical")
                    .time(ms_to_datetime(window_end))
                    .field("delta", float(delta))
                    .field("anomaly_score", float(anomaly_score))
                )
                write_point(p, "Помилка запису в Influx")



        elif data["sensor_type"] == "state":

            try:

                distinct_counts = int(data["distinct_counts"])

            except (KeyError, TypeError, ValueError):

                continue

            ensure_model(datastream_id)

            features = {"distinct_counts": distinct_counts, "time": time_of_event, "day of week": day_of_week}

            anomaly_score = ml_models[datastream_id]['model'].score_one(features)

            ml_models[datastream_id]['model'].learn_one(features)

            ml_models[datastream_id]['count'] += 1

            dirty_models.add(datastream_id)

            register_progress()

            if ml_models[datastream_id]["cooldown"] > 0:

                ml_models[datastream_id]["cooldown"] -= 1

            elif ml_models[datastream_id]["count"] >= MIN_SAMPLES_FOR_DRIFT:

                ml_models[datastream_id]["drift_detector"].update(anomaly_score)

                if ml_models[datastream_id]["drift_detector"].drift_detected:
                    ml_models[datastream_id]["reset_count"] += 1

                    reset_count = ml_models[datastream_id]["reset_count"]

                    print(

                        f"ВИЯВЛЕНО CONCEPT DRIFT! Сенсор {datastream_id}({metric}). "

                        f"Score: {anomaly_score:.3f}. Модель буде скинута."

                    )

                    p = (

                        Point(DRIFT_MEASUREMENT)

                        .tag("sensor_type", "state")

                        .tag("metric", metric)

                        .tag("datastream_id", datastream_id)

                        .tag("detector", "ADWIN")

                        .time(ms_to_datetime(window_end))

                        .field("drift_value", float(anomaly_score))

                        .field("count_before_reset", ml_models[datastream_id]["count"])

                        .field("reset_count", reset_count)

                    )

                    write_point(p, "Помилка запису drift в Influx")

                    ml_models[datastream_id] = create_entry(reset_count=reset_count)

                    ml_models[datastream_id]["cooldown"] = DRIFT_COOLDOWN_WINDOWS

                    dirty_models.add(datastream_id)

                    save_ml_models()

            if anomaly_score > THRESHOLD and ml_models[datastream_id]["count"] >= MIN_SAMPLES_FOR_ANOMALY:
                print(

                    f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). "

                    f"distinct_counts={distinct_counts}, Score: {anomaly_score:.3f}"

                )

                p = (Point(INFLUX_MEASUREMENT)

                     .tag("sensor_type", "state")

                     .tag("metric", metric)

                     .tag("datastream_id", datastream_id)

                     .time(ms_to_datetime(window_end))

                     .field("distinct_counts", distinct_counts)

                     .field("anomaly_score", anomaly_score))

                write_point(p, "Помилка запису в Influx")



        elif data["sensor_type"] == "alarm":

            try:

                true_ratio = float(data["true_ratio"])

                true_count = int(data["true_count"])

                anomaly_score = 0.0

            except (KeyError, TypeError, ValueError):

                continue

            ensure_model(datastream_id)

            if true_count == 0:

                continue


            else:

                anomaly_score = 0.0

                if metric == "smoke":

                    anomaly_score = 1.0

                    ml_models[datastream_id]['count'] += 1

                    dirty_models.add(datastream_id)

                    register_progress()

                    print(

                        f"SCORE | datastream_id={datastream_id} | sensor_type={sensor_type} | "

                        f"metric={metric} | score={anomaly_score:.3f} | "

                        f"samples_before={ml_models[datastream_id]['count'] - 1} | "

                        f"reset_count={ml_models[datastream_id]['reset_count']}"

                    )


                elif metric == "move":

                    features = {

                        "activity_level": true_ratio,

                        "time": time_of_event,

                        "day of week": day_of_week

                    }

                    anomaly_score = ml_models[datastream_id]['model'].score_one(features)

                    print(

                        f"SCORE | datastream_id={datastream_id} | sensor_type={sensor_type} | "

                        f"metric={metric} | score={anomaly_score:.3f} | "

                        f"samples_before={ml_models[datastream_id]['count']} | "

                        f"reset_count={ml_models[datastream_id]['reset_count']}"

                    )

                    ml_models[datastream_id]['model'].learn_one(features)

                    ml_models[datastream_id]['count'] += 1

                    dirty_models.add(datastream_id)

                    register_progress()

                    if ml_models[datastream_id]['cooldown'] > 0:

                        ml_models[datastream_id]['cooldown'] -= 1


                    elif ml_models[datastream_id]['count'] >= MIN_SAMPLES_FOR_DRIFT:

                        ml_models[datastream_id]['drift_detector'].update(anomaly_score)

                        if ml_models[datastream_id]['drift_detector'].drift_detected:

                            ml_models[datastream_id]['reset_count'] += 1

                            reset_count = ml_models[datastream_id]['reset_count']

                            print(

                                f"ВИЯВЛЕНО CONCEPT DRIFT! Сенсор {datastream_id}({metric}). "

                                f"Score: {anomaly_score:.3f}. Модель буде скинута."

                            )

                            p = (Point(DRIFT_MEASUREMENT)

                                 .tag("sensor_type", "alarm")

                                 .tag("metric", metric)

                                 .tag("datastream_id", datastream_id)

                                 .tag("detector", "ADWIN")

                                 .time(ms_to_datetime(window_end))

                                 .field("drift_value", anomaly_score)

                                 .field("count_before_reset", ml_models[datastream_id]['count'])

                                 .field("reset_count", reset_count))

                            try:

                                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

                            except Exception as e:

                                print(f"Помилка запису drift в Influx: {e}")

                            ml_models[datastream_id] = create_entry(reset_count=reset_count)

                            ml_models[datastream_id]['cooldown'] = DRIFT_COOLDOWN_WINDOWS

                            dirty_models.add(datastream_id)

                            save_ml_models()

                if metric == "smoke":

                    print(

                        f"КРИТИЧНА ПОДІЯ! Сенсор {datastream_id}({metric}). Smoke detected, Score: {anomaly_score:.3f}"

                    )

                    p = (Point(INFLUX_MEASUREMENT)

                         .tag("sensor_type", "alarm")

                         .tag("metric", metric)

                         .tag("datastream_id", datastream_id)

                         .time(ms_to_datetime(window_end))

                         .field("true_count", true_count)

                         .field("anomaly_score", anomaly_score))

                    try:

                        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

                    except Exception as e:

                        print(f"Помилка запису в Influx: {e}")


                elif anomaly_score > THRESHOLD and ml_models[datastream_id]['count'] >= MIN_SAMPLES_FOR_ANOMALY:

                    print(

                        f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). Значення {true_count:.2f}, Score: {anomaly_score:.3f}"

                    )

                    p = (Point(INFLUX_MEASUREMENT)

                         .tag("sensor_type", "alarm")

                         .tag("metric", metric)

                         .tag("datastream_id", datastream_id)

                         .time(ms_to_datetime(window_end))

                         .field("true_count", true_count)

                         .field("anomaly_score", anomaly_score))

                    try:

                        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

                    except Exception as e:

                        print(f"Помилка запису в Influx: {e}")



finally:
    save_ml_models()