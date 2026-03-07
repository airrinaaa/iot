import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
import influxdb_client
from influxdb_client import Point
import pickle
import os

from river import anomaly
from river.drift import ADWIN

from config import env

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
PROCESSED_TOPIC = env("PROCESSED_TOPIC", "processed_data")
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID_ANOMALY", "anomaly_detector_group_v3")

INFLUX_URL = env("INFLUX_URL", "http://localhost:8086")
INFLUX_BUCKET = env("INFLUX_BUCKET", "iot_bucket")
INFLUX_ORG = env("INFLUX_ORG", "ukma")
INFLUX_TOKEN = env("INFLUX_TOKEN")

INFLUX_MEASUREMENT = "anomalies"

MODEL_STATE_FILE = "anomaly_models.pkl"

SAVE_EVERY = 2000
DRIFT_MEASUREMENT = "concept_drift"
MIN_SAMPLES_FOR_DRIFT = 15
DRIFT_COOLDOWN_WINDOWS = 10
ADWIN_DELTA = 0.002
ADWIN_CLOCK = 32
ADWIN_GRACE_PERIOD = 20
ADWIN_MIN_WINDOW_LENGTH = 10

consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

if os.path.exists(MODEL_STATE_FILE):
    try:
        with open(MODEL_STATE_FILE, "rb") as f:
            ml_models = pickle.load(f)
    except Exception as e:
        print("PICKLE LOAD ERROR:", e)
        ml_models = {}
else:
    ml_models = {}

def save_ml_models():
    with open(MODEL_STATE_FILE, "wb") as f:
        pickle.dump(ml_models, f)

progress = {"processed_messages": 0}

def register_progress():
    progress["processed_messages"] += 1
    if progress["processed_messages"] % SAVE_EVERY == 0:
        save_ml_models()

def ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


print("Модуль виявлення аномалій запущено.")
THRESHOLD = 0.85
try:
    for message in consumer:
        try:
            raw = message.value.decode("utf-8")
            data = json.loads(raw)
        except Exception as e:
            continue
        datastream_id = str(data["datastream_id"])
        metric = str(data["metric"])
        window_end = int(data["window_end"])
        time_of_event = ms_to_datetime(window_end).hour
        day_of_week = ms_to_datetime(window_end).weekday()
        if data["sensor_type"] == "analog":
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
                    f"ЗБІЙ СЕНСОРА (Hardware Fault)! Датчик {datastream_id}({metric}) видав неможливе значення: {anomaly_value:.2f} (середнє вікна: {average:.2f})")
                p = (Point(INFLUX_MEASUREMENT)
                     .tag("sensor_type", "analog")
                     .tag("metric", metric)
                     .tag("datastream_id", datastream_id)
                     .tag("anomaly_type", "hardware_fault")
                     .time(ms_to_datetime(window_end))
                     .field("anomaly_value", anomaly_value)
                     .field("anomaly_score", 1.0))
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
                except Exception as e:
                    print(f"Помилка запису в Influx: {e}")
                continue

            spread = max_value - min_value
            if datastream_id not in ml_models:
                ml_models[datastream_id] = {
                    'model': anomaly.HalfSpaceTrees
                    (n_trees=25, height=8,window_size=50,seed=42),
                    'count': 0,
                    "drift_detector": ADWIN(delta=ADWIN_DELTA,clock=ADWIN_CLOCK,grace_period=ADWIN_GRACE_PERIOD,
                                            min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                    "reset_count": 0,
                    "cooldown": 0}
            features = {"average": average, "spread": spread, "time": time_of_event, "day of week": day_of_week}
            anomaly_score = ml_models[datastream_id]['model'].score_one(features)
            ml_models[datastream_id]['model'].learn_one(features)
            ml_models[datastream_id]['count'] += 1
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
                        .tag("sensor_type", "analog")
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
                    ml_models[datastream_id] = {
                        'model': anomaly.HalfSpaceTrees
                        (n_trees=25,height=8,window_size=50,seed=42),
                        'count': 0,
                        'drift_detector': ADWIN(delta=ADWIN_DELTA,clock=ADWIN_CLOCK,grace_period=ADWIN_GRACE_PERIOD,
                                                min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                        'reset_count': reset_count,
                        'cooldown': DRIFT_COOLDOWN_WINDOWS}
                    save_ml_models()
            if anomaly_score > THRESHOLD and ml_models[datastream_id]['count'] > 30:
                print(f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). Значення {average:.2f}, Score: {anomaly_score:.3f}")
                p = (Point(INFLUX_MEASUREMENT)
                    .tag("sensor_type", "analog")
                    .tag("metric", metric)
                    .tag("datastream_id", datastream_id)
                    .time(ms_to_datetime(window_end))
                    .field("average", average)
                    .field("anomaly_score", anomaly_score))
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
                except Exception as e:
                    print(f"Помилка запису в Influx: {e}")
        elif data["sensor_type"] == "counter":
            try:
                delta = float(data["delta"])
            except (KeyError, TypeError, ValueError):
                continue
            if delta == 0:
                continue
            if datastream_id not in ml_models:
                ml_models[datastream_id] = {
                    'model': anomaly.HalfSpaceTrees
                    (n_trees=25, height=8, window_size=50, seed=42),
                    'count': 0,
                    "drift_detector": ADWIN(delta=ADWIN_DELTA, clock=ADWIN_CLOCK, grace_period=ADWIN_GRACE_PERIOD,
                                            min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                    "reset_count": 0,
                    "cooldown": 0}
            features = {"delta": delta, "time": time_of_event, "day of week": day_of_week}
            anomaly_score = ml_models[datastream_id]['model'].score_one(features)
            ml_models[datastream_id]['model'].learn_one(features)
            ml_models[datastream_id]['count'] += 1
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
                        .tag("sensor_type", "counter")
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
                    ml_models[datastream_id] = {
                        'model': anomaly.HalfSpaceTrees
                        (n_trees=25,height=8,window_size=50,seed=42),
                        'count': 0,
                        'drift_detector': ADWIN(delta=ADWIN_DELTA,clock=ADWIN_CLOCK,grace_period=ADWIN_GRACE_PERIOD,
                                                min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                        'reset_count': reset_count,
                        'cooldown': DRIFT_COOLDOWN_WINDOWS}
                    save_ml_models()
            if anomaly_score > THRESHOLD and ml_models[datastream_id]['count'] > 30:
                print(f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). Значення {delta:.2f}, Score: {anomaly_score:.3f}")
                p = (Point(INFLUX_MEASUREMENT)
                    .tag("sensor_type", "counter")
                    .tag("metric", metric)
                    .tag("datastream_id", datastream_id)
                    .time(ms_to_datetime(window_end))
                    .field("delta", delta)
                    .field("anomaly_score", anomaly_score))
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
                except Exception as e:
                    print(f"Помилка запису в Influx: {e}")
        elif data["sensor_type"] == "state":
            try:
                distinct_counts = int(data["distinct_counts"])
            except (KeyError, TypeError, ValueError):
                continue
            if datastream_id not in ml_models:
                ml_models[datastream_id] = {
                    'model': anomaly.HalfSpaceTrees
                    (n_trees=25, height=8, window_size=50, seed=42),
                    'count': 0,
                    "drift_detector": ADWIN(delta=ADWIN_DELTA, clock=ADWIN_CLOCK, grace_period=ADWIN_GRACE_PERIOD,
                                            min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                    "reset_count": 0,
                    "cooldown": 0}
            features = {"distinct_counts": distinct_counts, "time":time_of_event, "day of week": day_of_week}
            anomaly_score = ml_models[datastream_id]['model'].score_one(features)
            ml_models[datastream_id]['model'].learn_one(features)
            ml_models[datastream_id]['count'] += 1
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
                        .tag("sensor_type", "state")
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
                    ml_models[datastream_id] = {
                        'model': anomaly.HalfSpaceTrees
                        (n_trees=25,height=8,window_size=50,seed=42),
                        'count': 0,
                        'drift_detector': ADWIN(delta=ADWIN_DELTA,clock=ADWIN_CLOCK,grace_period=ADWIN_GRACE_PERIOD,
                                                min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                        'reset_count': reset_count,
                        'cooldown': DRIFT_COOLDOWN_WINDOWS}
                    save_ml_models()
            if anomaly_score > THRESHOLD and ml_models[datastream_id]['count'] > 30:
                print(f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). Значення {distinct_counts:.2f}, Score: {anomaly_score:.3f}")
                p = (Point(INFLUX_MEASUREMENT)
                    .tag("sensor_type", "state")
                    .tag("metric", metric)
                    .tag("datastream_id", datastream_id)
                    .time(ms_to_datetime(window_end))
                    .field("distinct_counts", distinct_counts)
                    .field("anomaly_score", anomaly_score))
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
                except Exception as e:
                    print(f"Помилка запису в Influx: {e}")
        elif data["sensor_type"] == "alarm":
            try:
                true_ratio = float(data["true_ratio"])
                true_count = int(data["true_count"])
                anomaly_score = 0.0
            except (KeyError, TypeError, ValueError):
                continue
            if datastream_id not in ml_models:
                ml_models[datastream_id] = {
                    'model': anomaly.HalfSpaceTrees
                    (n_trees=25, height=8, window_size=50, seed=42),
                    'count': 0,
                    "drift_detector": ADWIN(delta=ADWIN_DELTA, clock=ADWIN_CLOCK, grace_period=ADWIN_GRACE_PERIOD,
                                            min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                    "reset_count": 0,
                    "cooldown": 0}
            if true_count == 0:
                continue
            else:
                anomaly_score = 0.0
                if metric == "smoke":
                    anomaly_score = 1.0
                    ml_models[datastream_id]['count'] += 1
                    register_progress()
                elif metric == "move":
                    features = {"activity_level": true_ratio, "time": time_of_event, "day of week": day_of_week}
                    anomaly_score = ml_models[datastream_id]['model'].score_one(features)
                    ml_models[datastream_id]['model'].learn_one(features)
                    ml_models[datastream_id]['count'] += 1
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
                            ml_models[datastream_id] = {
                                'model': anomaly.HalfSpaceTrees
                                (n_trees=25, height=8, window_size=50, seed=42),
                                'count': 0,
                                'drift_detector': ADWIN(delta=ADWIN_DELTA, clock=ADWIN_CLOCK,
                                                        grace_period=ADWIN_GRACE_PERIOD,
                                                        min_window_length=ADWIN_MIN_WINDOW_LENGTH),
                                'reset_count': reset_count,
                                'cooldown': DRIFT_COOLDOWN_WINDOWS}
                            save_ml_models()
                if metric == "smoke":
                    print(f"КРИТИЧНА ПОДІЯ! Сенсор {datastream_id}({metric}). Smoke detected, Score: {anomaly_score:.3f}")
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
                elif anomaly_score > THRESHOLD and ml_models[datastream_id]['count'] > 30:
                    print(
                        f"ВИЯВЛЕНО АНОМАЛІЮ! Сенсор {datastream_id}({metric}). Значення {true_count:.2f}, Score: {anomaly_score:.3f}")
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





