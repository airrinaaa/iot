from enum import Enum

from pyflink.datastream.functions import WindowFunction
from datetime import datetime

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



class CollectAll(WindowFunction):
    def apply(self, key, window, inputs):
        all_data = list(inputs)
        type = type_by_metric(all_data[0])
        window_start = datetime.fromtimestamp(window.start / 1000.0).strftime('%H:%M:%S')
        window_end = datetime.fromtimestamp(window.end / 1000.0).strftime('%H:%M:%S')
        if type == Type.COUNTER:
            sorted_data = sorted(all_data, key=lambda x: x["event_time"])
            yield {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": len(all_data),
                "metric": all_data[0]['metric'],
                "first value":sorted_data[0]["value"],
                "last value":sorted_data[-1]["value"],
                "delta": sorted_data[-1]["value"] - sorted_data[0]["value"],
            }
        elif type == Type.ANALOG:
            all_values = [event["value"] for event in all_data]
            all_values = sorted(all_values)
            count = len(all_values)
            sum_value = sum(all_values)

            yield {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": len(all_data),
                "metric": all_data[0]['metric'],
                "min value": all_values[0],
                "max value": all_values[-1],
                "average": sum_value/count,
            }
        elif type == Type.STATE:
            sorted_data = sorted(all_data, key=lambda x: x["event_time"])
            all_values = [event["value"] for event in sorted_data]
            count_on = 0
            count_off = 0
            for val in all_values:
                if val == "ON":
                    count_on += 1
                elif val == "OFF":
                    count_off += 1

            yield {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": len(all_data),
                "metric": all_data[0]['metric'],
                "distinct counts": len(set(all_values)),
                "on-count": count_on,
                "off-count": count_off,
                "last state": sorted_data[-1]["value"],
            }
        elif type == Type.ALARM:
            sorted_data = sorted(all_data, key=lambda x: x["event_time"])
            all_values = [event["value"] for event in sorted_data]
            count_true = 0
            count_false = 0
            for val in all_values:
                if val:
                    count_true += 1
                elif not val:
                    count_false += 1
            yield {
                "datastream_id": key,
                "window": f"{window_start} - {window_end}",
                "count": len(all_data),
                "metric": all_data[0]['metric'],
                "distinct counts": len(set(all_values)),
                "triggered": any(all_values),
                "true-count": count_true,
                "false-count": count_false,
                "true_ratio": count_true / len(all_values),
                "last value": sorted_data[-1]["value"],
            }

