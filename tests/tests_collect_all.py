from CollectAll import Type, type_by_metric, type_by_value


def test_type_by_metric_known_metrics():
    assert type_by_metric({"metric": "temperature", "value": 10}) == Type.ANALOG
    assert type_by_metric({"metric": "water", "value": 10}) == Type.COUNTER
    assert type_by_metric({"metric": "door", "value": "ON"}) == Type.STATE
    assert type_by_metric({"metric": "move", "value": True}) == Type.ALARM
    assert type_by_metric({"metric": "humidity", "value": 50}) == Type.ANALOG
    assert type_by_metric({"metric": "co2", "value": 500}) == Type.ANALOG
    assert type_by_metric({"metric": "voltage", "value": 220}) == Type.ANALOG
    assert type_by_metric({"metric": "electricity", "value": 12}) == Type.COUNTER
    assert type_by_metric({"metric": "oven", "value": "OFF"}) == Type.STATE
    assert type_by_metric({"metric": "alarm", "value": "ON"}) == Type.STATE
    assert type_by_metric({"metric": "fridge", "value": 4}) == Type.ANALOG
    assert type_by_metric({"metric": "smoke", "value": True}) == Type.ALARM


def test_type_by_value_string():
    assert type_by_value({"value": "ON"}) == Type.STATE


def test_type_by_value_bool():
    assert type_by_value({"value": True}) == Type.ALARM


def test_type_by_value_number():
    assert type_by_value({"value": 12.5}) == Type.ANALOG
    assert type_by_value({"value": 7}) == Type.ANALOG


def test_type_by_value_none():
    assert type_by_value({"value": None}) == Type.ANALOG


def test_type_by_metric_fallback_string():
    assert type_by_metric({"metric": "unknown", "value": "OFF"}) == Type.STATE


def test_type_by_metric_fallback_bool():
    assert type_by_metric({"metric": "unknown", "value": False}) == Type.ALARM


def test_type_by_metric_fallback_number():
    assert type_by_metric({"metric": "unknown", "value": 5.1}) == Type.ANALOG


def test_type_by_metric_fallback_none():
    assert type_by_metric({"metric": "unknown", "value": None}) == Type.ANALOG