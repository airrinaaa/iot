from datetime import datetime
from SensorTimestampAssigner import SensorTimestampAssigner



def test_extract_timestamp_known_iso_time():
    assigner = SensorTimestampAssigner()
    result = assigner.extract_timestamp({"event_time": "1970-01-01T00:00:01+00:00"}, 0)
    assert result == 1000


def test_extract_timestamp_timezone_aware_datetime():
    assigner = SensorTimestampAssigner()
    result = assigner.extract_timestamp({"event_time": "2026-03-09T12:00:00+02:00"}, 0)
    expected = int(datetime.fromisoformat("2026-03-09T12:00:00+02:00").timestamp() * 1000)
    assert result == expected


def test_extract_timestamp_keeps_correct_order():
    assigner = SensorTimestampAssigner()
    first = assigner.extract_timestamp({"event_time": "2026-03-09T10:00:00+00:00"}, 0)
    second = assigner.extract_timestamp({"event_time": "2026-03-09T10:00:01+00:00"}, 0)
    assert first < second


def test_extract_timestamp_is_in_milliseconds():
    assigner = SensorTimestampAssigner()
    result = assigner.extract_timestamp({"event_time": "1970-01-01T00:00:10+00:00"}, 0)
    assert result == 10000

