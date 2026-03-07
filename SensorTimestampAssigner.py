from datetime import datetime
from pyflink.common.watermark_strategy import TimestampAssigner


class SensorTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(datetime.fromisoformat(value["event_time"]).timestamp() * 1000)