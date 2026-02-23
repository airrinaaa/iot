from datetime import datetime
from pyflink.common.watermark_strategy import TimestampAssigner


class SensorTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        date_time = datetime.fromisoformat(element['event_time'].replace('Z', '+00:00'))
        return int(date_time.timestamp() * 1000)