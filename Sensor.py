from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, TypeAlias, Any

StateText: TypeAlias = Literal["ON", "OFF"]
ValueType: TypeAlias = float | int | StateText | bool
SensorType: TypeAlias = Literal["analog", "counter", "state", "alarm"]
from uuid import UUID, uuid5, NAMESPACE_URL


@dataclass
class Observation:
    """
    клас для збереження одного спостереження від сенсора
    містить унікальні ідентифікатори пристрою, потоку,
    вимірювану метрику, тип сенсора, номер повідомлення, час події, та значення
    """
    thing_id: UUID
    datastream_id: UUID
    metric: str
    sensor_type: SensorType
    seq: int
    event_time: datetime
    value: ValueType

    def to_dict(self) -> dict[str, Any]:
        """
        перетворення спостереження на словник
        """
        return {
            "thing_id": self.thing_id,
            "datastream_id": self.datastream_id,
            "metric": self.metric,
            "sensor_type": self.sensor_type,
            "seq": self.seq,
            "event_time": self.event_time,
            "value": self.value,
        }
class Sensor:
    """
    базовий клас сенсорів
    """
    SENSOR_TYPE: SensorType | None = None
    def __init__(self, thing_id: UUID, metric: str):
        self.thing_id = thing_id
        if not metric.strip():
            raise ValueError("metric cannot be empty")
        self.metric = metric
        self.datastream_id = uuid5(NAMESPACE_URL, f"{thing_id}:{metric}")
        self.seq = 0
        if self.SENSOR_TYPE is None:
            raise ValueError(f"SENSOR_TYPE is not defined for {self.__class__.__name__}")

        self.sensor_type = self.SENSOR_TYPE

    def generate_observation(self) -> Observation:
        """
        генерує новий запис спостереження від сенсора
        """
        event_time = datetime.now(timezone.utc)

        self.seq += 1
        value = self._get_value()

        return Observation(
            thing_id=self.thing_id,
            datastream_id=self.datastream_id,
            metric=self.metric,
            sensor_type=self.sensor_type,
            seq=self.seq,
            event_time=event_time,
            value=value,
        )
    def _get_value(self):
        """
        отримання поточного значення від сенсора
        """
        raise NotImplementedError