from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, TypeAlias, Any

StateText: TypeAlias = Literal["ON", "OFF"]
ValueType: TypeAlias = float | int | StateText
from uuid import UUID, uuid5, NAMESPACE_URL


@dataclass
class Observation:
    thing_id: UUID
    datastream_id: UUID
    metric: str
    seq: int
    event_time: datetime
    ingestion_time: datetime
    value: ValueType

    def to_dict(self) -> dict[str, Any]:
        return {
            "thing_id": self.thing_id,
            "datastream_id": self.datastream_id,
            "metric": self.metric,
            "seq": self.seq,
            "event_time": self.event_time,
            "ingestion_time": self.ingestion_time,
            "value": self.value,
        }
class Sensor:
    def __init__(self, thing_id: UUID, metric: str):
        self.thing_id = thing_id
        self.metric = metric
        self.datastream_id = uuid5(NAMESPACE_URL, f"{thing_id}:{metric}")
        self.seq = 0

    def generate_observation(self) -> Observation:
        event_time = datetime.now(timezone.utc)

        ingestion_time = datetime.now(timezone.utc)

        self.seq += 1
        value = self._get_value()

        return Observation(
            thing_id=self.thing_id,
            datastream_id=self.datastream_id,
            metric=self.metric,
            seq=self.seq,
            event_time=event_time,
            ingestion_time=ingestion_time,
            value=value,
        )

    def _get_value(self):
        raise NotImplementedError