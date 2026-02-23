from Sensor import Sensor
import random
from uuid import UUID

class CounterSensor(Sensor):

    def __init__(self, thing_id: UUID, metric: str, min_inc: float, max_inc: float, initial_value: float):
        super().__init__(thing_id, metric)
        self.min_inc = min_inc
        self.max_inc = max_inc
        self.value = initial_value



    def _get_value(self) -> int:
        self.value += random.uniform(self.min_inc, self.max_inc)
        return round(self.value, 3)
