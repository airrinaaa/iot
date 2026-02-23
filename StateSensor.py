from Sensor import Sensor
import random
from uuid import UUID
class StateSensor(Sensor):
    def __init__(self, thing_id: UUID, metric: str, initial_state: int = 0, flip_prob: float = 0.1):
        super().__init__(thing_id, metric)
        self.state = initial_state
        self.flip_prob = flip_prob



    def _get_value(self) -> int | str:
        if random.random() < self.flip_prob:
            self.state = 1 - self.state
        if self.state:
            return "ON"
        else:
            return "OFF"




