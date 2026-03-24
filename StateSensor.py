from Sensor import Sensor
import random
from uuid import UUID
class StateSensor(Sensor):
    """
    клас сенсора зі станами (повертає ON або OFF)
    """
    SENSOR_TYPE = "state"
    def __init__(self, thing_id: UUID, metric: str, initial_state: int = 0, flip_prob: float = 0.1):
        super().__init__(thing_id, metric)
        if initial_state != 0 and initial_state != 1:
            raise ValueError("initial_state must be 0 or 1")
        if flip_prob < 0 or flip_prob > 1:
            raise ValueError("flip_prob must be between 0 and 1")
        self.state = initial_state
        self.flip_prob = flip_prob #ймовірність зміни стану при кожному виклику



    def _get_value(self) -> str:
        """
        отримання поточного значення стану сенсора
        """
        if random.random() < self.flip_prob:
            self.state = 1 - self.state
        if self.state:
            return "ON"
        else:
            return "OFF"




