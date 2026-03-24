from Sensor import Sensor
import random
from uuid import UUID

class CounterSensor(Sensor):
    """
    клас для сенсора-лічильника (для генерації значень які поступово збільшуються)
    """
    SENSOR_TYPE = "counter"
    def __init__(self, thing_id: UUID, metric: str, min_inc: float, max_inc: float, initial_value: float):
        super().__init__(thing_id, metric)
        if min_inc > max_inc:
            raise ValueError("min_inc must be less or equal max_inc")
        if min_inc < 0 or max_inc < 0:
            raise ValueError("min_inc must be non-negative")
        self.min_inc = min_inc
        self.max_inc = max_inc
        if initial_value < 0:
            raise ValueError("initial_value must be greater than 0")
        self.value = initial_value



    def _get_value(self) -> float:
        """
        отримання поточного значення сенсора-лічильника
        """
        self.value += random.uniform(self.min_inc, self.max_inc)
        return round(self.value, 3)
