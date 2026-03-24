from Sensor import Sensor
import random
from uuid import UUID

class AnalogSensor(Sensor):
    """
    клас аналогового сенсора (для генерації значень, які плавно змінюються у межах заданого діапазону)
    """
    SENSOR_TYPE = "analog"
    def __init__(self, thing_id: UUID, metric:str, min_value: float, max_value: float, initial_value: float, drift: float, noise: float):
        super().__init__(thing_id, metric)
        if min_value > max_value:
            raise ValueError("min_value must be less or equal max_value")
        self.min_value = min_value
        if initial_value < min_value or initial_value > max_value:
            raise ValueError("initial_value must be between {} and {}".format(min_value, max_value))
        self.max_value = max_value
        self.value = initial_value
        if drift < 0:
            raise ValueError("drift must be non-negative")
        self.drift = drift
        if noise < 0:
            raise ValueError("noise must be non-negative")
        self.noise = noise

    def _get_value(self) -> float:
        """
        отримання поточного значення аналогового сенсора
        """
        change = random.uniform(-self.drift, self.drift)
        self.value += change

        if self.value > self.max_value:
            self.value = self.max_value
        elif self.value < self.min_value:
            self.value = self.min_value
        value_with_noise = self.value + random.gauss(0,self.noise)
        value_with_noise = max(self.min_value, min(self.max_value, value_with_noise))
        return round(value_with_noise, 3)
       