from scipy.constants import value

from Sensor import Sensor
import random
from uuid import UUID

class AnalogSensor(Sensor):

    def __init__(self, thing_id: UUID, metric:str, min_value: float, max_value: float, initial_value: float, drift: float, noise: float):
        super().__init__(thing_id, metric)
        self.min_value = min_value
        self.max_value = max_value
        self.value = initial_value
        self.drift = drift
        self.noise = noise

    def _get_value(self) -> float:
        change = random.uniform(-self.drift, self.drift)
        self.value += change

        if self.value > self.max_value:
            self.value = self.max_value
        elif self.value < self.min_value:
            self.value = self.min_value
        value_with_noise = self.value + random.gauss(0,self.noise)
        return round(value_with_noise, 3)
       