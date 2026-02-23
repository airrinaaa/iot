import time
from Sensor import Sensor
import random
from uuid import UUID

class AlarmSensor(Sensor):

    def __init__(self, thing_id:UUID, metric:str, trigger_prob: float, cooldown:int):
        super().__init__(thing_id, metric)
        self.trigger_prob = trigger_prob
        self.cooldown = cooldown
        self.timer = 0

    def _get_value(self) -> bool:
        if self.timer > 0:
            self.timer -= 1
            return False
        else:
            if random.random() < self.trigger_prob:
                self.timer = self.cooldown
                return True
            else:
                return False