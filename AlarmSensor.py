from Sensor import Sensor
import random
from uuid import UUID

class AlarmSensor(Sensor):
    """
    клас сенсора тривоги
    """
    SENSOR_TYPE = "alarm"
    def __init__(self, thing_id:UUID, metric:str, trigger_prob: float, cooldown:int):
        super().__init__(thing_id, metric)
        self.trigger_prob = trigger_prob #ймовірність спрацювання тривоги
        self.cooldown = cooldown #кількість наступних викликів під час яких тривога не може знову спрацювати
        self.timer = 0

    def _get_value(self) -> bool:
        """
        отримання поточного значення сенсора тривоги (True - тривога спрацювала, False - не спрацювала)
        """
        if self.timer > 0:
            self.timer -= 1
            return False
        else:
            if random.random() < self.trigger_prob:
                self.timer = self.cooldown
                return True
            else:
                return False