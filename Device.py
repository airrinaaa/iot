from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum

from uuid import UUID, uuid4

from Sensor import Sensor, Observation
from StateSensor import StateSensor
from CounterSensor import CounterSensor
from AnalogSensor import AnalogSensor
from AlarmSensor import AlarmSensor


class DeviceType(Enum):
    CLIMATE = "climate"
    SECURITY = "security"
    UTILITY = "utility"
    KITCHEN = "kitchen"


@dataclass()
class Device:
    thing_id: UUID
    device_type: DeviceType
    time_skew: int = 0
    jitter_prob: float = 0.15
    jitter_max: int = 900
    shuffle_prob: float = 0.1
    sensors: list[Sensor] = field(default_factory=list)


    def read_all(self) -> list[Observation]:
        publishing_time = datetime.now(timezone.utc)
        observations: list[Observation] = []
        for sensor in self.sensors:
            observation = sensor.generate_observation()
            observation.ingestion_time = publishing_time
            if self.time_skew != 0:
                observation.event_time += timedelta(milliseconds=self.time_skew)
            if random.random() < self.jitter_prob:
                observation.event_time -= timedelta(milliseconds=random.randint(50, self.jitter_max))
            observations.append(observation)
        if random.random() < self.shuffle_prob:
            random.shuffle(observations)
        return observations

    @staticmethod
    def create_by_type(deviceType: DeviceType) -> Device:
        new_id = uuid4()
        time_skew = random.randint(-300, 300)
        new_device = Device(new_id, deviceType, time_skew=time_skew)
        if deviceType == DeviceType.CLIMATE:
            initial_temp = random.uniform(18, 24)
            initial_humidity = random.uniform(35, 55)
            initial_co2 = random.uniform(450, 900)
            temperature_sensor = AnalogSensor(new_id, "temperature", -10, 35, initial_temp, 0.03, 0.08)
            humidity_sensor = AnalogSensor(new_id, "humidity", 10, 90, initial_humidity, 0.10, 0.35)
            co2_sensor = AnalogSensor(new_id, "co2", 350, 2500, initial_co2, 1.5, 6.0)
            new_device.sensors.append(temperature_sensor)
            new_device.sensors.append(humidity_sensor)
            new_device.sensors.append(co2_sensor)
        elif deviceType == DeviceType.SECURITY:
            door_sensor = StateSensor(new_id, "door", 0, 0.0005)
            move_sensor = AlarmSensor(new_id, "move", 0.0007, 30)
            alarm_sensor = StateSensor(new_id, "alarm", 0, 0.0002)
            new_device.sensors.append(door_sensor)
            new_device.sensors.append(move_sensor)
            new_device.sensors.append(alarm_sensor)
        elif deviceType == DeviceType.UTILITY:
            initial_electricity = random.uniform(0, 2000)
            initial_water = random.uniform(0, 50)
            initial_voltage = random.uniform(228, 232)
            electricity_sensor = CounterSensor(new_id, "electricity", 0.001, 0.01, initial_electricity)
            water_sensor = CounterSensor(new_id, "water", 0.00005, 0.002, initial_water)
            voltage_sensor = AnalogSensor(new_id, "voltage", 210, 250, initial_voltage, 0.2, 0.7)
            new_device.sensors.append(electricity_sensor)
            new_device.sensors.append(water_sensor)
            new_device.sensors.append(voltage_sensor)
        elif deviceType == DeviceType.KITCHEN:
            initial_fridge = random.uniform(3, 6)
            fridge_sensor = AnalogSensor(new_id, "fridge", 1, 8, initial_fridge, 0.03, 0.12)
            oven_sensor = StateSensor(new_id, "oven", 0, 0.0007)
            smoke_sensor = AlarmSensor(new_id, "smoke", 0.0000001, 60)
            new_device.sensors.append(fridge_sensor)
            new_device.sensors.append(oven_sensor)
            new_device.sensors.append(smoke_sensor)
        return new_device



