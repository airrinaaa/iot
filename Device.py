from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    sensors: list[Sensor] = field(default_factory=list)
    def read_all(self) -> list[Observation]:
        observations: list[Observation] = []
        for sensor in self.sensors:
            observations.append(sensor.generate_observation())
        return observations

    @staticmethod
    def create_by_type(deviceType: DeviceType) -> Device:
        new_id = uuid4()
        new_device = Device(new_id, deviceType)
        if deviceType == DeviceType.CLIMATE:
            initial_temp = random.uniform(15, 25)
            initial_humidity = random.uniform(30, 60)
            initial_co2 = random.uniform(400, 800)
            temperature_sensor = AnalogSensor(new_id, "temperature", -10, 40, initial_temp, 0.1, 0.05)
            humidity_sensor = AnalogSensor(new_id, "humidity", 0, 100, initial_humidity, 0.5, 0.2)
            co2_sensor = AnalogSensor(new_id, "co2", 300, 5000, initial_co2, 5, 1)
            new_device.sensors.append(temperature_sensor)
            new_device.sensors.append(humidity_sensor)
            new_device.sensors.append(co2_sensor)
        elif deviceType == DeviceType.SECURITY:
            door_sensor = StateSensor(new_id, "door", 0, 0.005)
            move_sensor = AlarmSensor(new_id, "move", 0.002, 20)
            alarm_sensor = StateSensor(new_id, "alarm", 0, 0.001)
            new_device.sensors.append(door_sensor)
            new_device.sensors.append(move_sensor)
            new_device.sensors.append(alarm_sensor)
        elif deviceType == DeviceType.UTILITY:
            initial_electricity = random.uniform(0, 5000)
            initial_water = random.uniform(0, 200)
            initial_voltage = random.uniform(225, 235)
            electricity_sensor = CounterSensor(new_id, "electricity", 0.01, 0.15, initial_electricity)
            water_sensor = CounterSensor(new_id, "water", 0.001, 0.02, initial_water)
            voltage_sensor = AnalogSensor(new_id, "voltage", 210, 250, initial_voltage, 0.5, 0.1)
            new_device.sensors.append(electricity_sensor)
            new_device.sensors.append(water_sensor)
            new_device.sensors.append(voltage_sensor)
        elif deviceType == DeviceType.KITCHEN:
            initial_fridge = random.uniform(3, 5)
            fridge_sensor = AnalogSensor(new_id, "fridge", 0, 12, initial_fridge, 0.05, 0.1)
            oven_sensor = StateSensor(new_id, "oven", 0, 0.01)
            smoke_sensor = AlarmSensor(new_id, "smoke", 0.0001, 30)
            new_device.sensors.append(fridge_sensor)
            new_device.sensors.append(oven_sensor)
            new_device.sensors.append(smoke_sensor)
        return new_device



