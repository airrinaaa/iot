from Device import Device, DeviceType
from AnalogSensor import AnalogSensor

from StateSensor import StateSensor
from AlarmSensor import AlarmSensor


def test_create_climate_device():
    device = Device.create_by_type(DeviceType.CLIMATE, 1)
    metrics = [sensor.metric for sensor in device.sensors]
    assert len(device.sensors) == 3
    assert metrics == ["temperature", "humidity", "co2"]

def test_create_security_device():
    device = Device.create_by_type(DeviceType.SECURITY,1 )
    metrics = [sensor.metric for sensor in device.sensors]
    assert metrics == ["door", "move", "alarm"]


def test_create_utility_device():
    device = Device.create_by_type(DeviceType.UTILITY, 1)
    metrics = [sensor.metric for sensor in device.sensors]
    assert metrics == ["electricity", "water", "voltage"]


def test_create_kitchen_device():
    device = Device.create_by_type(DeviceType.KITCHEN, 1)
    metrics = [sensor.metric for sensor in device.sensors]
    assert metrics == ["fridge", "oven", "smoke"]


def test_sensor_types_for_climate():
    device = Device.create_by_type(DeviceType.CLIMATE, 1)
    assert isinstance(device.sensors[0], AnalogSensor)
    assert isinstance(device.sensors[1], AnalogSensor)
    assert isinstance(device.sensors[2], AnalogSensor)


def test_sensor_types_for_security():
    device = Device.create_by_type(DeviceType.SECURITY, 1)
    assert isinstance(device.sensors[0], StateSensor)
    assert isinstance(device.sensors[1], AlarmSensor)
    assert isinstance(device.sensors[2], StateSensor)


