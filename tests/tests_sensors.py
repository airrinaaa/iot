from uuid import uuid4
from Sensor import Sensor
from AnalogSensor import AnalogSensor
from CounterSensor import CounterSensor
from StateSensor import StateSensor
from AlarmSensor import AlarmSensor


def test_datastream_id():
    thing_id = uuid4()
    sensor1 = Sensor(thing_id, "test_metric")
    sensor2 = Sensor(thing_id, "test_metric")
    sensor3= Sensor(thing_id, "test_metric3")
    sensor4 = Sensor(thing_id, "test_metric4")
    assert sensor3.datastream_id != sensor4.datastream_id
    assert sensor1.datastream_id == sensor2.datastream_id



def test_generate_observation_sequence():
    thing_id = uuid4()
    sensor = AnalogSensor(thing_id, "temperature", -10, 35, 15, 0.03, 0.08)
    first = sensor.generate_observation()
    second = sensor.generate_observation()
    assert first.seq == 1
    assert second.seq == 2

def test_counter_value_always_increases():
    sensor = CounterSensor(uuid4(), "water", 100.0, 1.0, 2.0)
    value1 = sensor._get_value()
    value2 = sensor._get_value()
    assert value2 >= value1

def test_state_sensor_returns_on_or_off():
    sensor = StateSensor(uuid4(), "door", 0, 0.5)
    value = sensor._get_value()
    assert value in ["ON", "OFF"]

def test_alarm_sensor_first_call_true_when_trigger_prob_one():
    sensor = AlarmSensor(uuid4(), "move",1.0, 2)
    value = sensor._get_value()
    assert value is True


def test_alarm_sensor_returns_false_during_cooldown():
    sensor = AlarmSensor(uuid4(), "move", 1.0, 2)
    first = sensor._get_value()
    second = sensor._get_value()
    assert first is True
    assert second is False

def test_alarm_sensor_cooldown():
    sensor = AlarmSensor(uuid4(), "move",1.0, 2)
    value1 = sensor._get_value()
    value2 = sensor._get_value()
    value3 = sensor._get_value()
    value4 = sensor._get_value()
    assert value1 is True
    assert value2 is False
    assert value3 is False
    assert value4 is True

def test_state_sensor():
    sensor = StateSensor(uuid4(), "door", 0, 1.0)
    value1 = sensor._get_value()
    value2 = sensor._get_value()
    assert value1 == "ON"
    assert value2 == "OFF"

def test_counter_with_fixed_increment():
    sensor = CounterSensor(uuid4(), "electricity", 2.0, 2.0, 100.0)
    value1 = sensor._get_value()
    value2 = sensor._get_value()
    assert value1 == 102.0
    assert value2 == 104.0

def test_analog_min_max_value():
    sensor = AnalogSensor(uuid4(), "temperature", 100.0, 100.0, 100.0, 3.0, 0.0)
    assert sensor._get_value() == 100.0

