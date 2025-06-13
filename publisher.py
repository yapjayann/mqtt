"""
MQTT Smart temperature Sensor
"""

import time

import paho.mqtt.client as mqtt
from faker import Faker
import json

# let's connect to the MQTT broker
MQTT_BROKER_URL    = "localhost"
MQTT_PUBLISH_TOPIC = "yapjayann/temperature" #

mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL)

# Init faker our fake data provider
fake = Faker()


from datetime import datetime, UTC

# Infinite loop of fake data sent to the Broker
while True:
    temperature = fake.random_int(min=0, max=30)
    timestamp = datetime.now(UTC).isoformat()  # ISO 8601 format for clarity
    payload = json.dumps({
        "temperature": temperature,
        "sent_time": timestamp
    })
    mqttc.publish(MQTT_PUBLISH_TOPIC, payload)
    print(f"Published temperature: {temperature} at {timestamp}")
    time.sleep(1)