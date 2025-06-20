"""
MQTT Smart Farm Sensor Publisher
Publishes temperature, soil moisture, light intensity, and humidity.
Adds message ID to support message loss tracking.
Occasionally injects outliers and duplicates to test system robustness.
"""

import time
import json
import paho.mqtt.client as mqtt
from faker import Faker
from datetime import datetime, UTC
import random

# MQTT config
MQTT_BROKER_URL = "localhost"
MQTT_PUBLISH_TOPIC = "yapjayann/temperature"

# Connect to broker
mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL)

# Faker instance
fake = Faker()

# Message counter
message_id = 0
last_payload = None

# Probabilities
OUTLIER_PROBABILITY = 0.08     # 8%
DUPLICATE_PROBABILITY = 0.05   # 5%

while True:
    tag = "NORMAL"

    # Occasionally send a duplicate
    if last_payload and random.random() < DUPLICATE_PROBABILITY:
        mqttc.publish(MQTT_PUBLISH_TOPIC, last_payload)
        tag = "DUPLICATE"
        sent = json.loads(last_payload)
        print(f"{tag} | 📤 ID {sent['message_id']} | Temp: {sent['temperature']}°C | Moisture: {sent['soil_moisture']}% | Light: {sent['light_intensity']} lux | Humidity: {sent['humidity']}%")
        time.sleep(1)
        continue

    # Generate new message
    message_id += 1
    timestamp = datetime.now(UTC).isoformat()

    is_outlier = random.random() < OUTLIER_PROBABILITY

    if is_outlier:
        # Inject one or more bad values
        temperature = random.choice([fake.random_int(51, 70), fake.random_int(-30, -5)])
        soil_moisture = random.choice([fake.random_int(-10, -1), fake.random_int(101, 150)])
        tag = "OUTLIER"
    else:
        temperature = fake.random_int(10, 35)
        soil_moisture = fake.random_int(30, 90)

    light_intensity = fake.random_int(100, 1000)
    humidity = fake.random_int(40, 90)

    payload = json.dumps({
        "message_id": message_id,
        "sent_time": timestamp,
        "temperature": temperature,
        "soil_moisture": soil_moisture,
        "light_intensity": light_intensity,
        "humidity": humidity
    })

    mqttc.publish(MQTT_PUBLISH_TOPIC, payload)
    print(f"{tag} | 📤 ID {message_id} | Temp: {temperature}°C | Moisture: {soil_moisture}% | Light: {light_intensity} lux | Humidity: {humidity}%")

    last_payload = payload
    time.sleep(1)
