"""
Multi-Sensor MQTT Smart Farm Publisher
Publishes temperature, soil moisture, light intensity, and humidity
Simulates multiple sensors using threads
Injects occasional outliers and duplicates
"""

import time
import json
import paho.mqtt.client as mqtt
from faker import Faker
from datetime import datetime, UTC
import random
import threading

# ====== Config ======
MQTT_BROKER_URL = "localhost"
MQTT_PUBLISH_TOPIC = "yapjayann/sensors"
NUM_SENSORS = 5              # Number of virtual sensors
PUBLISH_INTERVAL = 1.0       # Seconds between each sensor publish
OUTLIER_PROBABILITY = 0.08   # 8%
DUPLICATE_PROBABILITY = 0.05 # 5%

# =====================
faker = Faker()

def simulate_sensor(sensor_id):
    mqttc = mqtt.Client()
    mqttc.connect(MQTT_BROKER_URL)

    message_id = 0
    last_payload = None

    while True:
        tag = f"SENSOR {sensor_id} | NORMAL"

        # Send duplicate occasionally, as network issues or retries might cause duplicate packets
        if last_payload and random.random() < DUPLICATE_PROBABILITY:
            mqttc.publish(MQTT_PUBLISH_TOPIC, last_payload)
            sent = json.loads(last_payload)
            print(
                f"SENSOR {sensor_id} | DUPLICATE | 📤 ID {sent['message_id']} | "
                f"Temp: {sent['temperature']}°C | Moisture: {sent['soil_moisture']}% | "
                f"Light: {sent['light_intensity']} lux | Humidity: {sent['humidity']}%"
            )
            time.sleep(PUBLISH_INTERVAL)
            continue

        message_id += 1
        timestamp = datetime.now(UTC).isoformat()

        # Outlier injection
        if random.random() < OUTLIER_PROBABILITY:
            temperature = random.choice([faker.random_int(51, 70), faker.random_int(-30, -5)])
            soil_moisture = random.choice([faker.random_int(-10, -1), faker.random_int(101, 150)])
            tag = f"SENSOR {sensor_id} | OUTLIER"
        else:
            temperature = faker.random_int(10, 35)
            soil_moisture = faker.random_int(30, 90)

        light_intensity = faker.random_int(100, 1000)
        humidity = faker.random_int(40, 90)

        payload = json.dumps({
            "sensor_id": sensor_id,
            "message_id": message_id,
            "sent_time": timestamp,
            "temperature": temperature,
            "soil_moisture": soil_moisture,
            "light_intensity": light_intensity,
            "humidity": humidity
        })

        mqttc.publish(MQTT_PUBLISH_TOPIC, payload)
        print(
            f"{tag} | 📤 ID {message_id} | Temp: {temperature}°C | "
            f"Moisture: {soil_moisture}% | Light: {light_intensity} lux | Humidity: {humidity}%"
        )

        last_payload = payload
        time.sleep(PUBLISH_INTERVAL)

# Start a thread per virtual sensor
for sensor_id in range(1, NUM_SENSORS + 1):
    threading.Thread(target=simulate_sensor, args=(sensor_id,), daemon=True).start()

# Keep main thread alive
while True:
    time.sleep(1)
