# === Imports ===
import time
import json
import paho.mqtt.client as mqtt             # MQTT client for publishing messages
from faker import Faker                     # Used to generate random but realistic values
from datetime import datetime, UTC          # For timestamping messages
import random
import threading                            # To simulate concurrent sensor behavior
import sys
import signal                               # For graceful shutdown on Ctrl+C

# === Configuration Constants ===
MQTT_BROKER_URL = "mqtt-broker"               # MQTT broker address
MQTT_PUBLISH_TOPIC = "yapjayann/sensors"    # Topic to publish sensor data
NUM_SENSORS = 5                             # Number of simulated sensors
MESSAGES_PER_SENSOR = 50                    # Number of messages each sensor will send
PUBLISH_INTERVAL = 1.0                      # Delay (in seconds) between publishes
OUTLIER_PROBABILITY = 0.08                  # Chance of sending an outlier message
DUPLICATE_PROBABILITY = 0.05                # Chance of sending a duplicate message

faker = Faker()                             # Faker instance for generating random data
stop_event = threading.Event()              # Thread-safe flag to allow graceful shutdown

# === Sensor Simulation Function ===
def simulate_sensor(sensor_id):
    # Create MQTT client and connect to broker
    mqttc = mqtt.Client()
    mqttc.connect(MQTT_BROKER_URL)

    message_id = 0
    last_payload = None                     # To reuse for sending duplicates
    sent_count = 0                          # Track how many messages sent

    # Keep sending data unless we've hit the max or received a stop signal
    while sent_count < MESSAGES_PER_SENSOR and not stop_event.is_set():
        tag = f"SENSOR {sensor_id} | NORMAL"

        # Occasionally publish a duplicate of the last message
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

        # Generate new message
        message_id += 1
        sent_count += 1
        timestamp = datetime.now(UTC).isoformat()

        # Occasionally inject an outlier reading
        if random.random() < OUTLIER_PROBABILITY:
            temperature = random.choice([faker.random_int(61, 80), faker.random_int(-30, -1)])
            soil_moisture = random.choice([faker.random_int(-20, -1), faker.random_int(101, 150)])
            tag = f"SENSOR {sensor_id} | OUTLIER"
        else:
            temperature = faker.random_int(10, 35)
            soil_moisture = faker.random_int(30, 90)

        light_intensity = faker.random_int(100, 1000)
        humidity = faker.random_int(40, 90)

        # Construct message payload
        payload = json.dumps({
            "sensor_id": sensor_id,
            "message_id": message_id,
            "sent_time": timestamp,
            "temperature": temperature,
            "soil_moisture": soil_moisture,
            "light_intensity": light_intensity,
            "humidity": humidity
        })

        # Publish to MQTT broker
        mqttc.publish(MQTT_PUBLISH_TOPIC, payload)
        print(
            f"{tag} | 📤 ID {message_id} | Temp: {temperature}°C | "
            f"Moisture: {soil_moisture}% | Light: {light_intensity} lux | Humidity: {humidity}%"
        )

        last_payload = payload              # Save message for potential duplication
        time.sleep(PUBLISH_INTERVAL)        # Wait before next message

# === Graceful Shutdown Handler ===
def handle_interrupt(sig, frame):
    print("\n🛑 Stopping all sensor threads...")
    stop_event.set()                       # Signal threads to stop

# Register signal handler for Ctrl+C (SIGINT)
signal.signal(signal.SIGINT, handle_interrupt)

# === Start Sensor Threads ===
threads = []
for sensor_id in range(1, NUM_SENSORS + 1):
    t = threading.Thread(target=simulate_sensor, args=(sensor_id,))
    t.start()
    threads.append(t)

# === Wait for All Threads to Finish ===
for t in threads:
    t.join()

print("✅ Publisher exited cleanly.")

