"""
MQTT subscriber - Listen to a topic and sends data to InfluxDB
"""

import os
import json
import sys
import signal
from dotenv import load_dotenv
from time import perf_counter  # For performance measurement
from datetime import datetime, UTC  # For handling timestamps
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Load environment variables from .env file
load_dotenv()

# InfluxDB config
INFLUXDB_URL    = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Set up InfluxDB client
influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)

write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# MQTT broker config
MQTT_BROKER_URL    = "localhost"
MQTT_PUBLISH_TOPIC = "yapjayann/temperature"

mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL)

# Track latencies
write_latencies = []
total_latencies = []

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    print("Subscribing to topic:", MQTT_PUBLISH_TOPIC)
    client.subscribe(MQTT_PUBLISH_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"Received on topic {msg.topic}: {payload}")

        data = json.loads(payload)
        temperature = data["temperature"]
        sent_time = datetime.fromisoformat(data["sent_time"]).astimezone(UTC)

        # Start timer before writing
        write_start = perf_counter()

        # Create point using publisher's time
        point = Point("temperature") \
            .field("value", temperature) \
            .time(sent_time)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        # Stop timer after writing
        write_end = perf_counter()

        now = datetime.now(UTC)
        total_latency_ms = (now - sent_time).total_seconds() * 1000
        write_latency_ms = (write_end - write_start) * 1000

        # Store latencies
        total_latencies.append(total_latency_ms)
        write_latencies.append(write_latency_ms)

        # Output
        print(f"✅ Wrote temperature {temperature} sent at {sent_time} to InfluxDB")
        print(f"🧾 InfluxDB Write Time: {write_latency_ms:.2f} ms")
        print(f"📈 Total Architecture Latency: {total_latency_ms:.2f} ms")

    except Exception as e:
        print(f"❌ Error: {e}")

# Graceful exit to calculate averages
def handle_exit(sig, frame):
    print("\n\n📉 Final Latency Report:")
    if total_latencies and write_latencies:
        avg_total = sum(total_latencies) / len(total_latencies)
        avg_write = sum(write_latencies) / len(write_latencies)
        print(f"📊 Average Total Architecture Latency: {avg_total:.2f} ms")
        print(f"📊 Average InfluxDB Write Time: {avg_write:.2f} ms")
        print(f"📦 Messages processed: {len(total_latencies)}")
    else:
        print("⚠️ No data received.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)

# Start MQTT loop
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.loop_forever()
