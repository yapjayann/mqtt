"""
MQTT Subscriber — Listens to a topic and sends smart farm data to InfluxDB.
Now includes: total messages received (including duplicates/outliers)
"""

import os
import json
import sys
import signal
import logging
import time
from dotenv import load_dotenv
from datetime import datetime, UTC
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
import statistics
from threading import Lock

# Logging with ms precision
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Load environment variables
load_dotenv()

# InfluxDB config
INFLUXDB_URL    = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# MQTT config
MQTT_BROKER_URL    = "localhost"
MQTT_PUBLISH_TOPIC = "yapjayann/sensors"

# Tracking
total_latencies = []
last_message_ids = {}  # Track per-sensor last IDs
seen_ids = set()
filtered_count = 0
duplicate_count = 0
message_loss_count = 0
total_received = 0
counter_lock = Lock()

# Outlier thresholds
MAX_TEMP = 50
MIN_TEMP = -10
MIN_MOISTURE = 0
MAX_MOISTURE = 100

# InfluxDB batching callbacks
class BatchingCallback:
    def success(self, conf: tuple, data: str):
        logging.info("✅  Batch write successful.")

    def error(self, conf: tuple, data: str, exception: InfluxDBError):
        logging.error(f"❌  Batch write failed: {exception}")

    def retry(self, conf: tuple, data: str, exception: InfluxDBError):
        logging.warning(f"🔁  Retrying batch write: {exception}")

callback = BatchingCallback()

influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)

write_api = influx_client.write_api(
    write_options=WriteOptions(
        batch_size=5,
        flush_interval=5000,
        jitter_interval=1000,
        retry_interval=2000
    ),
    success_callback=callback.success,
    error_callback=callback.error,
    retry_callback=callback.retry
)

mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL)

def on_connect(client, userdata, flags, rc):
    logging.info(f"🔌 Connected to MQTT broker (code={rc})")
    logging.info(f"📡 Subscribing to topic: {MQTT_PUBLISH_TOPIC}")
    client.subscribe(MQTT_PUBLISH_TOPIC)

def on_message(client, userdata, msg):
    global filtered_count, duplicate_count, message_loss_count, total_received

    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        with counter_lock:
            total_received += 1

        sensor_id = data.get("sensor_id", "unknown")
        msg_id = data["message_id"]
        temperature = data["temperature"]
        soil_moisture = data["soil_moisture"]
        light_intensity = data["light_intensity"]
        humidity = data["humidity"]
        sent_time = datetime.fromisoformat(data["sent_time"]).astimezone(UTC)
        now = datetime.now(UTC)

        id_key = f"{sensor_id}-{msg_id}"

        # Duplicate check
        if id_key in seen_ids:
            duplicate_count += 1
            logging.warning(
                f"⚠️  Duplicate ID: {id_key} | "
                f"Temp: {temperature}°C | Moisture: {soil_moisture}% | "
                f"Light: {light_intensity} lux | Humidity: {humidity}%"
            )
            # Still update last_message_ids to avoid false loss detection
            last_message_ids[sensor_id] = msg_id
            return
        seen_ids.add(id_key)

        # Message loss check
        last_id = last_message_ids.get(sensor_id)
        if last_id is not None and msg_id != last_id + 1:
            lost = msg_id - last_id - 1
            if lost > 0:
                message_loss_count += lost
                logging.warning(
                    f"⚠️  Message loss (Sensor {sensor_id}): Missed {lost} "
                    f"(last ID: {last_id}, current: {msg_id})"
                )

        # Update last message ID for this sensor
        last_message_ids[sensor_id] = msg_id

        # Outlier check
        if (
            temperature > MAX_TEMP or temperature < MIN_TEMP or
            soil_moisture < MIN_MOISTURE or soil_moisture > MAX_MOISTURE
        ):
            filtered_count += 1
            logging.warning(
                f"🚫 Outlier (Sensor {sensor_id} | ID {msg_id}): "
                f" | Temp={temperature}°C  | Moisture={soil_moisture}% — Dropped."
            )
            return


        # Latency
        latency_ms = (now - sent_time).total_seconds() * 1000
        total_latencies.append(latency_ms)

        # Write to InfluxDB
        point = Point("smart_farm") \
            .field("temperature", temperature) \
            .field("soil_moisture", soil_moisture) \
            .field("light_intensity", light_intensity) \
            .field("humidity", humidity) \
            .field("message_id", msg_id) \
            .field("sensor_id", int(sensor_id)) \
            .time(sent_time)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        logging.info(
            f"📥  Sensor {sensor_id} | ID {msg_id} | Temp: {temperature}°C | "
            f"Moisture: {soil_moisture}% | Light: {light_intensity} lux | "
            f"Humidity: {humidity}% | Latency: {latency_ms:.2f} ms"
        )

    except Exception as e:
        logging.error(f"❌  Message processing error: {e}")

def handle_exit(sig, frame):
    print()
    logging.info("📉  Final Report")

    if total_latencies:
        avg = sum(total_latencies) / len(total_latencies)
        if len(total_latencies) > 1:
            std_dev = statistics.stdev(total_latencies)
            logging.info(f"📊  Latency Std Dev: {std_dev:.2f} ms")
        else:
            logging.info("📊  Latency Std Dev: N/A (only 1 data point)")

        logging.info(f"📊  Average Ingestion Latency: {avg:.2f} ms")
        logging.info(f"📦  Total Messages Processed: {len(total_latencies)}")
        logging.info(f"📉  Total Messages Lost (order check): {message_loss_count}")
        logging.info(f"🔁  Duplicate Messages Skipped: {duplicate_count}")
        logging.info(f"🚫  Outliers Filtered: {filtered_count}")
        logging.info(f"🧾  Unique Message IDs Seen: {len(seen_ids)}")
        with counter_lock:
            logging.info(f"📬  TOTAL MESSAGES RECEIVED (raw, incl. dupes/outliers): {total_received}")
    else:
        logging.warning("⚠️  No data received.")

    # Shutdown sequence
    logging.info("🛑  Stopping MQTT loop...")
    mqttc.loop_stop()
    mqttc.disconnect()

    logging.info("🛑  Flushing writes...")
    try:
        write_api.flush()
        logging.info("⏳  Waiting for async writes...")
        time.sleep(6)
    except Exception as e:
        logging.error(f"❌  Flush failed: {e}")

    logging.info("🔒  Closing InfluxDB client...")
    influx_client.close()

    logging.info("✅  Exiting cleanly.")
    sys.exit(0)

# Bind signal
signal.signal(signal.SIGINT, handle_exit)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.loop_forever()
