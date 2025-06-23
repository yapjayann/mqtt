"""
MQTT Subscriber — Listens to a topic and sends smart farm data to InfluxDB.
Now includes: total messages received (including duplicates/outliers)
Auto-shutdowns after 30s of inactivity.
"""

# === Imports ===
import os
import json
import sys
import signal
import logging
import time
from dotenv import load_dotenv                       # Load environment variables from .env
from datetime import datetime, UTC                  # For timestamp handling
import paho.mqtt.client as mqtt                     # MQTT client
from influxdb_client import InfluxDBClient, Point   # InfluxDB client and data format
from influxdb_client.client.write_api import WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
import statistics
from threading import Lock, Timer                   # For thread safety and inactivity timeout

# === Auto-shutdown Configuration ===
INACTIVITY_TIMEOUT = 30  # Seconds before auto-exit if no messages received
shutdown_timer = None    # Holds the Timer object so we can reset it

# === Logger Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# === Load .env Configs ===
load_dotenv()

# === InfluxDB Configuration ===
INFLUXDB_URL    = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# === MQTT Configuration ===
MQTT_BROKER_URL    = "mqtt-broker"  
MQTT_PUBLISH_TOPIC = "yapjayann/sensors"

# === Metrics and Counters ===
total_latencies = []             # Store latencies to compute average & std dev
last_message_ids = {}            # To detect missing messages per sensor
seen_ids = set()                 # For duplicate detection
filtered_count = 0
duplicate_count = 0
message_loss_count = 0
total_received = 0
negative_latency_count = 0         # Count messages with negative latency
counter_lock = Lock()            # Ensure safe increment from multiple threads

# === Outlier Thresholds ===
MAX_TEMP = 60
MIN_TEMP = 0
MIN_MOISTURE = 0
MAX_MOISTURE = 100

# === InfluxDB Write Callbacks ===
class BatchingCallback:
    def success(self, conf: tuple, data: str):
        logging.info("✅  Batch write successful.")

    def error(self, conf: tuple, data: str, exception: InfluxDBError):
        logging.error(f"❌  Batch write failed: {exception}")

    def retry(self, conf: tuple, data: str, exception: InfluxDBError):
        logging.warning(f"🔁  Retrying batch write: {exception}")

# === Initialize InfluxDB Client ===
callback = BatchingCallback()
influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = influx_client.write_api(
    write_options=WriteOptions(
        batch_size=5,            # Buffer size before writing
        flush_interval=5000,     # Flush every 5s if buffer not full
        jitter_interval=1000,    # Add jitter to reduce load spikes
        retry_interval=2000      # Retry delay if failure occurs
    ),
    success_callback=callback.success,
    error_callback=callback.error,
    retry_callback=callback.retry
)

# === Initialize MQTT Client ===
mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL,1883)

# === Reset the inactivity timer (called every message) ===
def reset_timer():
    global shutdown_timer
    if shutdown_timer:
        shutdown_timer.cancel()
    shutdown_timer = Timer(INACTIVITY_TIMEOUT, handle_exit, [None, None])
    shutdown_timer.start()

# === MQTT: On Connect ===
def on_connect(client, userdata, flags, rc):
    logging.info(f"🔌 Connected to MQTT broker (code={rc})")
    logging.info(f"📡 Subscribing to topic: {MQTT_PUBLISH_TOPIC}")
    client.subscribe(MQTT_PUBLISH_TOPIC)

# === MQTT: On Message Received ===
def on_message(client, userdata, msg):
    global filtered_count, duplicate_count, message_loss_count, total_received

    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        reset_timer()  # Restart inactivity timer since we got data

        with counter_lock:
            total_received += 1

        # Parse fields
        sensor_id = data.get("sensor_id", "unknown")
        msg_id = data["message_id"]
        temperature = data["temperature"]
        soil_moisture = data["soil_moisture"]
        light_intensity = data["light_intensity"]
        humidity = data["humidity"]
        sent_time = datetime.fromisoformat(data["sent_time"]).astimezone(UTC)
        now = datetime.now(UTC)

        id_key = f"{sensor_id}-{msg_id}"

        # === Duplicate Check ===
        if id_key in seen_ids:
            duplicate_count += 1
            logging.warning(
                f"⚠️  Duplicate ID: {id_key} | "
                f"Temp: {temperature}°C | Moisture: {soil_moisture}% | "
                f"Light: {light_intensity} lux | Humidity: {humidity}%"
            )
            last_message_ids[sensor_id] = msg_id  # Still update last ID
            return
        seen_ids.add(id_key)

        # === Message Loss Check ===
        last_id = last_message_ids.get(sensor_id)
        if last_id is not None and msg_id != last_id + 1:
            lost = msg_id - last_id - 1
            if lost > 0:
                message_loss_count += lost
                logging.warning(
                    f"⚠️  Message loss (Sensor {sensor_id}): Missed {lost} "
                    f"(last ID: {last_id}, current: {msg_id})"
                )
        last_message_ids[sensor_id] = msg_id

        # === Outlier Check ===
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

        # === Latency Calculation (clamp negative to 0) ===
        latency_ms = (now - sent_time).total_seconds() * 1000
        if latency_ms < 0:
            latency_ms = 0
            with counter_lock:
                negative_latency_count += 1
        total_latencies.append(latency_ms)


        # === Write to InfluxDB ===
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

# === Graceful Exit Handler ===
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

        percent_negative = (negative_latency_count / len(total_latencies)) * 100

        logging.info(f"📊  Average Ingestion Latency: {avg:.2f} ms")
        logging.info(f"⏱️  Negative Latencies Clamped to 0 ms: {negative_latency_count} ({percent_negative:.2f}%)")
        logging.info(f"📦  Total Messages Processed: {len(total_latencies)}")
        logging.info(f"📉  Total Messages Lost (order check): {message_loss_count}")
        logging.info(f"🔁  Duplicate Messages Skipped: {duplicate_count}")
        logging.info(f"🚫  Outliers Filtered: {filtered_count}")
        logging.info(f"🧾  Unique Message IDs Seen: {len(seen_ids)}")
        with counter_lock:
            logging.info(f"📬  TOTAL MESSAGES RECEIVED (raw, incl. dupes/outliers): {total_received}")
    else:
        logging.warning("⚠️  No data received.")

    # === Cleanup and Exit ===
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

# === Setup MQTT Callbacks and Begin ===
signal.signal(signal.SIGINT, handle_exit)       # Handle Ctrl+C
mqttc.on_connect = on_connect
mqttc.on_message = on_message
reset_timer()                                   # Start inactivity countdown
mqttc.loop_forever()                            # Block and listen forever
