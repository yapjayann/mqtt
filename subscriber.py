# === Imports ===
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
from threading import Lock, Timer

# === Auto-shutdown ===
INACTIVITY_TIMEOUT = 30
shutdown_timer = None

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# === Load .env ===
load_dotenv()

# === InfluxDB Config ===
INFLUXDB_URL    = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# === MQTT Config ===
MQTT_BROKER_URL    = "mqtt-broker"
MQTT_PUBLISH_TOPIC = "yapjayann/sensors"

# === Shared Metrics Dictionary ===
metrics = {
    "total_latencies": [],
    "last_message_ids": {},
    "filtered_count": 0,
    "message_loss_count": 0,
    "total_received": 0,
    "negative_latency_count": 0
}
counter_lock = Lock()

# === Outlier Thresholds ===
MAX_TEMP = 60
MIN_TEMP = 0
MIN_MOISTURE = 0
MAX_MOISTURE = 100

# === InfluxDB Callbacks ===
class BatchingCallback:
    def success(self, conf, data):
        logging.info("✅  Batch write successful.")

    def error(self, conf, data, exception):
        logging.error(f"❌  Batch write failed: {exception}")

    def retry(self, conf, data, exception):
        logging.warning(f"🔁  Retrying batch write: {exception}")

# === InfluxDB Client ===
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

# === MQTT Client ===
mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL, 1883)

# === Reset inactivity timer ===
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

# === MQTT: On Message ===
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        reset_timer()

        with counter_lock:
            metrics["total_received"] += 1

        # Parse fields
        sensor_id = data.get("sensor_id", "unknown")
        msg_id = data["message_id"]
        temperature = data["temperature"]
        soil_moisture = data["soil_moisture"]
        light_intensity = data["light_intensity"]
        humidity = data["humidity"]
        sent_time = datetime.fromisoformat(data["sent_time"]).astimezone(UTC)
        now = datetime.now(UTC)

        # === Message Loss Check ===
        last_id = metrics["last_message_ids"].get(sensor_id)
        if last_id is not None and msg_id != last_id + 1:
            lost = msg_id - last_id - 1
            if lost > 0:
                metrics["message_loss_count"] += lost
                logging.warning(
                    f"⚠️  Message loss (Sensor {sensor_id}): Missed {lost} "
                    f"(last ID: {last_id}, current: {msg_id})"
                )
        metrics["last_message_ids"][sensor_id] = msg_id

        # === Outlier Check ===
        if (
            temperature > MAX_TEMP or temperature < MIN_TEMP or
            soil_moisture < MIN_MOISTURE or soil_moisture > MAX_MOISTURE
        ):
            metrics["filtered_count"] += 1
            logging.warning(
                f"🚫 Outlier (Sensor {sensor_id} | ID {msg_id}): "
                f" | Temp={temperature}°C  | Moisture={soil_moisture}% — Dropped."
            )
            return

        # === Latency Calculation (allow negative) ===
        latency_ms = (now - sent_time).total_seconds() * 1000
        with counter_lock:
            if latency_ms < 0:
                metrics["negative_latency_count"] += 1
            metrics["total_latencies"].append(latency_ms)

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

    lats = metrics["total_latencies"]
    if lats:
        avg = sum(lats) / len(lats)
        std_dev = statistics.stdev(lats) if len(lats) > 1 else 0
        percent_negative = (metrics["negative_latency_count"] / len(lats)) * 100

        logging.info(f"📊  Average Ingestion Latency: {avg:.2f} ms")
        logging.info(f"📊  Latency Std Dev: {std_dev:.2f} ms")
        logging.info(f"⏱️  Negative Latencies: {metrics['negative_latency_count']} ({percent_negative:.2f}%)")
        logging.info(f"📦  Total Messages Processed: {len(lats)}")
        logging.info(f"📉  Total Messages Lost (order check): {metrics['message_loss_count']}")
        logging.info(f"🚫  Outliers Filtered: {metrics['filtered_count']}")
        logging.info(f"📬  TOTAL MESSAGES RECEIVED: {metrics['total_received']}")
    else:
        logging.warning("⚠️  No data received.")

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

# === Start ===
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
reset_timer()
mqttc.loop_forever()
