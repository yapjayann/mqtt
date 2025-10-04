
# Investigation on IoT and Cloud Technology on Smart Agriculture

The following are the project files for part of my capstone project. This is a sample centralized client-server architecture solution that can be deployed for smart agriculture use cases. This architecture can be run with your local machine as the client and any cloud compute instance (AWS EC2 or others) as the server.




## How It Works
This architecture follows a **centralized client-server design**, where the client (publisher) sends simulated IoT sensor data to a single server (subscriber) for processing and storage.

On the client side, `publisher.py` generates JSON-encoded sensor data (temperature, humidity, soil moisture, light intensity) using the Faker and paho-mqtt libraries. Each message includes a unique ID and timestamp, and occasional outliers to simulate real-world noise. The data is sent to the cloud using the MQTT protocol, chosen for its lightweight and low-latency communication.

In the cloud, an **MQTT broker** (Mosquitto) runs inside a Docker container on an AWS EC2 (or any cloud compute service) instance, which provides burstable performance for variable IoT workloads. The subscriber (`subscriber.py`) subscribes to MQTT topics, processes incoming data by filtering outliers, detecting message loss, calculating latency, and inserting data asynchronously into the database.

The processed data is stored in **InfluxDB**, a time-series database optimized for timestamped sensor data. Grafana is optionally included for real-time visualization and performance monitoring through dashboards connected to InfluxDB.

All components — Mosquitto, subscriber, InfluxDB, and Grafana — are containerized with Docker Compose, ensuring portability, consistency, and easier deployment between local and cloud environments.

Overall, this setup represents a typical event-driven IoT data processing pipeline, suitable for demonstrating data flow, reliability, and monitoring. However, its centralized nature means the single server could become a bottleneck as device count or data volume grows, which is a limitation explored in this research.

In summary, 
1) The sensor simulator (publisher.py) generates and publishes sensor data (with occasional outliers) over MQTT.
2) The Mosquitto MQTT broker receives and forwards the message.
3) The subscriber (subscriber.py) receives the message, processes it (e.g., filters outliers, calculates latency), and inserts it into InfluxDB.
4) Grafana queries InfluxDB to visualize the data in real-time dashboards.

## What you need
- A Linux environment in your local machine
- An AWS account
- Docker installed on your local machine
- Python3, paho-mqtt and Faker libraries installed on your local machine




## How to deploy and run this project
1) Clone the folder contents into your local machine and EC2 server.
2) Copy the IP address of your EC2 server and paste it in `publisher.py` file. You may edit the script to change the frequency of data sent. 
3) In the EC2 server, ensure Docker Compose is installed and use the following command to setup the Mosquitto MQTT broker and InfluxDB:
```
docker-compose up
```
4) To see the results separately from the logs, open a separate command line of the EC2 server to start the subscriber separately:
```
docker-compose --profile manual up
```
5) In the local machine, run the `publisher.py` script to send data to the server.
```
python3 publisher.py
```
