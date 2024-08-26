import os
import time
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random

LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}


# Calculate movement increments
LATITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]
) / 100
LONGITUDE_INCREMENT = (
    BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]
) / 100

# ENV variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type="private"):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0, 40),
        "direction": "North-East",
        "vehicleType": vehicle_type,
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "cameraId": camera_id,
        "location": location,
        "snapshot": "Base64EncodedImage",
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "location": location,
        "temperature": random.uniform(10, 30),
        "humidity": random.uniform(0, 100),
        "rainfall": random.uniform(0, 10),
        "windSpeed": random.uniform(0, 50),
        "airQualityIndex": random.uniform(0, 350),
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "incidentId": uuid.uuid4(),
        "timestamp": timestamp,
        "location": location,
        "type": random.choice(["Accident", "Fire", "Police", "Medical"]),
        "status": random.choice(["Active", "Resolved"]),
        "description": "Incident Description",
    }


def simulate_vehicle_movement():
    global start_location

    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location["latitude"], location["longitude"]),
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": "Toyota",
        "model": "Corolla",
        "year": 2019,
        "fuelType": "Petrol",
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(
        f"Object of type '{obj.__class__.__name__}' is not JSON serializable"
    )


def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {str(err)}")
    else:
        print(f"Message produced: {msg.key()}: {msg.value()}")


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic,
        key=str(data["id"]),
        value=json.dumps(data, default=json_serializer).encode("utf-8"),
        on_delivery=delivery_report,
    )
    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = generate_traffic_camera_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"], "camera-1"
        )
        weather_data = generate_weather_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"]
        )
        emergency_incident_data = generate_emergency_incident_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"]
        )

        if (vehicle_data["location"][0] >= BIRMINGHAM_COORDINATES["latitude"]) and (
            vehicle_data["location"][1] <= BIRMINGHAM_COORDINATES["longitude"]
        ):
            print("Vehicle has reached Birmingham!")
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"Error: {err}"),
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, "Vehicle-Test")
    except KeyboardInterrupt:
        print("Stopping simulation")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
