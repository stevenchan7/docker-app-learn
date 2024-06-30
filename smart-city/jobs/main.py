from datetime import datetime, timedelta
import os
import random
import uuid

from confluent_kafka import SerializingProducer

LONDON_COOR = {'latitude': 51.5074, 'longitude': -0.1278}
BIRMINGHAM_COOR = {'latitude': 52.4862, 'longitude': -1.8904}

# Value for simulate vehicle movement
latitude_increment = (BIRMINGHAM_COOR['latitude'] - LONDON_COOR['latitude']) / 100
longitude_increment = (BIRMINGHAM_COOR['longitude'] - LONDON_COOR['longitude']) / 100

start_coor = LONDON_COOR.copy()
start_time = datetime.now()

# Enviroment variables for cofiguration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_vehicle_movement():
    global start_coor

    # Move toward Birmingham
    start_coor['latitude'] += latitude_increment
    start_coor['longitude'] += longitude_increment

    # Add some randomness
    start_coor['latitude'] += random.uniform(-0.0005, 0.0005)
    start_coor['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_coor

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.randint(10, 40),
        'direction': 'North East',
        'brand': 'BMW',
        'model': 'sedan',
        'year': 2024,
        'fuelType': 'Hybrid',
        'vechile_number': 'DK1121AX'
    }

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'location': location,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', "Snow"]),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100), # Percentage
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', "None"]),
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def produce_data_to_kafka(producer, TOPIC, data):
    pass

def simulate_journey(producer: SerializingProducer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='cctv_001')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_data = generate_emergency_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_data)

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)


        break

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f"Kafka error: {err}")
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-StevenChan')
    except KeyboardInterrupt:
        print('Simulation stopped by the user')
    except Exception as e:
        print('Unxpected error: {e}')