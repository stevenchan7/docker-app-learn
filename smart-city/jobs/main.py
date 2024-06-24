from datetime import datetime, timedelta
import os
import random
import uuid

from confluent_kafka import SerializingProducer

LONDON_COOR = {'latitude': 0, 'longitude': 0}
BIRMINGHAM_COOR = {'latitude': 0, 'longitude': 0}

# Value for simulate vehicle movement
latitude_increment = BIRMINGHAM_COOR['latitude'] - LONDON_COOR['latitude'] / 100
longitude_increment = BIRMINGHAM_COOR['longitude'] - LONDON_COOR['longitude'] / 100

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

    start_coor['latitude'] += latitude_increment
    start_coor['longitude'] += longitude_increment

    # Add some randomness
    start_coor['latitude'] = random.uniform(-0.0005, 0.0005)
    start_coor['longitude'] = random.uniform(-0.0005, 0.0005)

    return start_coor

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        'id': uuid.uuid4,
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.randint(10, 40),
        'direction': 'North East',
        'brand': 'BMW',
        'model': 'sedan',
        'vechile_number': 'DK1121AX'
    }

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        print(vehicle_data)
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