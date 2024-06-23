from datetime import datetime

LONDON_COOR = {'latitude': 0, 'longitude': 0}
BIRMINGHAM_COOR = {'latitude': 0, 'longitude': 0}

# Value for simulate vehicle movement
latitude_increment = BIRMINGHAM_COOR['latitude'] - LONDON_COOR['latitude'] / 100
longitude_increment = BIRMINGHAM_COOR['longitude'] - LONDON_COOR['longitude'] / 100

start_coor = LONDON_COOR.copy()
start_time = datetime.now()
