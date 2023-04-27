from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from config import config, sr_config, express_schema, topic
import time
import datetime
import random
import numpy as np

class Car(object):
    def __init__(self, timestamp, uid_car, road_name, speed):
        self.timestamp = timestamp
        self.uid_car = uid_car
        self.road_name = road_name
        self.speed = speed

def temp_to_dict(car,ctx):
    return {"timestamp": car.timestamp,
            "uid_car": car.uid_car,
            "road_name": car.road_name,
            "speed": car.speed}

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')

def generate_speed_reading(road_name, timestamp, prev_speed=None, prev_timestamp=None, traffic=None):
    # Get day of the week and time of day
    day_of_week = timestamp.weekday()
    hour_of_day = timestamp.hour

    # Define base speed based on day of the week and time of day
    if day_of_week < 5:
        # Weekday - fast speeds
        if 7 <= hour_of_day < 10:
            base_speed = random.randint(80, 100)
        elif 18 <= hour_of_day < 21:
            base_speed = random.randint(80, 100)
        else:
            base_speed = random.randint(90, 120)
    else:
        # Saturday - slower speeds
        if 10 <= hour_of_day < 12:
            base_speed = random.randint(40, 60)
        elif 14 <= hour_of_day < 17:
            base_speed = random.randint(50, 80)
        else:
            base_speed = random.randint(60, 100)

    # Adjust speed based on traffic conditions
    if traffic is not None:
        base_speed *= (1 - 0.2 * traffic)

    # Adjust speed based on car model
    car_model = random.choice(["sedan", "SUV", "sports car"])
    if car_model == "sedan":
        base_speed *= 1
    elif car_model == "SUV":
        base_speed *= 0.9
    elif car_model == "sports car":
        base_speed *= 1.2

    # Add random variation to speed
    speed = base_speed + np.random.normal(loc=0, scale=5)

    # Ensure speed is within range
    speed = max(speed, 40)
    speed = min(speed, 120)

    # Adjust speed if previous speed and timestamp are provided
    if prev_speed is not None and prev_timestamp is not None:
        time_diff = timestamp - prev_timestamp
        time_diff_hours = time_diff.total_seconds() / 3600.0

        # Calculate maximum speed change allowed for time difference
        max_speed_change = time_diff_hours * 10 / 0.5

        # Limit speed change to maximum allowed
        if speed - prev_speed > max_speed_change:
            speed = prev_speed + max_speed_change
        elif prev_speed - speed > max_speed_change:
            speed = prev_speed - max_speed_change

    # Simulate accident with a probability of 0.01
    if random.random() < 0.01:
        speed *= random.uniform(0.2, 0.5)  # Decrease speed by random factor between 0.2 and 0.5

    # Generate random car ID
    uid_car = ''.join(random.choices('0123456789ABCDEF', k=6))

    # Adjust speed based on time and day of the week to meet requirements
    if (day_of_week < 5 and ((7 <= hour_of_day < 10) or (18 <= hour_of_day < 21))) or \
        (day_of_week == 5 and 10 <= hour_of_day < 17):
        speed = random.randint(0, 40)
    elif day_of_week == 5 and 7 <= hour_of_day < 10:
        speed = random.randint(80, 120)
    elif day_of_week == 6 and random.random() < 0.2:
        speed = random.randint(0, 40)
    else:
        speed = random.randint(80, 120)

    timestamp_milliseconds = int(timestamp.timestamp()*1000)

    return (timestamp_milliseconds, uid_car, road_name, round(speed))


if __name__ == "__main__":
    # KAFKA CONFIG
    producer = Producer(config)

    schema_registry_client = SchemaRegistryClient(sr_config)

    json_serializer = JSONSerializer(express_schema,
                                     schema_registry_client,
                                     temp_to_dict)
    
    # genarate data
    road_name = 'Si Rat Expressway'
    delta = datetime.timedelta(seconds=60)
    start_date = datetime.datetime(2021, 1, 1)
    end_date = datetime.datetime(2022, 1, 1)

    # Generate list of datetime objects between start and end dates
    current_date = start_date
    dates_list = []
    while current_date < end_date:
        dates_list.append(current_date)
        current_date += delta

    # Filter list to include only hours between 3AM and 11PM
    filtered_dates = [d for d in dates_list if d.hour >= 3 and d.hour <= 23]

    prev_speed = None
    prev_timestamp = None
    for current_date in filtered_dates:
        # Generate car data for current second
        traffic = random.uniform(0, 1)
        weather = random.choice([None, "rainy"])
        
        speed_reading = generate_speed_reading(road_name, current_date, prev_speed, prev_timestamp, traffic)

        # Print speed reading
        print(speed_reading)

        # Update previous speed and timestamp
        prev_speed = speed_reading[3]
        prev_timestamp = current_date

        # Increment current date
        current_date += delta

        # Create a new Police object with the above information and add it to the police_objects list
        car_obj = Car(speed_reading[0], 
                         speed_reading[1], 
                         speed_reading[2],      
                         speed_reading[3]
                        )
        
        producer.produce(topic=topic, key=str(car_obj.road_name),
                        value=json_serializer(car_obj, 
                        SerializationContext(topic, MessageField.VALUE)),
                        on_delivery=delivery_report)

        producer.flush()

        time.sleep(1)