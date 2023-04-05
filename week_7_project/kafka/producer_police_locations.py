from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from config import config, sr_config, schema_location_police
import json
import random
import datetime
import time

class Police(object):
    def __init__(self, timestamp, uid, first_name, last_name, age, age_in_police_job, rank, lat, long, sub_district, district, state):
        self.timestamp = timestamp
        self.uid = uid
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.age_in_police_job = age_in_police_job
        self.rank = rank
        self.lat = lat
        self.long = long
        self.sub_district = sub_district
        self.district = district
        self.state = state

def temp_to_dict(temp,ctx):
    return {"timestamp": temp.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "uid": temp.uid,
            "first_name": temp.first_name,
            "last_name": temp.last_name,
            "age": temp.age,
            "age_in_police_job": temp.age_in_police_job,
            "rank": temp.rank,
            "lat": temp.lat,
            "long": temp.long,
            "sub_district": temp.sub_district,
            "district": temp.district,
            "state": temp.state}

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')

if __name__ == "__main__":
    # FAKER CONFIG
    with open('data/locations.json', 'r') as location_json:
        LOCATIONS = json.load(location_json)

    with open('data/polices.json', 'r') as police_json:
        POLICES = json.load(police_json)

    # Define datetime bounds for 2021 and 2022
    start_date = datetime.datetime(2021, 1, 1)
    end_date = datetime.datetime(2021, 12, 31)

    # Generate datetimes between the bounds with a time delta of 60 minutes
    DATETIMES = []
    current_datetime = start_date
    while current_datetime <= end_date:
        DATETIMES.append(current_datetime)
        current_datetime += datetime.timedelta(minutes=30)
    
    # KAFKA CONFIG
    topic = 'police_locations'

    producer = Producer(config)

    schema_registry_client = SchemaRegistryClient(sr_config)

    json_serializer = JSONSerializer(schema_location_police,
                                     schema_registry_client,
                                     temp_to_dict)
    for timestamp in DATETIMES:
        for p in POLICES:
            uid = p['uid']
            first_name = p['first_name']
            last_name = p['last_name']
            age = p['age']
            age_in_police_job = p['age_in_police_job']
            rank = p['rank']
            
            # Assign random location information based on the LOCATIONS list
            location = LOCATIONS[random.randint(0, len(LOCATIONS)-1)]
            lat = location['lat']
            long = location['long']
            sub_district = location['sub_district']
            district = location['district']
            state = location['state']
            
            # Create a new Police object with the above information and add it to the police_objects list
            police_obj = Police(timestamp, uid, first_name, last_name, age, age_in_police_job, rank, lat, long, sub_district, district, state)
            
            producer.produce(topic=topic, key=str(police_obj.uid),
                         value=json_serializer(police_obj, 
                         SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

            producer.flush()
        
        time.sleep(1)