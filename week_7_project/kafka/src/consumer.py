from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from config import config, sr_config, topic, bucket

import datetime
import requests
import json
from google.cloud import storage

def set_consumer_configs():
    config['group.id'] = 'express_gcs'
    config['auto.offset.reset'] = 'earliest'

class Car(object):
    def __init__(self, timestamp, uid_car, road_name, speed):
        self.timestamp = timestamp
        self.uid_car = uid_car
        self.road_name = road_name
        self.speed = speed

def get_schema(schema_id):
    if schema_id not in schema_id_cache:
        response = requests.get(f'{schema_registry_url}/schemas/ids/{schema_id}')
        schema = json.loads(response.content)['schema']
        schema_id_cache[schema_id] = schema
    return schema_id_cache[schema_id]

def dict_to_temp(dict, ctx):
    return Car(dict['timestamp'], dict['uid_car'], dict['road_name'], dict['speed'])

def get_latest_schema_id_for_topic(topic):
    response = requests.get(f'{schema_registry_url}/subjects/{topic}-value/versions/latest')
    response.raise_for_status()
    return response.json()['id']

if __name__ == '__main__':
    # Create a client to access GCS
    client = storage.Client()

    # Get the bucket where you want to write the data
    bucket = client.get_bucket(bucket)

    # Set up the Schema Registry client
    schema_registry_url = sr_config['url']
    schema_id_cache = {}

    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe([topic])

    while True:
        try:
            event = consumer.poll(5.0)
            if event is None:
                continue
            schema_id = get_latest_schema_id_for_topic(topic)
            schema_str = get_schema(schema_id)
            json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_temp)
            temp = json_deserializer(event.value(), 
                SerializationContext(topic, MessageField.VALUE))
            if event is not None:
                timestamp = temp.timestamp/1000
                dt_object = datetime.datetime.fromtimestamp(timestamp)

                # Create a file name for the event data
                year = dt_object.strftime('%Y')
                month = dt_object.strftime('%m')
                day = dt_object.strftime('%d')
                file_name = f"{temp.timestamp}_{temp.road_name.replace(' ', '_')}.json"
                file_path = f"yyyy={year}/mm={month}/dd={day}/{file_name}"

                # Create data in json format
                data = {"datetime": temp.timestamp, "uid_car" : temp.uid_car, "road_name" : temp.road_name, "speed": temp.speed}
                print(data)

                # Write the event data to a JSON file
                blob = bucket.blob(file_path)
                blob.upload_from_string(json.dumps(data), content_type='application/json')
                print(f"Event data written to GCS file {file_path}")
                
        except KeyboardInterrupt:
            break

    consumer.close()