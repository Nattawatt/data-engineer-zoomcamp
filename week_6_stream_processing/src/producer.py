import csv
import json
from typing import List, Dict
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # pu_location_id
                records.append(str({row[0]}))
                ride_keys.append(str(row[0]))
                i += 1
                if i == 5:
                    break
        return zip(ride_keys, records)

    # def publish_rides(self, topic: str, messages: List[Ride]):
    #     for ride in messages:
    #         try:
    #             record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
    #             print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
    #         except KafkaTimeoutError as e:
    #             print(e.__str__())
    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(10)

if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        # 'key_serializer': lambda key: str(key).encode(),
        # 'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish(topic=KAFKA_TOPIC, records=rides)
