import csv
from json import dumps
from kafka import KafkaProducer
import argparse

parser = argparse.ArgumentParser(prog='ProgramName')
parser.add_argument('-filename','-f')
args = parser.parse_args()
# print(args.filename)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open(args.filename)
# file = open('../../resources/green_tripdata_2019-01.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    if args.filename == "../../resources/fhv_tripdata_2019-01.csv":
        if row[3] == '':
            key = {"pu_location_id": int(999)}
            value = {"pu_location_id": int(999)}
        else :
            key = {"pu_location_id": int(float(row[3]))}
            value = {"pu_location_id": int(float(row[3]))}
    else :
        key = {"pu_location_id": int(row[0])}
        value = {"pu_location_id": int(row[0])}
    producer.send('rides_json_test1', value=value, key=key)
    print("producing")
    # sleep(1)