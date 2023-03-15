# HOW TO RUN

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets fhv_tripdata_2019-01.csv.gz
and green_tripdata_2019-01.csv.gz

## First start Kafka cluster

At `../../docker/kafka`
```
docker-compose up
```

```
pip install -r requirements.txt
```
## Download Data

[fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)

[green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

Save it to `../../resources`

## Producer
green

```
python producer.py -f "../../resources/green_tripdata_2019-01.csv"
```

fhv
```
python producer.py -f "../../resources/fhv_tripdata_2019-01.csv"
```

## Consumer
```
faust -A consumer worker -l info
```
