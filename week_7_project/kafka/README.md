# RUN DOCKER-COMPOSE

```
poetry install
```

At `infra/`

```
docker-compose up -d
```

**check docker container up all**
```bash
docker container ls
```
![Alt text](../images/container-check.JPG)

# CREATE TOPICS
```
topics = express_sensor
partition = 6
```
visit http://localhost:9021/

![Alt text](../images/create-kafka-topics-ui.JPG)

```json
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Car Report",
    "description": "Report of a car on a road",
    "type": "object",
    "properties": {
        "timestamp": {
            "description": "Time of the report in Unix timestamp format",
            "type": "integer",
            "format": "int64"
        },
        "uid_car": {
            "description": "Unique identifier for the car",
            "type": "string"
        },
        "road_name": {
            "description": "Name of the road the car is on",
            "type": "string"
        },
        "speed": {
            "description": "Speed of the car in km/h",
            "type": "integer",
            "format": "int64"
        }
    }
}
```
# CREATE STREAMING
```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

```sql
CREATE STREAM express_sensor_stream(
  timestamp TIMESTAMP,
  uid_car VARCHAR,
  road_name VARCHAR,
  speed BIGINT
) WITH (
  KAFKA_TOPIC='express_sensor',
  VALUE_FORMAT='JSON_SR',
  TIMESTAMP='timestamp'
);
```

```sql
CREATE TABLE express_report_5_mins
AS
SELECT 
 road_name, 
 AVG(speed) AS AVG_SPEED_5_MIN
FROM 
express_sensor_stream WINDOW TUMBLING (SIZE 5 MINUTE) 
GROUP BY road_name;
```

```sql
SELECT
    road_name,
    WINDOWSTART,
    WINDOWEND,
    FROM_UNIXTIME(WINDOWSTART+25200000) AS window_start_gmt_plus_7,
    FROM_UNIXTIME(WINDOWEND+25200000) AS window_end_gmt_plus_7,
    AVG_SPEED_5_MIN,
    CASE
      WHEN AVG_SPEED_5_MIN > 100 THEN 'flow'
      WHEN AVG_SPEED_5_MIN > 60 THEN 'normal'
      ELSE 'jam' END AS TRAFFIC_STATUS
FROM express_report_5_mins EMIT CHANGES;
```

# RUN PRODUCER
```bash
poetry shell
```

at `./kafka/src`
```bash
python producer.py
```

# RUN CONSUMER TO GCP
you need to create bucket name in gcp first using terraform or manual

requirement

1. [terraform](https://github.com/Nattawatt/data-engineer-zoomcamp/tree/main/week_7_project/terraform)

2. [gcloud cli](https://cloud.google.com/sdk/docs/install)

at `./kafka/src`
```bash
gcloud init
```

```bash
poetry shell
```

```bash
python consumer.py
```

# USEFUL COMAND
```bash
docker-compose down -v
```

# REF

ksql-getstart - ( https://ksqldb.io/quickstart.html )

ksql-python - ( https://github.com/bryanyang0528/ksql-python )
