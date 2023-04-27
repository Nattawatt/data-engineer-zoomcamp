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

# CREATE TOPICS
```
topics = express_sensor
partition = 6
```
## UI ( OPTION 1 )
visit http://localhost:9021/

![Alt text](images/create-kafka-topics-ui.JPG)

# CREATE STREAMING
```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

```sql
CREATE STREAM express_sensor(
  timestamp BIGINT,
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
express_sensor WINDOW TUMBLING (SIZE 5 MINUTE) 
GROUP BY road_name;
```

```sql
SELECT
    road_name, 
    TIMESTAMPTOSTRING(WINDOWSTART,'yyyy-MM-dd HH:mm:ss') AS window_start,
    TIMESTAMPTOSTRING(WINDOWEND,'yyyy-MM-dd HH:mm:ss') AS window_end,
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

at `./kafka`
```bash
python producer.py
```


# REF

ksql-getstart - ( https://ksqldb.io/quickstart.html )

ksql-python - ( https://github.com/bryanyang0528/ksql-python )
