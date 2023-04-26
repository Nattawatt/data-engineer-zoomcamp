import faust
import json
from datetime import timedelta

app = faust.App('speed-stream', broker='kafka://localhost:9092')

class Express(faust.Record, serializer='json'):
    timestamp: str
    uid_car: str
    road_name: str
    speed: float

topic = app.topic('express_sensor', value_type=Express)
table = app.Table('speed-table', default=float)

@app.agent(topic)
async def process_express(expresses):
    async for express in expresses:
        print(express)