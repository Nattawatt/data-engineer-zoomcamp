import faust
from settings import TaxiRide


app = faust.App('pu_location_count_test', broker='kafka://localhost:9092')
topic = app.topic('rides_json_test1', value_type=TaxiRide)

vendor_rides = app.Table('pu_location_count_test', default=int)


@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.pu_location_id):
        vendor_rides[event.pu_location_id] += 1
        print(vendor_rides.items())

if __name__ == '__main__':
    app.main()