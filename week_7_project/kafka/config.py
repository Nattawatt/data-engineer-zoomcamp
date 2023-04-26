topic = 'express_sensor_unix'

config = {
     'bootstrap.servers': 'localhost:9092',     
}

sr_config = {
    'url': 'http://localhost:8081',
}

express_schema = """
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
"""