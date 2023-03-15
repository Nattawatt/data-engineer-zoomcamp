import pyspark.sql.types as T
INPUT_DATA_PATH = '../resources/green_tripdata_2019-01.csv'

BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'rides_json'

TOPIC_WINDOWED_PU_LOCATION_ID_COUNT = 'pu_location_counts_windowed'

RIDE_SCHEMA = T.StructType(
    [# T.StructField("vendor_id", T.IntegerType()),
    #  T.StructField('tpep_pickup_datetime', T.TimestampType()),
     # T.StructField('tpep_dropoff_datetime', T.TimestampType()),
     T.StructField('pu_location_id', T.IntegerType()),
     # T.StructField("passenger_count", T.IntegerType()),
     # T.StructField("trip_distance", T.FloatType()),
     # T.StructField("payment_type", T.IntegerType()),
     # T.StructField("total_amount", T.FloatType()),
     ])