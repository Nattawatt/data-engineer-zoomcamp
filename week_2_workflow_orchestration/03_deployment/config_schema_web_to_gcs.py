tables = {
    'green' : {
        'schema' : {
                    "VendorID" : "Int64",
                    "store_and_fwd_flag" : str,
                    "tpep_pickup_datetime" : 'datetime64[ns]',
                    "tpep_dropoff_datetime" : 'datetime64[ns]',
                    "RatecodeID" : 'Int64',
                    "PULocationID" : 'Int64',
                    "DOLocationID" : 'Int64',
                    "passenger_count" : "Int64",
                    "trip_distance" : float,
                    "fare_amount" : float,
                    "extra" : float,
                    "mta_tax" : float,
                    "tip_amount" : float,
                    "tolls_amount" : float,
                    "ehail_fee" : float,
                    "improvement_surcharge" : float,
                    "total_amount" : float,
                    "payment_type" : "Int64",
                    "trip_type" : "Int64",
                    "congestion_surcharge" : float
                    }
                },
    'yellow' : {
        "schema" : {
                    "VendorID" : "Int64",
                    "tpep_pickup_datetime" : 'datetime64[ns]',
                    "tpep_dropoff_datetime" : 'datetime64[ns]',
                    "passenger_count" : "Int64",
                    "trip_distance" : float,
                    "RatecodeID" : 'Int64',
                    "store_and_fwd_flag" : str,
                    "PULocationID" : 'Int64',
                    "DOLocationID" : 'Int64',
                    "payment_type" : "Int64",
                    "fare_amount" : float,
                    "extra" : float,
                    "mta_tax" : float,
                    "tip_amount" : float,
                    "tolls_amount" : float,
                    "improvement_surcharge" : float,
                    "tolls_amount" : float,
                    "congestion_surcharge" : float
                    }
                },
    'fhv' :     {
        "schema" : {
                    "dispatching_base_num" : str,
                    "pickup_datetime" : 'datetime64[ns]' ,
                    "dropOff_datetime" : 'datetime64[ns]',
                    "PULocationID" : 'Int64',
                    "DOLocationID" : 'Int64',
                    "SR_Flag" : 'Int64',
                    "Affiliated_base_number" : str
                    }
                }
}