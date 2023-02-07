import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints = True, retries = 3, cache_key_fn = task_input_hash, cache_expiration = timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # create genarator for read csv
    df_iter = pd.read_csv(csv_name, iterator = True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(log_prints = True)
def transform_data(df):
    print(f"pre: missing passnger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passnger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, data):

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine = create_engine(postgres_url)

    data.head(n=0).to_sql(name = table_name, con=engine, if_exists= "replace")

    data.to_sql(name = table_name, con=engine, if_exists = "append")

@flow(name = "Subflow", log_prints = True)
def log_subflow(table_name: str):
    print(f"logging Subflow for {table_name}")

@flow(name = "Ingest Flow")
def main(table_name: str):
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == "__main__":
    main(table_name = "yellow_taxi_trips")