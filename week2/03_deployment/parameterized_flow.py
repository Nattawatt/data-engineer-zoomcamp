from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

SCHEMA = {
    "VendorID" : str,
    "store_and_fwd_flag" : str,
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

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True, retries=3)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == "green":
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df = df.astype(SCHEMA)
        df.rename(columns={"lpep_pickup_datetime": "tpep_pickup_datetime", "lpep_dropoff_datetime": "tpep_dropoff_datetime"}, inplace= True)
    elif color == "fhv":
        df.dispatching_base_num = df.dispatching_base_num.astype(str)
        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
        df.PUlocationID = df.PUlocationID.astype(float)
        df.DOlocationID = df.DOlocationID.astype(float)
        df.SR_Flag = df.SR_Flag.astype('Int64')
        df.Affiliated_base_number = df.Affiliated_base_number.astype(str)
    elif color == "yellow":
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet").as_posix()
    data_dir = Path(f"data/{color}")
    data_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("yellow-data-5425")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(months: list[int], year: int, color: str):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)