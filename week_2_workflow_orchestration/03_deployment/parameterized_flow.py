from pathlib import Path
import numpy as np
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from config_schema_web_to_gcs import tables

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
        df.VendorID.fillna(np.nan,inplace = True)
        df.store_and_fwd_flag.fillna("N",inplace= True)
        df = df.astype(tables[color]['schema'])
        df.rename(columns={"lpep_pickup_datetime": "tpep_pickup_datetime", "lpep_dropoff_datetime": "tpep_dropoff_datetime"}, inplace= True)
    elif color == "fhv":
        df = df.astype(tables[color]['schema'])
    elif color == "yellow":
        df.VendorID.fillna(np.nan,inplace = True)
        df.store_and_fwd_flag.fillna("N",inplace= True)
        df = df.astype(tables[color]['schema'])
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
    print(tables[color]['schema'])
    # etl_parent_flow(months, year, color)