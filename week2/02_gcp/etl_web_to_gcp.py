from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url : str) -> pd.DataFrame:
    """"Read taxi data from web into pandas DataFrame"""
    
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints= True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
    """Clean data type of datetime"""
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
    print(path)
    df.to_parquet(path, compression= "gzip")
    return path

@task(log_prints= True)
def write_gcs(path : Path) -> None:
    """Upload local parquet file to Google Cloud Storage"""
    gcp_block = GcsBucket.load("yellow-data-5425")
    gcp_block.upload_from_path(
        from_path = path,
        to_path= path
    )

@flow()
def etl_web_to_gcs() -> None:
    """main function ETL from web to google cloud storage"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()