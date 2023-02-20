from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month:int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("yellow-data-5425")
    gcs_block.get_directory(from_path = gcs_path, local_path = ".")
    return Path(gcs_path).as_posix()

@task(log_prints= True)
def transform(path: Path)-> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace= True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-credentials-for-gcs")
    df.to_gbq(
        destination_table=f"dezoomcamp.{color}_rides",
        project_id="de-zoomcamp-376514",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize= 500_000,
        if_exists= "append",
    )


@flow()
def etl_gcs_to_bq(year : int, month : int, color : str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df,color)

@flow()
def etl_parent_gcs_to_bq_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    """Parent flow for parameterized flow"""
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == "__main__":
    months = [1, 2]
    year = 2021
    color = "yellow"
    etl_parent_gcs_to_bq_flow(months,year,color)