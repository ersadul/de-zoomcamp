from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_block = GcsBucket.load("gcs-zoomcamp")
    gcp_block.get_directory(from_path=gcs_path, local_path=f"./")
    path = Path(f"./{gcs_path}")

    df = pd.read_parquet(path)

    return df


# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passanger count: {df['passenger_count'].isna().sum()}")
#     df['passenger_count'].fillna(0, inplace=True)
#     print(f"post: missing passanger count: {df['passenger_count'].isna().sum()}")
#     return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("credentials-gcp-zoomcamp")

    df.to_gbq(
        destination_table="zoomcamp.taxi",
        project_id="dte-de-375705",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    """Main ETL flow to load data into BigQuery"""
    df = extract_from_gcs(color, year, month)
    write_bq(df)
    print(f"Rows transfered: {len(df)}")

    return len(df)

@flow(log_prints=True)
def etl_parent_flow(color: str, year: int, month: list[int]) -> None:
    total_rows=0

    for month in months:
        total_rows += etl_gcs_to_bq(color, year, month)

    print(f"Total data being transfered into bigQuery is: {total_rows}")

if __name__ == '__main__':
    color="yellow"
    year=2019
    months=[2, 3]
    etl_parent_flow(color, year, months)