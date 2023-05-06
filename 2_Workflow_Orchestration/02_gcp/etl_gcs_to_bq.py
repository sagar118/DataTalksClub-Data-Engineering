import argparse
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:0>2}.parquet"

    gcs_block = GcsBucket.load('dtc-de-gcs')
    gcs_block.download_object_to_path(
        from_path=gcs_path, 
        to_path=f"./data/gcs/{color}_tripdata_{year}-{month:0>2}.parquet"
    )
    
    return Path(f"./data/gcs/{color}_tripdata_{year}-{month:0>2}.parquet")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""

    df = pd.read_parquet(path)
    print(f"pre: Missing Passenger Count: {df.passenger_count.isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: Missing Passenger Count: {df.passenger_count.isna().sum()}")

    return df

@task(log_prints=True)
def write_dq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_cred_block = GcpCredentials.load("dtc-de-gcp-creds")

    df.to_gbq(
        destination_table = "trips_data_all.taxi_rides",
        project_id = "teak-alloy-385319",
        credentials = gcp_cred_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = "append"
    )

@flow(name="ETL from GCS to BQ")
def etl_gcs_to_bq(params):
    """Main ETL Flow to load data into BigQuery"""

    path = extract_from_gcs(params.color, params.year, params.month)
    df = transform(path)
    write_dq(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Params for ETL from web to GCS")

    parser.add_argument("--color", required=True, help="Taxi color")
    parser.add_argument("--year", required=True, help="Data from year")
    parser.add_argument("--month", required=True, help="Data from month")

    args = parser.parse_args()

    etl_gcs_to_bq(args)