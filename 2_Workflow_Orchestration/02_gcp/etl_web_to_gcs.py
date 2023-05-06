import argparse
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""

    df = pd.read_csv(url, compression="gzip")
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as a parquet file"""

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""

    gcs_block = GcsBucket.load('dtc-de-gcs')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
        )
    return

@flow(name="ETL Web to GCS", log_prints=True)
def etl_web_to_gcs(params, month) -> None:
    """The main ETL Function"""
    
    print(f"Loading data for month: {month:0>2}")

    dataset_file = f"{params.color}_tripdata_{params.year}-{month:0>2}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{params.color}/{dataset_file}.csv.gz"

    df = fetch(url)
    df_clean = clean(df)
    path = write_local(df_clean, params.color, dataset_file)
    write_gcs(path)

@flow(name="Web-GCS Parent Flow")
def etl_parent_flow(params):

    for month in params.months:
        # etl_web_to_gcs(params, month)
        print(month)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Params for ETL from web to GCS")

    parser.add_argument("--color", required=True, help="Taxi color")
    parser.add_argument("--year", required=True, help="Data from year")
    parser.add_argument("--months", nargs="+", required=True, help="Data from months")

    args = parser.parse_args()

    etl_parent_flow(args)