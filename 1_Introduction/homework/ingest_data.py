import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

zone_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

def main(params):
    if params.url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {params.url} -O {csv_name}")
    os.system(f"wget {zone_url} -O taxi_zone_lookup.csv")

    engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}')

    df_taxi = pd.read_csv(csv_name, compression='gzip')
    df_taxi_zone = pd.read_csv('taxi_zone_lookup.csv')

    df_taxi.lpep_pickup_datetime = pd.to_datetime(df_taxi.lpep_pickup_datetime)
    df_taxi.lpep_dropoff_datetime = pd.to_datetime(df_taxi.lpep_dropoff_datetime)

    df_taxi.to_sql(name=params.table_name, con=engine, if_exists='replace')
    df_taxi_zone.to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data into a database')

    parser.add_argument('--user', type=str, help='database user')
    parser.add_argument('--password', type=str, help='database password')
    parser.add_argument('--host', type=str, help='database host')
    parser.add_argument('--port', type=str, help='database port')
    parser.add_argument('--db', type=str, help='database name')
    parser.add_argument('--table_name', type=str, help='table name')
    parser.add_argument('--url', type=str, help='url to download data from')

    args = parser.parse_args()

    main(args)
