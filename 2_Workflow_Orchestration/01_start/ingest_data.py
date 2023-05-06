#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash)
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: Missing Passenger Count: {df.passenger_count.isin([0]).sum()}")
    df = df[df.passenger_count != 0]
    print(f"post: Missing Passenger Count: {df.passenger_count.isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(params, df):
    # engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}')
    
    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=params.table_name, con=engine, if_exists='replace')
        df.to_sql(name=params.table_name, con=engine, if_exists='append')


    # while True: 
    #     try:
    #         t_start = time()

    #         df = next(df_iter)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()
    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Data")
def main(args):
    log_subflow(args.table_name)
    raw_data = extract_data(args.url)
    data = transform_data(raw_data)
    ingest_data(args, data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # parser.add_argument('--user', required=True, help='user name for postgres')
    # parser.add_argument('--password', required=True, help='password for postgres')
    # parser.add_argument('--host', required=True, help='host for postgres')
    # parser.add_argument('--port', required=True, help='port for postgres')
    # parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)