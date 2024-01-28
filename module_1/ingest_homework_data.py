#!/usr/bin/env python
# coding: utf-8
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output_homework.csv.gz'

    # download csv
    os.system(f'wget {url} -O {csv_name}')

    # create an engine to connect to postgres database
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    # break the csv into chunksize of 100,000 rows at a time to read into table
    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # to write only the headers into database to create the table with colomn names
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            # To convert 'lpep_pickup_datetime' and 'lpep_dropoff_datetime' to the correct datetime format
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('inserted another chunk..., took %.3f second' %(t_end-t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url to download the data from')

    args = parser.parse_args()

    main(args)
