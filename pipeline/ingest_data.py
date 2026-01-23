#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


year = 2025
month = 11

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

zonedtype = dtype = {
    "VendorID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'

pg_user = 'root'
pg_pass='root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'
target_table = 'taxi_trips_2025_11'
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(url)          # or url/http/s3 path

# Optional: select only needed columns to save memory
# df = pd.read_parquet("your_file.parquet", columns=["col1", "col2", "col3"])

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",           # or "append" if table already exists
        method="multi",                # faster for larger inserts
        index=False
    )
    print("loaded data")

if __name__ == '__main__':
    run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table)





