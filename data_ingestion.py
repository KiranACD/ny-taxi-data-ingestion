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
    table_name_taxi_data = params.table_name_taxi_data
    table_name_zones_data = params.table_name_zones_data
    taxi_data_url = params.taxi_data_url
    zones_data_url = params.zones_data_url

    # print("URL: ", url)

    # print(f"Engine: postgresql://{user}:{password}@{host}:{port}/{db}")

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if taxi_data_url.endswith('.csv.gz'):
        csv_name_taxi_data = 'output.csv.gz'
    else:
        csv_name_taxi_data = 'output.csv'

    csv_name_zones_data = 'zones_output.csv'
    
    os.system(f'wget {taxi_data_url} -O {csv_name_taxi_data}')
    os.system(f'wget {zones_data_url} -O {csv_name_zones_data}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name_taxi_data, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.reset_index(inplace=True)

    df.head(n=0).to_sql(name=table_name_taxi_data, con=engine, if_exists='replace')

    df.to_sql(name=table_name_taxi_data, con=engine, if_exists='append')

    # while True:
    #     t_start = time()
    #     df = next(df_iter)
    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #     df.reset_index(inplace=True)
    #     df.to_sql(name=table_name_taxi_data, con=engine, if_exists="append")
    #     t_end = time()
    #     print("Inserted a chunk..., took %.3f seconds" % (t_end-t_start))

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.reset_index(inplace=True)
            df.to_sql(name=table_name_taxi_data, con=engine, if_exists="append")
            t_end = time()
            print("Inserted a chunk..., took %.3f seconds" % (t_end-t_start))
        except StopIteration:
            print("Finished ingesting data into the database")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            break
    
    df = pd.read_csv(csv_name_zones_data)

    df.head(n=0).to_sql(name=table_name_zones_data, con=engine, if_exists='replace')

    df.to_sql(name=table_name_zones_data, con=engine, if_exists='append')

    # while True:
    #     t_start = time()
    #     df = next(df_iter)
    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #     df.reset_index(inplace=True)
    #     df.to_sql(name=table_name_taxi_data, con=engine, if_exists="append")
    #     t_end = time()
    #     print("Inserted a chunk..., took %.3f seconds" % (t_end-t_start))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user 
    # password 
    # host 
    # port 
    # database name 
    # table name
    # url of the csv - https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz 

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name_taxi_data', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--table_name_zones_data', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--taxi_data_url', required=True, help='url of the csv file')
    parser.add_argument('--zones_data_url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)