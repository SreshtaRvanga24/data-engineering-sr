import requests
import psycopg2 as pg
import pandas as pd
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from io import StringIO
import csv

database = os.getenv("DB_NAME")
port = 5432
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")

conn = pg.connect(
    dbname=database,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()
print(host, database, user, password)
engine = create_engine(
    f'''postgresql+psycopg2://sreshtar.vanga:ujwalavmr@localhost:5432/sreshtar.vanga''' 
    # Acts like an adaptor - it provides an abstraction, connection pool, cross database connection
)

endpoints = ['products', 'users', 'carts', 'orders', 'posts', 'comments', 'quotes', 'todos', 'photos']
# endpoints = ['carts','posts', 'comments', 'quotes']

def get_data_from_api(url):
    try: 
        response = requests.get(url)
        # print(response)
        if response.status_code == 200:
            # print(response.json())
            return response.json()
    except Exception as error:
        print("Could not get data! Error fetching data from endpoint:", error)

def insert_data_to_db(data, table_name):
    try:
        df = pd.DataFrame(data)
        # print(df.head())
        
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )
        
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False
        )
        
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_ALL)  # ← needs import csv
        buffer.seek(0)
        
        cur.copy_expert(
            f"""
            COPY {table_name}
            FROM STDIN
            WITH (
                FORMAT CSV,
                HEADER FALSE,
                DELIMITER ',',
                QUOTE '"',
                ESCAPE '"'
            )
            """,
            buffer
        )
        conn.commit()
        print(f"Data Ingestion Successful: {table_name}")
    except Exception as error:
        conn.rollback()
        print(f"Failed {table_name}: {error}")

        
def get_paginated_data(url, endpoint, limit = 30, load_date = pd.Timestamp.now()):
    all_data = []
    skip = 0
    while True:
        data_url = f'''{url}/{endpoint}?limit={limit}&skip={skip}'''
        data = get_data_from_api(data_url)
        if not data:
            break
        print(f"Fetched {len(data[endpoint])} records from API endpoint {endpoint}.")
        temp_data = data[endpoint]
        for product in temp_data:
            product["load_date"] = load_date
        all_data.extend(temp_data)
        skip+=limit
        if skip >= data["total"]:
            break
    # print(all_data.head())
    return all_data

for i in endpoints:
    data = get_paginated_data(f"https://dummyjson.com/", endpoint = i)
    insert_data_to_db(data, "stage_" + i)

