import requests
import boto3
import json
import pandas as pd
import pyhdb
import psycopg2
from io import StringIO

def extract_api_data(api_url):
    response = requests.get(api_url)
    return pd.DataFrame(response.json())

def extract_hana_data(host, port, user, password, query):
    connection = pyhdb.connect(host=host, port=port, user=user, password=password)
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame(rows, columns=columns)

def merge_data(api_df, hana_df, join_key):
    return pd.merge(api_df, hana_df, on=join_key)

def load_to_redshift(df, redshift_conn_str, table_name):
    conn = psycopg2.connect(redshift_conn_str)
    cur = conn.cursor()
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
    conn.commit()
    cur.close()
    conn.close()
