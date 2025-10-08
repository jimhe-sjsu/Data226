import os
import requests
import pandas as pd
import numpy as np
import snowflake.connector

from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime

user_id = Variable.get('snowflake_userid')
password = Variable.get('snowflake_password')
account = Variable.get('snowflake_account')
database = Variable.get('snowflake_database')
vwh = Variable.get('snowflake_vwh')
alphavantage_api = Variable.get('ALPHAVANTAGE_API')

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

def get_api_data(api_key, symbol):

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    r = requests.get(url)
    data = r.json()

    results = []
    for d in data["Time Series (Daily)"]:
        record = data["Time Series (Daily)"][d]
        record['date'] = d
        record['symbol'] = symbol
    results.append(record)

    return results

def load_table(con, symbol):
    target_table = f"{database}.raw.{symbol}_stock_daily"
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      date DATETIME,
      symbol varchar(25),
      open FLOAT,
      high FLOAT,
      low FLOAT,
      close FLOAT,
      volume FLOAT,
      PRIMARY KEY (date, symbol)
    )""")

def delete_all_data(con, symbol):
    con.execute(f"""
    TRUNCATE TABLE {database}.raw.{symbol}_stock_daily
    """)

def insert_data(con, data, symbol):
  for row in data:
    open = row["1. open"]
    high = row["2. high"]
    low = row["3. low"]
    close = row["4. close"]
    volume = row["5. volume"]
    date = row["date"]
    symbol = row["symbol"]

    insert_sql = f"INSERT INTO {database}.raw.{symbol}_stock_daily (date, symbol, open, high, low, close, volume) VALUES ('{date}', '{symbol}', {open}, {high}, {low}, {close}, {volume})"
    # print(insert_sql)
    con.execute(f"{insert_sql}")

def show_table_length(con, symbol):
  con.execute(f"""
  SELECT COUNT(1) FROM {database}.raw.{symbol}_stock_daily
  """)
  return con.fetchall()

@task
def refresh_data(con, symbol, data):
  
    try:
        delete_all_data(con, symbol)
        load_table(con, symbol)
        insert_data(con, data, symbol)
    except Exception as e:
        print(e)
        con.rollback()
        raise

with DAG(
    dag_id = 'amazon_stock_data_pipeline',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    symbol = "AMZN"
    data = get_api_data(alphavantage_api, symbol)
    con = return_snowflake_conn()
    refresh_data(con, symbol, data)

    

