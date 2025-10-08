import os
import pandas as pd
import numpy as np

from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import yfinance as yf

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn

def get_api_data(symbol):

    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="180d")

    results = []
    for date, row in hist.iterrows():
        record = {
            'date': date.strftime('%Y-%m-%d'),
            'symbol': symbol,
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close'],
            'volume': row['Volume']
        }
        results.append(record)
    return results

def load_table(con):
    target_table = f"raw.lab1_stock_daily"
    try:
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
        )
        """)
    except Exception as e:
        print(e)
        con.rollback()
        raise
    return

@task
def insert_data(data, symbol):
    con = return_snowflake_conn()
    cur = con.cursor()
    target_table = f"raw.lab1_stock_daily"
    #in case it's first run, create the table
    load_table(cur)
    try:
        for row in data:
            open = row["open"]
            high = row["high"]
            low = row["low"]
            close = row["close"]
            volume = row["volume"]
            date = row["date"]
            symbol = row["symbol"]
            cur.execute(f"""
            INSERT INTO {target_table} (date, symbol, open, high, low, close, volume)
            VALUES ('{date}', '{symbol}', {open}, {high}, {low}, {close}, {volume})
            """)
    except Exception as e:
        print(e)
        con.rollback()
        raise
    return

@task
def delete_all_data():
    cur = return_snowflake_conn().cursor()
    load_table(cur)
    cur.execute(f"""
    TRUNCATE TABLE raw.lab1_stock_daily
    """)
    return


with DAG(
    dag_id = 'lab1_yahoo_finance_data_pipeline',
    start_date = datetime(2025,10,1),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    delete_data = delete_all_data()

    amzn_data = get_api_data("AMZN")
    insert_amazon = insert_data(amzn_data, "AMZN")

    aapl_data = get_api_data("AAPL")
    insert_apple = insert_data(aapl_data, "AAPL")


delete_data >> insert_amazon >> insert_apple

