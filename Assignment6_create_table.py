from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_create_table(create_sql):

    cur = return_snowflake_conn()

    try:
        sql = f"{create_sql}"
        cur.execute(sql)

    except Exception as e:
        raise

def create_stage(cur):
    try:
        sql = f"""
            CREATE OR REPLACE STAGE raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        raise

@task
def run_copy_into_data(schema, table, file_path):

    logging.info(table)
    logging.info(file_path)

    cur = return_snowflake_conn()
    
    try:
        create_stage(cur)
        copy_sql = f"""
            COPY INTO {schema}.{table}
            FROM '{file_path}'
        """
        logging.info(copy_sql)
        cur.execute(copy_sql)
    except Exception as e:
        raise

with DAG(
    dag_id = 'Assignment6_create_table',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    run_create_user_session_channel = run_create_table(
        create_sql="""
            CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'  
            );
        """
    )
    run_create_session_timestamp = run_create_table(
        create_sql="""
            CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp  
            );
        """
    )
    run_copy_user_session_channel = run_copy_into_data(
        schema='raw',
        table='user_session_channel',
        file_path='@raw.blob_stage/user_session_channel.csv'
    )
    run_copy_session_timestamp = run_copy_into_data(
        schema='raw',
        table='session_timestamp',
        file_path='@raw.blob_stage/session_timestamp.csv'
    )

    run_create_user_session_channel >> run_copy_user_session_channel
    run_create_session_timestamp >> run_copy_session_timestamp
