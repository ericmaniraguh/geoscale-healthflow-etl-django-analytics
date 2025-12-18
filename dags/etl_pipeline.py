from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

def check_postgres_connection():
    try:
        # Use 'postgres_default' connection ID. 
        # User needs to set this up in Airflow UI -> Admin -> Connections
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        logging.info(f"Connection successful! Result: {result}")
        return True
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        raise e

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline to check connection',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='check_db_connection',
        python_callable=check_postgres_connection,
    )
