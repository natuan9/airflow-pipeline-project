from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from spark_operators import SparkHealthCheckOperator
from telegram_notifier import send_telegram_alert
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_telegram_alert,
}

def check_database_ingestion():
    """
    Check if new records are being inserted into fact_view.
    Adjust interval to be longer than the maximum Spark processing time.
    """
    hook = PostgresHook(postgres_conn_id='postgres_spark')
    # Check if there are records from the last 90 minutes
    # time_stamp is stored as Unix timestamp (bigint), so we need to convert it
    sql = """
        SELECT COUNT(*) FROM fact_view 
        WHERE to_timestamp(time_stamp / 1000) > NOW() - INTERVAL '90 minutes'
    """
    count = hook.get_first(sql)[0]
    
    if count == 0:
        logging.warning("No data ingested in the last 90 minutes!")
        # We raise AirflowSkipException if we don't want to Fail, 
        # but for monitoring, let's keep it as is or log.
    else:
        logging.info(f"Data ingestion OK. Found {count} records in the last 90 minutes.")
    return count

with DAG(
    'spark_monitoring_dag',
    default_args=default_args,
    description='Monitor Spark (YARN) Health and Data Ingestion',
    schedule_interval=timedelta(minutes=10),
    start_date=days_ago(1),
    catchup=False,
    tags=['monitoring', 'spark', 'postgres'],
) as dag:

    # Task 1: Check YARN ResourceManager and Nodes
    check_spark_health = SparkHealthCheckOperator(
        task_id='check_spark_health',
        rm_host='resourcemanager',
        rm_port=8088
    )

    # Task 2: Check Postgres Ingestion
    # Note: Requires a connection 'postgres_spark' to be configured in Airflow UI
    check_db = PythonOperator(
        task_id='check_database_ingestion',
        python_callable=check_database_ingestion
    )

    check_spark_health >> check_db
