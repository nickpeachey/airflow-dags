import os
import json
from airflow import DAG
import logging
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

configuration = '/load_json_file_dag_config.json'

with open(configuration, 'r') as config_file:
    config = json.load(config_file)
    logger.info(f"Configuration loaded: {config}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_json_file_dag',
    default_args=default_args,
    description='A DAG to load and process a JSON file',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)