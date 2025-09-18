import os
import json
from airflow import DAG
import logging
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

configPath = os.path.dirname(os.path.realpath(__file__))

configuration = configPath + '/load_json_file_dag_config.json'
config = {}
with open(configuration, 'r') as config_file:
    config = json.load(config_file)
    print(f"Configuration loaded: {config}")


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

def read_json_file():
    print(f"Reading JSON file from path: {config}")


with TaskGroup("json_tasks", 
               dag=dag, 
               tooltip="Read Json File"
               ) as json_tasks:
    
    task_id = "read_json_file"

    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    run = PythonOperator(
        task_id='read_json_file',
        python_callable=read_json_file,
        dag=dag,
    )


start >> run