from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='watch_s3_bucket',
    default_args=default_args,
    description='Watch for new files in an S3 bucket',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'sensor'],
) as dag:

    watch_for_file = S3KeySensor(
        task_id='watch_for_file',
        bucket_key='*',  # Watch for any file in the bucket
        bucket_name='data-lake',
        aws_conn_id='minio_conn',
        wildcard_match=True,  # Treat bucket_key as a wildcard pattern
        poke_interval=2,
        timeout=60 * 60,
        mode='poke',
    )

    list_s3_keys = S3ListOperator(
        task_id='list_s3_keys',
        bucket='data-lake',
        prefix='',  # list all keys; adjust as needed
        aws_conn_id='minio_conn',
        do_xcom_push=True,
    )

    def _pick_latest_key(**context):
        keys = context['ti'].xcom_pull(task_ids='list_s3_keys') or []
        if not keys:
            raise ValueError('No keys found in bucket data-lake')
        # Pick the last key lexicographically; adjust to your desired selection logic
        keys = sorted(keys)
        selected = keys[-1]
        print(f"Selected S3 key: {selected}")
        return selected

    pick_latest_key = PythonOperator(
        task_id='pick_latest_key',
        python_callable=_pick_latest_key,
    )

    trigger_spark_dag = TriggerDagRunOperator(
        task_id='trigger_spark_k8s_dag',
        trigger_dag_id='spark_kubernetes_job',  # Must match the DAG ID in spark_kubernetes_dag.py
        conf={
            'minio_conn_id': 'minio_conn',
            'namespace': 'default',
            's3_bucket': 'data-lake',
            's3_key': '{{ ti.xcom_pull(task_ids="pick_latest_key") }}',
            # You can add more context here if useful, e.g. bucket/key patterns
            # 'bucket_name': 'data-lake',
        },
        # wait_for_completion=False is default; set to True if you want to block until completion
    )

    watch_for_file >> list_s3_keys >> pick_latest_key >> trigger_spark_dag