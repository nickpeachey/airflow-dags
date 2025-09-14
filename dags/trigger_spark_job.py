from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="trigger_spark_job",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    trigger_spark = SparkKubernetesOperator(
        task_id="spark_pi",
        namespace="default",
        application_file="/opt/airflow/spark-jobs/spark-pi.yaml",
        do_xcom_push=True,
    )