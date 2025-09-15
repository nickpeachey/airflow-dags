from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.hooks.base import BaseHook
from datetime import datetime

with DAG(
    dag_id='spark_kubernetes_job',
    description='Submit a SparkApplication to Kubernetes using the Spark K8s Operator',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'kubernetes'],
) as dag:

    def _log_minio_conf():
        context = get_current_context()
        dag_run = context.get('dag_run')
        conf = getattr(dag_run, 'conf', {}) or {}
        minio_conn_id = conf.get('minio_conn_id', 'minio_conn')

        # Resolve the Airflow connection (do not print secrets)
        try:
            conn = BaseHook.get_connection(minio_conn_id)
            safe_conn_summary = {
                'conn_id': conn.conn_id,
                'conn_type': conn.conn_type,
                'host': conn.host,
                'port': conn.port,
                'login': conn.login,
                'schema': conn.schema,
                'extra': bool(conn.extra),  # indicate presence only
            }
            print(f"Using MinIO connection: {safe_conn_summary}")
        except Exception as e:
            print(f"Failed to resolve connection '{minio_conn_id}': {e}")

    log_minio_conf = PythonOperator(
        task_id='log_minio_conf',
        python_callable=_log_minio_conf,
    )

    submit_spark_app = SparkKubernetesOperator(
        task_id='submit_spark_app',
        namespace="default",
        # Path to your SparkApplication manifest (relative to the Airflow worker filesystem)
        application_file='spark-job.yaml',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=False,
        in_cluster=True
    )
    log_minio_conf >> submit_spark_app
