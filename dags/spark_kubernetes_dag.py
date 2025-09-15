from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.hooks.base import BaseHook
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes.client import V1Secret, V1ObjectMeta
from kubernetes.client.rest import ApiException
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

    def _ensure_minio_secret():
        context = get_current_context()
        dag_run = context.get('dag_run')
        conf = getattr(dag_run, 'conf', {}) or {}
        minio_conn_id = conf.get('minio_conn_id', 'minio_conn')
        namespace = conf.get('namespace', 'airflow')
        secret_name = conf.get('secret_name', 'minio-credentials')

        # Resolve connection
        conn = BaseHook.get_connection(minio_conn_id)
        access_key = conn.login or ''
        secret_key = conn.password or ''

        hook = KubernetesHook(conn_id='kubernetes_default')
        api = hook.core_v1_client

        body = V1Secret(
            metadata=V1ObjectMeta(name=secret_name, namespace=namespace),
            string_data={
                'accessKey': access_key,
                'secretKey': secret_key,
            },
            type='Opaque',
        )

        try:
            # Try to create; if exists, replace
            api.create_namespaced_secret(namespace=namespace, body=body)
            print(f"Created secret {secret_name} in namespace {namespace}")
        except ApiException as e:
            if e.status == 409:
                api.replace_namespaced_secret(name=secret_name, namespace=namespace, body=body)
                print(f"Updated existing secret {secret_name} in namespace {namespace}")
            else:
                raise

    ensure_minio_secret = PythonOperator(
        task_id='ensure_minio_secret',
        python_callable=_ensure_minio_secret,
    )

    submit_spark_app = SparkKubernetesOperator(
        task_id='submit_spark_app',
        namespace='{{ dag_run.conf.namespace | default("airflow") }}',
        # Path to your SparkApplication manifest (relative to the Airflow worker filesystem)
        application_file='dags/spark-job.yaml',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    ensure_minio_secret >> log_minio_conf >> submit_spark_app
