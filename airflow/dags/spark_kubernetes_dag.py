import logging
import socket
import json
from datetime import datetime
import os
import ast
from confluent_kafka import Consumer, TopicPartition
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from kafka import BrokerConnection
from kafka.protocol.commit import OffsetFetchRequest_v3
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.DEBUG)

import pathlib
from os.path import join
relative_base_path = pathlib.Path(__file__).parent.resolve()
print('test: base path: ', relative_base_path)

with DAG(
    dag_id='kafka_spark_streaming_kubernetes_dag',
    start_date=datetime(2024, 8, 16),
    schedule='@once',   #  '@continuous',   #'*/15 * * * *',
    max_active_runs=1,
    catchup=False
    ) as dag:
   
    spark_run_kubernetes = SparkKubernetesOperator(
    task_id="spark_task",
    image="gcr.io/spark-operator/spark-py:v3.1.1",  # OR custom image using that
    code_path="local://opt/airflow/spark_pipeline/spark_pipeline.py",
    application_file= "spark_job_template.yaml",  # OR spark_job_template.json
    # namespace='spark-namespace',
    # kubernetes_conn_id='kubernetes_default',
    dag=dag,
    )

    spark_run_kubernetes