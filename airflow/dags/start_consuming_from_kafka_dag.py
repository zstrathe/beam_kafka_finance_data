from datetime import datetime, timedelta
from confluent_kafka import Consumer, TopicPartition
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from kafka import BrokerConnection
from kafka.protocol.commit import *
import socket
from confluent_kafka import Consumer, TopicPartition

import logging
logger = logging.getLogger("airflow.task")

def check_kafka_topic():
    topic_name = 'data_stream'
    group_id = 'pipeline-consumer'
    hostname = 'broker'
    port = 29092

    ############
    ### FIRST: get the current offset values for each partition in the kafka topic
    ############
    # use python-kafka module for this 
    # link to source for getting offset data: https://stackoverflow.com/a/49244595
    bc = BrokerConnection(hostname, port, socket.AF_INET)
    
    bc.connect_blocking()
    
    fetch_offset_request = OffsetFetchRequest_v3(group_id, None)
    
    future = bc.send(fetch_offset_request)
    
    while not future.is_done:
        for resp, f in bc.recv():
            f.success(resp)

    current_offsets = None
    for t in future.value.topics:
        # t[0] is the topic string
        if t[0] == topic_name:
            current_offsets = {
                # partition data for each topic is organized in a tuple as: 
                # (partition_number[int], partition_offset[int], ??, ??)
                d[0]: d[1] for d in t[1]
            }
    print("TEST: CURRENT OFFSETS:", current_offsets)
    
    ############
    ### SECOND: get the total number of messages for each partition in the kafka topic
    ############
    consumer = Consumer({"bootstrap.servers": f"{hostname}:{port}",
                         "group.id": group_id})
  
    # # Get the partition information for the topic
    partitions = consumer.list_topics(topic=topic_name).topics[topic_name].partitions
    topic_partitions = [TopicPartition(topic_name, partition_id) for partition_id in partitions.keys()]
   
    # # Assign the partitions to the consumer
    consumer.assign(topic_partitions)

    # Get the "high watermark"/end offsets for the partitions
    end_offsets = {
        # get partition number (key) and partition offset watermarks (low, high), and only select high val
        partition.partition: consumer.get_watermark_offsets(partition, timeout=5)[1]
        for partition in topic_partitions
    }
    consumer.close()
    ############
    ### THIRD: Check if there are new messages
    ############
    if current_offsets:
        for partition, current_offset in current_offsets.items():
            end_offset = end_offsets[partition]
            print('TEST: current_offset:', current_offset, '...end_offset:', end_offset)
            if current_offset < end_offset:
                # New messages are available
                print('NEW MESSAGES AVAILABLE IN KAFKA')
                return True

    # No new messages found
    print('NEW MESSAGES ARE NOT AVAILABLE IN KAFKA')
    return False
    
def check_task_status(dag_id, current_run_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    for dag_run in dag_runs:
        if dag_run.state == 'running' and dag_run.run_id != current_run_id:
            print('Detected that this DAG is already currently running!')
            return True
    return False
    
def run_spark_streaming_task(new_messages_available, spark_task_running_status):
    # trigger the Spark Streaming task
    if new_messages_available and not spark_task_running_status:
        import time
        print('TEST: running spark streaming task...sleeping for 1 hour')
        time.sleep(60*60) # sleep for 1 hour
        print('TEST: finished task!')
    else:
        print('TEST: not running spark streaming task (no messages available or DAG is already running)')


# default_args = {
#     'owner': 'your_name',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 3, 30),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
#     'max_active_runs': 1,
#     'catchup': False
# }

with DAG(
    dag_id='kafka_spark_streaming_dag',
    start_date=datetime(2024, 3, 30),
    schedule='*/15 * * * *',
    catchup=False
    ) as dag:
    check_kafka_task = PythonOperator(
        task_id='check_if_new_messages_in_kafka_topic',
        python_callable=check_kafka_topic
    )

    check_spark_task_status = PythonOperator(
        task_id='check_if_spark_task_is_running',
        python_callable=check_task_status,
        op_args=['kafka_spark_streaming_dag', '{{run_id}}']
    )

    run_spark_streaming_task = PythonOperator(
        task_id='run_spark_streaming_task',
        python_callable=run_spark_streaming_task,
        op_args=[check_kafka_task.output, check_spark_task_status.output]
    )

    check_kafka_task >> check_spark_task_status >> run_spark_streaming_task
