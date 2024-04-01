import logging
import socket
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from kafka import BrokerConnection
from kafka.protocol.commit import *

logger = logging.getLogger("airflow.task")

def check_task_status(task_instance_str, current_run_id):
    dag_id = task_instance_str.split('__')[0]
    dag_runs = DagRun.find(dag_id=dag_id)
    for dag_run in dag_runs:
        if dag_run.state == 'running' and dag_run.run_id != current_run_id:
            print('TEST: Detected that this DAG is already currently running!')
            return False
    print('TEST: did not detect DAG currently running!')
    return True
    
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
    print("TEST CURRENT AND END OFFSETS:", current_offsets, end_offsets)
    if current_offsets:
        for partition, current_offset in current_offsets.items():
            end_offset = end_offsets[partition]
            print('TEST: current_offset:', current_offset, '...end_offset:', end_offset)
            if current_offset < end_offset:
                # New messages are available
                print('NEW MESSAGES AVAILABLE IN KAFKA')
                return True
    elif end_offsets:
        print('COULD NOT RETRIEVE ANY CURRENT OFFSETS, BUT FOUND END OFFSETS:', end_offsets)
        return True

    # No new messages found
    print('NEW MESSAGES ARE NOT AVAILABLE IN KAFKA')
    return False

def run_spark_streaming_task():
    # start the Spark Streaming task
    print('TEST: running spark streaming task...sleeping for 1 hour')
    import time
    time.sleep(60*60) # sleep for 1 hour
    print('TEST: finished task!')

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

    check_not_already_running = ShortCircuitOperator(
        task_id='dag_not_already_running',
        python_callable=check_task_status,
        op_args=['{{task_instance_key_str}}', '{{run_id}}']
    )
    
    check_kafka_new_messages = ShortCircuitOperator(
        task_id='check_if_new_messages_in_kafka_topic',
        python_callable=check_kafka_topic
    )

    run_spark_streaming_task = PythonOperator(
        task_id='run_spark_streaming_task',
        python_callable=run_spark_streaming_task
    )

    [check_not_already_running, check_kafka_new_messages] >> run_spark_streaming_task
