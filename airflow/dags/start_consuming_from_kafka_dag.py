import logging
import socket
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
from airflow import DAG
from airflow import models
from airflow.models import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from airflow.sensors.python import PythonSensor
from kafka import BrokerConnection
from kafka.protocol.commit import *

# add .packages/ directory to sys.path, so that other relative modules can be imported
import os
import sys

#sys.path.append(os.path.dirname('/opt/airflow/plugins')) 
#from test_operator import BeamRunPythonPipelineOperator

logger = logging.getLogger("airflow.task")
 
def check_kafka_topic_new_messages_threshold_sensor(msg_threshold: int):
    '''airflow sensor to check kafka topic for new messages for specified group_id
        - args: threshold: int: minumum number of new messages required to trigger sensor 
    '''
    topic_name = 'data_stream'
    group_id = 'pipeline-consumer'
    hostname = 'broker'
    port = 29092

    ### FIRST: get the current offset values for each partition in the kafka topic
    # use python-kafka module for this 
    # link to source for getting offset data: https://stackoverflow.com/a/49244595
    bc = BrokerConnection(hostname, port, socket.AF_INET)
    
    bc.connect_blocking()
    
    fetch_offset_request = OffsetFetchRequest_v3(group_id, None)
    
    future = bc.send(fetch_offset_request)
    
    while not future.is_done:
        for resp, f in bc.recv():
            f.success(resp)

    # setup default for current_offsets if none exist yet (i.e., partition 0, offset 0)
    current_offsets = {0: 0} 
    for t in future.value.topics:
        # t[0] is the topic string
        if t[0] == topic_name:
            current_offsets = {
                # partition data for each topic is organized in a tuple as: 
                # (partition_number[int], partition_offset[int], ??, ??)
                d[0]: d[1] for d in t[1]
            }
    print("TEST: CURRENT OFFSETS:", current_offsets)
    
    ### SECOND: get the total number of messages for each partition in the kafka topic
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
    
    ### THIRD: Check if there are new messages
    print("TEST CURRENT AND END OFFSETS:", " curr: ", current_offsets, "; end: ", end_offsets)
    for partition, current_offset in current_offsets.items():
        end_offset = end_offsets.get(partition, 0)
        print('TEST: current_offset:', current_offset, '...end_offset:', end_offset)
        num_new_messages = end_offset - current_offset
        if num_new_messages >= msg_threshold:
            # New messages are available
            print(f'TEST: ABOVE MESSAGE THRESHOLD: {num_new_messages} NEW MESSAGES AVAILABLE IN KAFKA')
            return True
        elif num_new_messages > 0:
            print(f'TEST: BELOW MESSAGE THRESHOLD: {num_new_messages} NEW MESSAGES AVAILABLE IN KAFKA')
            return False

    # No new messages found
    print('NEW MESSAGES ARE NOT AVAILABLE IN KAFKA')
    return False

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
    schedule='@once',   #  '@continuous',   #'*/15 * * * *',
    max_active_runs=1,
    catchup=False
    ) as dag:
    
    # check_kafka_new_messages = PythonSensor(
    #     task_id='check_if_new_messages_in_kafka_topic',
    #     python_callable=check_kafka_topic_new_messages_threshold_sensor,
    #     op_kwargs={'msg_threshold':300},
    #     mode='reschedule',
    #     poke_interval=600 # 10 minutes
    # )

    run_beam_consumer_batch_task = BeamRunPythonPipelineOperator(
        task_id="start_python_pipeline_local_direct_runner",
        py_file="/opt/airflow/beam_pipeline/beam_pipeline.py",
        pipeline_options={"num_messages": 5},
        runner='DirectRunner',
        py_interpreter="python3",
        py_system_site_packages=False,
    )

    # check_kafka_new_messages >> 
    run_beam_consumer_batch_task
