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

logger = logging.getLogger("airflow.task")

class CheckKafkaNewMessagesSensor:
    ''' airflow sensor to check kafka topic for new messages for specified group_id
        - args: threshold: int: minumum number of new messages required to trigger sensor 
    '''
    def __init__(self, 
                 topic_name: str = 'data_stream', 
                 group_id: str = 'pipeline-consumer', 
                 broker_bootstrap_url: str = 'broker:29092', # broker:29092 inside docker, localhost:9092 outside of docker
                 new_message_threshold: int = 25,
                 current_offset_source: str = "",
                 spark_checkpoints_dir: str = "", 
                 *args,
                 **kwargs
        ) -> None:

        self.topic_name = topic_name
        self.group_id = group_id
        url = broker_bootstrap_url.split(":")
        self.url_host = url[0]
        self.url_port = int(url[1])
        self.msg_threshold = new_message_threshold
        
        if current_offset_source == 'spark' and spark_checkpoints_dir == "":
            raise ValueError("need to include spark checkpoints dir if using spark offsets!")
        elif current_offset_source == 'spark':
            self.current_offset_source = current_offset_source
            self.spark_checkpoints_dir = spark_checkpoints_dir # "../spark_pipeline/tmp_checkpoints", (RUNNING IN DOCKER VIA AIRFLOW)"spark_pipeline/tmp_checkpoints"

    @classmethod
    def airflow_callable(cls, *args, **kwargs):
        '''callable method to call with airflow PythonSensor
        '''
        instance = cls(*args, **kwargs)
        return instance.check()

    def check(self) -> bool:
        ''' get current - high watermark offsets for kafka topic and return true if greater than msg_threshold
        '''
        if self.current_offset_source == "kafka":
            current_offsets = self.get_current_offsets_from_kafka()
        elif self.current_offset_source == "spark":
            current_offsets = self.get_current_offsets_from_spark_logged()

        end_offsets = self.get_high_watermark_offsets_from_kafka()
        
        # check if there are new messages
        print("CURRENT AND END OFFSETS:", " current: ", current_offsets, "; end: ", end_offsets)
        for partition, current_offset in current_offsets.items():
            end_offset = end_offsets.get(partition, 0)
            num_new_messages = end_offset - current_offset
            if num_new_messages >= self.msg_threshold:
                # New messages are available
                print(f'ABOVE MESSAGE THRESHOLD ({self.msg_threshold}): {num_new_messages} NEW MESSAGES AVAILABLE IN KAFKA')
                return True
            elif num_new_messages > 0:
                print(f'BELOW MESSAGE THRESHOLD ({self.msg_threshold}): {num_new_messages} NEW MESSAGES AVAILABLE IN KAFKA')
                return False
        # No new messages found
        print('NEW MESSAGES ARE NOT AVAILABLE IN KAFKA')
        return False

    def get_current_offsets_from_spark_checkpoint(self) -> dict:
        ''' read spark checkpoint file directory to retrieve current offsets 
            (when running pipeline via spark, because spark tracks offsets separately and does not commit them to kafka)
        '''
        max_timestamp = 0
        max_timestamp_offsets = {0: 0}
        for (root, _, files) in os.walk(self.spark_checkpoints_dir):  
            if root.endswith('/offsets'):
                for file in files:
                    # read only files that don't have any extension
                    if not os.path.splitext(file)[1]:
                        try:
                            file_path = os.path.join(root, file)
                            lines = open(file_path).readlines()
                            ts = json.loads(lines[1]).get('batchTimestampMs')
                            if ts > max_timestamp:
                                offsets = json.loads(lines[2]).get(self.topic_name)
                                # convert the offset key (partition number) to an int
                                offsets = {int(k): v for k, v in offsets.items()}
                                max_timestamp = ts
                                max_timestamp_offsets = offsets
                        except Exception as e:
                            print('Error reading file: ', e)
        return max_timestamp_offsets

    def get_current_offsets_from_spark_logged(self) -> dict:
        try:
            with open('spark_pipeline/last_offsets', 'r', encoding='utf-8') as f:
                read_offsets = f.read()
            current_offsets = ast.literal_eval(read_offsets)
        except Exception as e:
            print('error reading offsets: ', e)
            current_offsets = {0: 0}
        return current_offsets

    def get_current_offsets_from_kafka(self) -> dict:
        ''' 
        get the current offset values for each partition in the kafka topic
         - use python-kafka module for this 
         - link to source for getting offset data: https://stackoverflow.com/a/49244595
        '''
        bc = BrokerConnection(self.url_host, self.url_port, socket.AF_INET)
        
        bc.connect_blocking()
        
        fetch_offset_request = OffsetFetchRequest_v3(self.group_id, None)
        
        future = bc.send(fetch_offset_request)
        
        while not future.is_done:
            for resp, f in bc.recv():
                f.success(resp)

        # setup default for current_offsets if none exist yet (i.e., partition 0, offset 0)
        current_offsets = {0: 0} 
        for t in future.value.topics:
            # t[0] is the topic string
            if t[0] == self.topic_name:
                current_offsets = {
                    # partition data for each topic is organized in a tuple as: 
                    # (partition_number[int], partition_offset[int], ??, ??)
                    d[0]: d[1] for d in t[1]
                }

        return current_offsets
        
    def get_high_watermark_offsets_from_kafka(self) -> dict:
        ''' get the total number of messages for each partition in the kafka topic
        '''
        consumer = Consumer({"bootstrap.servers": f"{self.url_host}:{self.url_port}",
                            "group.id": self.group_id})
    
        # # Get the partition information for the topic
        partitions = consumer.list_topics(topic=self.topic_name).topics[self.topic_name].partitions
        topic_partitions = [TopicPartition(self.topic_name, partition_id) for partition_id in partitions.keys()]
    
        # # Assign the partitions to the consumer
        consumer.assign(topic_partitions)

        # Get the "high watermark"/end offsets for the partitions
        end_offsets = {
            # get partition number (key) and partition offset watermarks (low, high), and only select high val
            partition.partition: consumer.get_watermark_offsets(partition, timeout=5)[1]
            for partition in topic_partitions
        }
        consumer.close()
        return end_offsets
        
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
    start_date=datetime(2024, 8, 16),
    schedule='@continuous', #'@once',   #  '@continuous',   #'*/15 * * * *',
    max_active_runs=1,
    catchup=False
    ) as dag:
    
    check_kafka_new_messages = PythonSensor(
        task_id='check_if_new_messages_in_kafka_topic',
        python_callable=CheckKafkaNewMessagesSensor.airflow_callable,
        op_kwargs={'current_offset_source': 'spark',
                   'spark_checkpoints_dir': 'spark_pipeline/tmp_checkpoints'},
        mode='poke',
        poke_interval=60*45
    )

    start_spark_pipeline = BashOperator(
        task_id="start_python_pipeline_spark",
        bash_command='python3 ../../opt/airflow/spark_pipeline/spark_pipeline.py',
    )

    # run_beam_consumer_batch_task = BeamRunPythonPipelineOperator(
    #     task_id="start_python_pipeline_local_direct_runner",
    #     py_file="/opt/airflow/beam_pipeline/beam_pipeline.py",
    #     pipeline_options={"num_messages": 5},
    #     runner='DirectRunner',
    #     py_interpreter="python3",
    #     py_system_site_packages=False,
    # )

    check_kafka_new_messages >> start_spark_pipeline