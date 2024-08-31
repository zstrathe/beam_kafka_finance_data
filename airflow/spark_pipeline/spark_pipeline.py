import os
import time
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, mean
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.streaming import StreamingQuery

def create_spark_session():
    return SparkSession.builder \
        .appName("StockDataProcessing") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.sql.streaming.checkpointLocation", "./tmp_checkpoints/") \
        .getOrCreate()

def extract_data(df):
    schema = StructType([
        StructField("id", StringType()),
        StructField("price", FloatType()),
        StructField("time", StringType())
    ])
    return df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", (col("time").cast("float") / 1000).cast("timestamp")) \
        .select(
            col("id").alias("stock_ticker"),
            col("time").alias("timestamp"),
            col("price")
        )

def process_data(spark):  
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "data_stream") \
        .load()
#  .option("startingOffsets", "earliest") \

    # Extract and transform data
    extracted_df = extract_data(df)

    # clean df by dropping duplicate data
    # also set a 15 minute watermark to cutoff late arriving data
    cleaned_df = extracted_df \
        .withWatermark("timestamp", "15 minutes") \
        .dropDuplicatesWithinWatermark(["timestamp", "stock_ticker"])

    # Window and aggregate data
    # use 5 minute windows and avg of price per window
    ##### and set a 15 minute watermark (cutoff for late-arriving data)
    windowed_df = cleaned_df \
        .groupBy(
            window("timestamp", "5 minutes"),
            "stock_ticker"
        ) \
        .agg(mean("price").alias("avg_price"))
#   .withWatermark("timestamp", "15 minutes") \

    # Prepare output
    output_df = windowed_df.select(
        col("stock_ticker"),
        col("window.end").alias("timestamp"),
        col("avg_price").alias("price")
    )

    output_df.writeStream.format("console").start()

    # Write to PostgreSQL
    # to emit results early (when using "update" mode) use a processingTime trigger to set when 
    # results will start emitting for each window 
    # (set outputMode to "append" when not emitting early results)
    query = output_df \
        .writeStream \
        .trigger(processingTime='1 minutes') \
        .outputMode("update") \
        .foreachBatch(write_to_postgres) \
        .start()
    
    # monitor query and stop if idle (no new records from kafka) for 15 minutes
    monitor_and_stop_query_and_log_offsets(query, max_idle_time_seconds=60*15)

    query.awaitTermination()

def write_to_postgres(batch_df, batch_id):
    print(f"Writing batch {batch_id} to Postgres")
    print(f"Number of records: {batch_df.count()}")

    target_table_name = 'test_write'
    staging_table_name = target_table_name + '_staging'
    jdbc_properties = {
        "host": "db",
        "port": "5432",
        "database": "test_db",
        "user": "postgres",
        "password": "mysecretpassword",
        "driver": "org.postgresql.Driver"
    }
    jdbc_url = f"jdbc:postgresql://{jdbc_properties['host']}:{jdbc_properties['port']}/{jdbc_properties['database']}"

    # write staging table to PostgreSQL
    batch_df.write \
        .jdbc(url=jdbc_url, table=staging_table_name, mode="overwrite", properties=jdbc_properties)
    
    # run query to "upsert" from staging table to target table
    with psycopg2.connect(**{k:v for k, v in jdbc_properties.items() if k != 'driver'}) as conn:
        with conn.cursor() as cursor: 
            upsert_sql = (
                f"INSERT INTO {target_table_name} (stock_ticker, timestamp, price)"
                "SELECT stock_ticker, timestamp, price "
                f"FROM {staging_table_name} "
                "ON CONFLICT(stock_ticker, timestamp) "
                "DO UPDATE SET price = EXCLUDED.price"
            )
            cursor.execute(upsert_sql) 
            cursor.execute(f'DROP TABLE {staging_table_name}')
        conn.commit()

def monitor_and_stop_query_and_log_offsets(query, max_idle_time_seconds):
    last_progress_time = time.time()

    # initialize kafka offset logging
    offsets_logger = OffsetsWrittenToDBLogger()
   
    while query.isActive:
        time.sleep(10)  # Check every 10 seconds

        # Get the latest progress
        latest_progress = query.lastProgress
        
        if latest_progress:
            # print(latest_progress)
            
            # check and update custom logging of kafka offsets
            offsets_logger.check_progress(latest_progress)

            # update the last progress time if new data was processed
            if latest_progress['numInputRows'] > 0:
                last_progress_time = time.time()
            
            # check if idle time threshold is exceeded
            idle_time = time.time() - last_progress_time
            if idle_time > max_idle_time_seconds:
                print(f"No new data received for {idle_time} seconds. Stopping the query.")
                query.stop()
                break

        # Print some progress information
        print(f"Query is still active. Idle time: {time.time() - last_progress_time} seconds")

class OffsetsWrittenToDBLogger:
    ''' logger to keep track of Kafka high watermark offsets for records that have been included in db writes
    '''
    def __init__(self):
        # each time spark streaming query is started, the batchId will start at zero
        self.last_seen_result_batch_id = 0
        
        self.last_step_offsets = {0: 0}

    def check_progress(self, next_progress: StreamingQuery.lastProgress):
        ''' 
        check progress of most recent batch
            param: current_batch_id (str): the batch id of the step (micro-batch) being run 
                '''
        
        # update the offset file when the batch id increases 
        # (this assumes that new batch always follows a db write operation??)
        if next_progress['batchId'] > self.last_seen_result_batch_id:
            with open('last_offsets', 'w', encoding='utf-8') as f:
                    f.write(str(self.last_step_offsets))
                    print('Updated offset file!')   

        # update the current batch offsets
        latest_progress_offsets = next_progress['sources'][0]['endOffset']['data_stream']
        latest_progress_offsets = {int(k): int(v) for k, v in latest_progress_offsets.items()}
        
        # update tracked offsets and batch
        self.last_step_offsets = latest_progress_offsets
        self.last_seen_result_batch_id = next_progress['batchId']

def run_pipeline():
    # change current dir to spark_pipeline dev directory
    os.chdir('/opt/airflow/spark_pipeline') # COMMENT OUT IF NOT RUNNING VIA AIRFLOW
    spark = create_spark_session()
    process_data(spark)


if __name__ == "__main__":
    run_pipeline()
