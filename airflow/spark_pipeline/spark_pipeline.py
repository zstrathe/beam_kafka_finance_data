import argparse
import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, mean, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

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
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "data_stream") \
        .option("group.id", "pipeline-consumer") \
        .option("startingOffsets", "earliest") \
        .load()
#  .option("startingOffsets", "earliest") \

    # Extract and transform data
    extracted_df = extract_data(df)

    # Window and aggregate data
    windowed_df = extracted_df \
        .withWatermark("timestamp", "0 minutes") \
        .groupBy(
            window("timestamp", "5 minutes"),
            "stock_ticker"
        ) \
        .agg(mean("price").alias("avg_price"))

    # Prepare output
    output_df = windowed_df.select(
        col("stock_ticker"),
        col("window.end").alias("timestamp"),
        col("avg_price").alias("price")
    )

    output_df.writeStream.format("console").start()

    # Write to PostgreSQL
    query = output_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime='5 minutes') \
        .start()
    
    # monitor query and stop if idle (no new records from kafka) for 2 minutes
    monitor_and_stop_query_and_log_offsets(query, 60*2)

    query.awaitTermination()


def write_to_postgres(batch_df, batch_id):
    print(f"Writing batch {batch_id} to Postgres")
    print(f"Number of records: {batch_df.count()}")
    # JDBC properties
    jdbc_url = "jdbc:postgresql://db:5432/test_db"
    properties = {
        "user": "postgres",
        "password": "mysecretpassword",
        "driver": "org.postgresql.Driver"
    }

    # # Create a temporary view of the batch data
    # batch_df.createOrReplaceTempView("updates")

    # # Perform the upsert operation
    # upsert_sql = """
    # INSERT INTO test_write (stock_ticker, timestamp, price)
    # VALUES (stock_ticker, timestamp, price)
    # ON CONFLICT(stock_ticker, timestamp)
    # DO UPDATE SET price = EXCLUDED.price
    # """

    # # Execute the upsert operation
    # batch_df.sparkSession.sql(upsert_sql)

    # # Write to PostgreSQL
    batch_df.write \
        .jdbc(url=jdbc_url, table="test_write", mode="append", properties=properties)

def monitor_and_stop_query_and_log_offsets(query, max_idle_time_seconds):
    last_progress_time = time.time()
    last_progress_batch_id = 0
    last_progress_num_input_rows = 0

    while query.isActive:
        # Wait for some time before checking progress
        time.sleep(10)  # Check every 10 seconds

        # Get the latest progress
        latest_progress = query.lastProgress
        
        if latest_progress:
            # look for case when batchId increases, and the number of input rows goes from a positive number to zero
            # seems reasonably safe to assume that the offset has decreased then and rows have been written to db
            if latest_progress['batchId'] > last_progress_batch_id and latest_progress['numInputRows'] == 0 and last_progress_num_input_rows > 0:
                latest_progress_offsets = latest_progress['sources'][0]['endOffset']['data_stream']
                # make sure the offsets partition and value are stored as ints for comparison
                latest_progress_offsets = {int(k): int(v) for k, v in latest_progress_offsets.items()}
                with open('last_offsets', 'w', encoding='utf-8') as f:
                    f.write(str(latest_progress_offsets))
                print('Updated offset file!')

            last_progress_batch_id = latest_progress['batchId']
            last_progress_num_input_rows = latest_progress['numInputRows']
           
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

def run_pipeline():
    # change current dir to spark_pipeline dev directory
    os.chdir('/opt/airflow/spark_pipeline') # COMMENT OUT IF NOT RUNNING VIA AIRFLOW
    spark = create_spark_session()
    process_data(spark)

if __name__ == "__main__":
    run_pipeline()
