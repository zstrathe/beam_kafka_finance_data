import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, mean, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def create_spark_session():
    return SparkSession.builder \
        .appName("StockDataProcessing") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.sql.streaming.checkpointLocation", "./tmp_checkpoints/") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
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
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "data_stream") \
        .option("group.id", "pipeline-consumer") \
        .option("auto.offset.reset", "earliest") \
        .load()

    # Extract and transform data
    extracted_df = extract_data(df)

    # Window and aggregate data
    windowed_df = extracted_df \
        .withWatermark("timestamp", "2 minutes") \
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

    df.writeStream.format("console").start()

    # Write to PostgreSQL
    query = output_df \
        .writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime='5 minutes') \
        .start()

    query.awaitTermination(timeout=600)

def write_to_postgres(batch_df, batch_id):
    print(f"Writing batch {batch_id} to Postgres")
    print(f"Number of records: {batch_df.count()}")
    # JDBC properties
    jdbc_url = "jdbc:postgresql://localhost:5432/test_db"
    properties = {
        "user": "postgres",
        "password": "mysecretpassword",
        "driver": "org.postgresql.Driver"
    }

    # Create a temporary view of the batch data
    batch_df.createOrReplaceTempView("updates")

    # Perform the upsert operation
    upsert_sql = """
    INSERT INTO test_write (stock_ticker, timestamp, price)
    VALUES (stock_ticker, timestamp, price)
    ON CONFLICT(stock_ticker, timestamp)
    DO UPDATE SET price = EXCLUDED.price
    """

    # Execute the upsert operation
    batch_df.sparkSession.sql(upsert_sql)

    # # Write to PostgreSQL
    batch_df.write \
        .jdbc(url=jdbc_url, table="test_write", mode="append", properties=properties)


if __name__ == "__main__":
    spark = create_spark_session()
    process_data(spark)
