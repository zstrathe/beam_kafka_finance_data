import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka

# Setup Kafka consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': 'localhost:41239',
    'group.id': 'beam-consumer',
    'auto.offset.reset': 'earliest'
}

with beam.Pipeline() as pipeline:
    # Read data from Kafka topic
    websocket_data = pipeline | ReadFromKafka(
        consumer_config=kafka_consumer_config,
        topics=['websocket-topic']
    )

    # Apply additional transforms on websocket_data

