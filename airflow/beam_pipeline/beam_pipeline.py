import argparse
import logging
import re
import json
import typing
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# Setup Kafka consumer configuration
kafka_consumer_config = {
    'bootstrap.servers':'broker:29092', #'broker:9092',
    'group.id': 'pipeline-consumer'
}


class GetTimestamp(beam.DoFn):
    def process(self, record, timestamp=beam.DoFn.TimestampParam):
      yield '{} - {}'.format(timestamp.to_utc_datetime(timestamp), record)

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--num_messages',
            dest='num_messages',
            type=int,
            required=False,
            help='Number of messages for beam pipeline to consume from kafka topic')
    known_args, pipeline_args = parser.parse_known_args(argv)
    print("TEST: known_args:", known_args, "; pipeline_args:", pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session




    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        websocket_data = (pipeline 
            | beam.Create((
                (b'event', b'{"id": "BONN", "price": 200.0, "time": "1722598381000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
                (b'event', b'{"id": "NVDA", "price": 100.0, "time": "1722598382000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7176060676574707, "change": -4.05999755859375, "priceHint": "2"}'),
                (b'event', b'{"id": "GOOG", "price": 50.11000061035156, "time": "1722598384000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
                (b'event', b'{"id": "NVDA", "price": 50.0, "time": "1722598384000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
                (b'ERERLKJGklJDF89df8907sdfkjljk'),
                (b'event', b'{"id": "AMZN", "price": 10.0999984741211, "time": "1722598385000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.763392210006714, "change": -4.1100006103515625, "priceHint": "2"}'),
                (b'event', b'{"id": "NVDA", "price": 50.0, "time": "1722598387000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7176060676574707, "change": -4.05999755859375, "priceHint": "2"}'),
                (b'event', b'{"id": "BONN", "price": 400, "time": "1722914474725", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.69929575920105, "change": -4.040000915527344, "priceHint": "2"}')
            ))
        )
        # # Read data from Kafka topic
        # websocket_data = (pipeline 
        #     | ReadFromKafka(
        #         consumer_config=kafka_consumer_config,
        #         topics=['data_stream'],
        #         max_num_records=10, #known_args.num_messages,
        #         commit_offset_in_finalize=False
        #       )
        # )

        def extract_data(record):
            try:
                json_values = json.loads(record[1])
                # convert timestamp from milliseconds to seconds
                json_values['time'] = int(json_values['time'])/1000
                return (json_values['id'], json_values['time'], json_values['price'])
            except Exception as e:
                print("Error: " , e)
                return None
        
        extracted_data = (websocket_data
            | "extract data" >> beam.Map(extract_data)
            | "filter out invalid data" >> beam.Filter(lambda record: record is not None)
       #     | beam.Map(print)
        )

        timestamped_data = (extracted_data
            | "with timestamps" >> beam.Map(lambda record: beam.window.TimestampedValue(record, record[1]))
        )

        # class GetTimestamp(beam.DoFn):
        #     def process(self, record, timestamp=beam.DoFn.TimestampParam):
        #         yield '{} - {}'.format(timestamp.to_utc_datetime(timestamp), record)

        # print_timestamped = (timestamped_data   
        #     | "get timestamp" >> beam.ParDo(GetTimestamp())
        #     | "print" >> beam.Map(print)
        # )

        class GetWindow(beam.DoFn):
            def process(self, record, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
                yield '{} - {} - {}'.format(timestamp.to_utc_datetime(timestamp), window.end, record)

        windowed_data = (timestamped_data
            | "get fixed windows" >> beam.WindowInto(beam.window.FixedWindows(60))
            # | 'Count per window' >> beam.combiners.Count.PerKey()
            | "filter to price data" >> beam.Map(lambda record: (record[0], record[2]))
            | "avg stock price per minute per ticker" >> beam.combiners.Mean.PerKey()
        #    | "get window data" >> beam.ParDo(GetWindow())
            # | "print" >> beam.Map(print)
        )

        class GetOutput(beam.DoFn):
            def process(self, record, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
                yield (record[0], timestamp.to_utc_datetime(window.start), record[1])
        output_data = (windowed_data
            | "get data" >> beam.ParDo(GetOutput())
            | beam.Map(print)                            
        )

        # ExampleRow = typing.NamedTuple('ExampleRow',
        #                                [('id', int), ('time', datetime), ('price', float)])
        # beam.coders.registry.register_coder(ExampleRow, beam.coders.RowCoder)

        write_output = (output_data 
            | "Write to postgres" >> beam.io.jdbc.WriteToJdbc(
                table_name='test_write',
                driver_class_name='org.postgresql.Driver',
                jdbc_url='jdbc:postgresql://localhost:5432/postgres',
                username='postgres',
                password='postgres',
            ))
        

    


if __name__ == '__main__':
 # logging.getLogger().setLevel(logging.INFO)
  run()
