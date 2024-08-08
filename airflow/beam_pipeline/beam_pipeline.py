import argparse
import logging
import re
import json
import typing
from datetime import datetime

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.typehints import typehints
from apache_beam.typehints import with_input_types, with_output_types
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from beam_postgres.io import WriteToPostgres
from dataclasses import dataclass

# Setup Kafka consumer configuration
kafka_consumer_config = {
    'bootstrap.servers':'broker:29092', #'broker:9092',
    'group.id': 'pipeline-consumer'
}

# DBRowSchema = typing.NamedTuple('DBRowSchema', [
#     ('stock_ticker', str), # beam.coders.StrUtf8Coder
#     ('timestamp', float), # beam.coders.FloatCoder
#     ('price', float) # beam.coders.FloatCoder
# ])
# coders.registry.register_coder(DBRowSchema, coders.RowCoder)

class DebugPrintAndReturn(beam.DoFn):
    def __init__(self, identifier):
        self.lines = []
        self.identifier = identifier
        self.lines.append(f'\nPrinting test output for: {identifier}')
    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam): #  
        if self.identifier == 'after timestamped':
            self.lines.append(f'{element}; ts: {timestamp}; formatted: {timestamp.to_utc_datetime(timestamp)}')
        elif self.identifier == 'after windowing':
            self.lines.append(f'{element}; ts: {timestamp.to_utc_datetime(timestamp)}; window end: {window.end}; formatted: {timestamp.to_utc_datetime(window.end)}')
        else:
            self.lines.append(f'{element}')
        yield element
    def finish_bundle(self):
        # [logging.info(line) for line in self.lines]
        [print(line) for line in self.lines]


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
                ('event', '{"id": "BONN", "price": 200.0, "time": "1722598381000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
                ('event', '{"id": "NVDA", "price": 100.0, "time": "1722598382000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7176060676574707, "change": -4.05999755859375, "priceHint": "2"}'),
                ('event', '{"id": "GOOG", "price": 50.11000061035156, "time": "1722598384000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
                ('event', '{"id": "NVDA", "price": 50.0, "time": "1722598384000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
                ('ERERLKJGklJDF89df8907sdfkjljk'),
                ('event', '{"id": "AMZN", "price": 10.0999984741211, "time": "1722598385000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.763392210006714, "change": -4.1100006103515625, "priceHint": "2"}'),
                ('event', '{"id": "NVDA", "price": 50.0, "time": "1722598387000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7176060676574707, "change": -4.05999755859375, "priceHint": "2"}'),
                ('event', '{"id": "BONN", "price": 8000, "time": "1722914474725", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.69929575920105, "change": -4.040000915527344, "priceHint": "2"}')
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
                return (str(json_values['id']), float(json_values['time']), float(json_values['price']))
            except Exception as e:
                print("Error: " , e)
                return None

        extracted_data = (
            websocket_data
           | "extract data" >> beam.Map(extract_data)
           | "filter out invalid data" >> beam.Filter(lambda record: record is not None)
           | "DEBUG: after extracted" >> beam.ParDo(DebugPrintAndReturn('after extracted'))
        )

        ## TODO: remove duplicate data

        timestamped_data = (
            extracted_data
            | "with timestamps" >> beam.Map(lambda record: beam.window.TimestampedValue(record, record[1]))
            | "DEBUG: after timestamped" >> beam.ParDo(DebugPrintAndReturn('after timestamped'))
        )

        windowed_data = (
            timestamped_data
            | "DEBUG: before windowing" >> beam.ParDo(DebugPrintAndReturn('before windowing'))
            | "get fixed windows" >> beam.WindowInto(
                    beam.window.FixedWindows(60),
                    # trigger=AfterWatermark(early=AfterCount(60)),
                    # accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
                )
            | "filter to price data" >> beam.Map(lambda record: (record[0], record[2]))
            | "avg stock price per minute per ticker" >> beam.combiners.Mean.PerKey()
            | "DEBUG: After windowing" >> beam.ParDo(DebugPrintAndReturn('after windowing'))
        )

        class GetOutput(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                yield {'stock_ticker': str(element[0]), 'timestamp': float(window.end), 'price': float(element[1])}

        output_data = (
            windowed_data
            | "format output" >> beam.ParDo(GetOutput())
            | "DEBUG: Before JDBCWrite" >> beam.ParDo(DebugPrintAndReturn('before JDBC write'))             
        )
        
        write_output = (
            output_data
            | "upsert to postgres" >> WriteToPostgres(
                "host=db port=5432 dbname=test_db user=postgres password=mysecretpassword",
                ("INSERT INTO test_write_beam (stock_ticker, timestamp, price)"
                 "VALUES (%(stock_ticker)s, %(timestamp)s, %(price)s)"
                 "ON CONFLICT(stock_ticker, timestamp)"
                 "DO UPDATE SET price = %(price)s")
              )
        )

        # write_output = (output_data
        #     | "Write to postgres" >> beam.io.jdbc.WriteToJdbc(
        #          table_name='test_write_beam',
        #          driver_class_name='org.postgresql:postgresql:42.7.3', # org.postgresql.Driver 
        #          jdbc_url='jdbc:postgresql://db:5432/test_db',
        #          username='postgres',
        #          password='mysecretpassword',
        #       )
        # )

        # write_output = (pipeline
        #     | "create data" >> beam.Create([
        #         DBRowSchema(stock_ticker='TEST', timestamp=12345, price=100.00)
        #       ]).with_output_types(DBRowSchema)
        #     | "Write to postgres" >> beam.io.jdbc.WriteToJdbc(
        #          table_name='test_write_beam',
        #          driver_class_name='org.postgresql.Driver',
        #          jdbc_url='jdbc:postgresql://db:5432/test_d',
        #          username='postgres',
        #          password='mysecretpassword',
        #          classpath=['org.postgresql:postgresql:42.7.3']
        #       )
        # )
        pipeline.run().wait_until_finish()
        

if __name__ == '__main__':
#   logging.getLogger().setLevel(logging.INFO)
  run()
