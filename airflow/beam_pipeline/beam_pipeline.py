'''
NOT IN USE:
    Unfortunately I couldn't get Beam to run this streaming pipeline without crashing while using the Direct Runner. 
    It works fine when running in batch processing mode.
    Perhaps it would work with a Beam runner that has more development effort behind it (Google Dataflow or Spark runner)
'''

import argparse
import logging
import json

import apache_beam as beam
#from apache_beam import coders
#from apache_beam.io import ReadFromText
#from apache_beam.io import WriteToText
from apache_beam.io.kafka import ReadFromKafka
#from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
#from apache_beam.typehints import typehints
#from apache_beam.typehints import with_input_types, with_output_types
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from beam_postgres.io import WriteToPostgres

# Setup Kafka consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': 'localhost:9092', #'broker:29092', #'broker:9092',
    'group.id': 'pipeline-consumer'
}

# DBRowSchema = typing.NamedTuple('DBRowSchema', [
#     ('stock_ticker', str),
#     ('timestamp', float),
#     ('price', float)
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

        # test_batch_data = (pipeline 
        #     | beam.Create((
        #         ('event', '{"id": "BONN", "price": 9000, "time": "1722598381000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
        #         ('event', '{"id": "NVDA", "price": 1, "time": "1722598382000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7176060676574707, "change": -4.05999755859375, "priceHint": "2"}'),
        #         ('event', '{"id": "GOOG", "price": 50.11000061035156, "time": "1722598384000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
        #         ('event', '{"id": "NVDA", "price": 50.0, "time": "1722598384000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7542335987091064, "change": -4.099998474121094, "priceHint": "2"}'),
        #         ('ERERLKJGklJDF89df8907sdfkjljk'),
        #         ('event', '{"id": "AMZN", "price": 10.0999984741211, "time": "1722598385000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.763392210006714, "change": -4.1100006103515625, "priceHint": "2"}'),
        #         ('event', '{"id": "NVDA", "price": 50.0, "time": "1722598387000", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.7176060676574707, "change": -4.05999755859375, "priceHint": "2"}'),
        #         ('event', '{"id": "BONN", "price": 8000, "time": "1722914474725", "exchange": "NMS", "quoteType": "CURRENCY", "changePercent": -3.69929575920105, "change": -4.040000915527344, "priceHint": "2"}')
        #     ))
        # )

        # Read data from Kafka topic
        websocket_data = (
            pipeline 
            | ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': 'localhost:9092', #'broker:29092', #'broker:9092',
                    'group.id': 'pipeline-consumer'
                },
                topics=['data_stream'],
                #max_read_time=20,
               # max_num_records=5000, #known_args.num_messages,
                #commit_offset_in_finalize=False
              )
        )

        def extract_data(element):
            try:
                json_values = json.loads(element[1].decode())
                # convert timestamp from milliseconds to seconds
                json_values['time'] = int(json_values['time'])/1000
                return (str(json_values['id']), float(json_values['time']), float(json_values['price']))
            except Exception as e:
                print("Error: " , e)
                return None
         
        extracted_data = (
            websocket_data
           | "extract data" >> beam.Map(extract_data)
           | "filter out invalid data" >> beam.Filter(lambda element: element is not None)
           | "DEBUG: after extracted" >> beam.ParDo(DebugPrintAndReturn('after extracted'))
        )

        ## TODO: remove duplicate data

        timestamped_data = (
            extracted_data
            | "with timestamps" >> beam.Map(lambda element: beam.window.TimestampedValue(element, element[1]))
            | "DEBUG: after timestamped" >> beam.ParDo(DebugPrintAndReturn('after timestamped'))
            
        )

        windowed_data = (
            timestamped_data
            | "DEBUG: before windowing" >> beam.ParDo(DebugPrintAndReturn('before windowing'))
            | "get fixed windows" >> beam.WindowInto(
                    beam.window.FixedWindows(60*5),
                    trigger=AfterWatermark(early=AfterProcessingTime(delay=30), late=AfterCount(1)),
                    accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
                    allowed_lateness=2592000
                )
            | "DEBUG: after windowing" >> beam.ParDo(DebugPrintAndReturn('after windowing'))
            | "filter to price data" >> beam.Map(lambda element: (element[0], element[2]))
            | "avg stock price per minute per ticker" >> beam.combiners.Mean.PerKey()
            | "DEBUG: After aggregation" >> beam.ParDo(DebugPrintAndReturn('after aggregation'))
        )

        class FormatOutput(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                yield {'stock_ticker': str(element[0]), 'timestamp': float(window.end), 'price': float(element[1])}
               # yield DBRowSchema(str(element[0]), float(window.end), float(element[1]))
        
        # read postgres password from docker compose secrets file
        # db_password = open("/run/secrets/db-password", "r").read()
        
        write_output = (
            windowed_data
            | "format output" >> beam.ParDo(FormatOutput())
            | "DEBUG: Before postgres upsert" >> beam.ParDo(DebugPrintAndReturn('before postgres upsert'))#.with_output_types(DBRowSchema)             
            | "upsert to postgres" >> WriteToPostgres(
                f"host=localhost port=5432 dbname=test_db user=postgres password=mysecretpassword",
                ("INSERT INTO test_write_beam (stock_ticker, timestamp, price)"
                 "VALUES (%(stock_ticker)s, %(timestamp)s, %(price)s)"
                 "ON CONFLICT(stock_ticker, timestamp)"
                 "DO UPDATE SET price = %(price)s")
              )
        )

        # write_output = (output_data
        #     | "Write to postgres" >> beam.io.jdbc.WriteToJdbc(
        #          table_name='test_write_beam',
        #          driver_class_name='org.postgresql.Driver',
        #          jdbc_url='jdbc:postgresql://db:5432/test_db',
        #          username='postgres',
        #          password='mysecretpassword',
        #       )
        # )

    # pipeline.run().wait_until_finish()
        

if __name__ == '__main__':
#   logging.getLogger().setLevel(logging.INFO)
  run()
