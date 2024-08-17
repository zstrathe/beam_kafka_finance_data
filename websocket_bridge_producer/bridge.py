import logging
import threading
import time
import json
import base64
import websocket
import rel
from confluent_kafka import Producer
from yfindata import PricingData

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


class StockPricingDataSocket:
    # Kafka configuration
    kafka_conf = {'bootstrap.servers': 'broker:29092'}
    
    producer = Producer(kafka_conf)

    # setup the proto parser for the yahoo finance data stream
    y_finance_data_proto_parser = PricingData()

    def __init__(self):
        self.message_count = 0
        self.example_messages = []

    def run(self):
        # start a thread to monitor message count per minute
        threading.Thread(target=self.monitor_message_count, kwargs={'time_interval':60}).start()

        # start the websocket connection (use rel to dispatch and autoreconnect, 5 second delay)
        ws = websocket.WebSocketApp(
            url='wss://streamer.finance.yahoo.com/',
            on_message=self.on_message,
            on_error=self.on_error,
            on_open=self.on_open
        )
        ws.run_forever(dispatcher=rel, reconnect=5)
        rel.signal(2, rel.abort)
        rel.dispatch()
 
    def monitor_message_count(self, time_interval=60):
        while True:
            initial_count = self.message_count
            self.example_messages = []
            time.sleep(time_interval)
            logging.info('Count of messages in last %i seconds: %i' , time_interval, self.message_count - initial_count)
            if (len(self.example_messages) > 0):
                logging.info('Example messages: %s', self.example_messages)

    # WebSocket message callback function
    def on_open(self, ws):
        ticker_symbols = ['AAPL', 'AMZN', 'NVDA']
        _req = {"subscribe": ticker_symbols}
        logging.info("Sending request for data: %s", _req)
        ws.send(json.dumps(_req))

    def on_message(self, ws, message):
        try:
            message_bytes = base64.b64decode(message)
            dict_message = self.y_finance_data_proto_parser.parse(message_bytes).to_json()
            self.producer.produce('data_stream', value=dict_message.encode('utf-8'))
            self.producer.flush()
            if len(self.example_messages) <= 3: 
                self.example_messages.append(dict_message)
            self.message_count += 1
        except Exception as e:
            logging.error("Error producing Kafka message: %s", e)

    # WebSocket error callback function    
    def on_error(self, ws, error):
        logging.error(f"WebSocket error: %s", error)


if __name__ == '__main__':
    StockPricingDataSocket().run()

