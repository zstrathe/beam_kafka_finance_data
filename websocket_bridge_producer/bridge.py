import logging
import threading
import time
import json
import socket
import websocket
import base64
from confluent_kafka import Producer
from yfindata import PricingData


logging.basicConfig(level=logging.INFO)


class OpenTheSocket:
    # Kafka configuration
    kafka_conf = {'bootstrap.servers': 'broker:29092'  #'host.docker.internal:41239'
                #   'client.id': socket.gethostname()
                  }
    y_finance_data_proto_parser = PricingData()

    def __init__(self, socket_url:str|None=None):
       
        self.producer = Producer(self.kafka_conf)
        
        # start the websocket connection thread
        if not socket_url:
            self.open_socket_connection()
        else: 
            self.open_socket_connection(socket_url)

        # # start the connection monitor thread
        # self.monitor_connection()
     
    # WebSocket message callback function
    def on_message(self, ws, message):
        try:
            # message_bytes = base64.b64decode(message)
            # dict_message = self.y_finance_data_proto_parser.parse(message_bytes).to_json()
            # print('message type:', type(dict_message), '\n', dict_message, '\n')
            # print(f'Received a message from websocket: {message}')
            # self.producer.produce('data_stream', value=dict_message.encode('utf-8'))
            self.producer.produce('data_stream', value=message)
            self.producer.flush() 
        except Exception as e:
            logging.error(f"Error producing Kafka message: {e}")


    # WebSocket error callback function    
    def on_error(self, ws, error):
        logging.error(f"WebSocket error: {error}")
        time.sleep(2)
        self.open_socket_connection()


    def on_open(self, ws):
        ticker_symbols = ['AAPL', 'AMZN', 'NVDA']
        _req = {"subscribe": ticker_symbols}
        print('Sending request for data:', _req)
        ws.send(json.dumps(_req))

    def open_socket_connection(self, socket_url='wss://streamer.finance.yahoo.com/'):
        self.ws = websocket.WebSocketApp(socket_url,
                                on_message=self.on_message,
                                on_error=self.on_error,
                                #on_open=self.on_open
                                )
        ws_thread = threading.Thread(target=self.ws.run_forever)
        ws_thread.start()


    # def monitor_connection(self):
    #     def _mon_func():
    #         while True:
    #             if hasattr(self.ws.sock, 'connected') and self.ws.sock.connected:
    #                 pass
    #             else:
    #                 self.open_socket_connection()
    #             time.sleep(5)  # Check every 5 seconds
    #     # add an initial delay when starting monitor thread, so that socket has time to open
    #     time.sleep(3)
    #     print('STARTING CONNECTION MONITOR...')
    #     mon_thread = threading.Thread(target=_mon_func())
    #     mon_thread.start()

  
if __name__ == '__main__':
    OpenTheSocket('wss://api.gemini.com/v1/marketdata/BTCUSD') # 'wss://api.gemini.com/v1/marketdata/BTCUSD'

