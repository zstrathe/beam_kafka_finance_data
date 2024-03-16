source venv/bin/activate
yes | confluent local kafka start --plaintext-ports="41239"
echo "Waiting 5 seconds before creating topic..."
sleep 5
confluent local kafka topic create data_stream
echo "Starting Producer from websocket data..."
python3 websocket_kafka_bridge/bridge.py
sleep 5
