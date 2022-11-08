from kafka import KafkaConsumer
from json import loads

data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)
data_stream_consumer.subscribe(topics=["aicore_topic"])