from kafka import KafkaConsumer
from json import loads
import os
import json


data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" 
)
data_stream_consumer.subscribe(topics=["aicore_topic"])

for message in data_stream_consumer:
    os.chdir("/home/awoyelu12/GitHub/pinterest-data-processing-pipeline")
    os.chdir(os.getcwd () + '/batch_data')
    with open(os.getcwd () + '/data.json','w') as f:
        json.dump(message,f)
