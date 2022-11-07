from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer("aicore_topic")

for msg in consumer:
    print(msg)