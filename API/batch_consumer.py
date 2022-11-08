import boto3
import credentials
from botocore.client import Config
import json
from json import loads
from kafka import KafkaConsumer
import os

ACCESS_SECRET_KEY = credentials.AWSSecretKey
ACCESS_KEY_ID = credentials.AWSAccessKeyId
bucketname = 'pintrest-data-b5d598e8-12fd-488f-8e22-215f5936044'


s3 = boto3.resource(
    's3',
    aws_access_key_id = ACCESS_KEY_ID,
    aws_secret_access_key = ACCESS_SECRET_KEY,
    config = Config(signature_version = 's3v4'))



data_stream_consumer = KafkaConsumer(
    "aicore_topic",
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message.decode("utf-8")),
    auto_offset_reset="earliest" 
)


def load_to_S3():
    for _,message in enumerate(data_stream_consumer):
        message = message.value
        
        s3obj = s3.Object(bucketname,f"{bucketname}/message{_}.json")
        s3obj.put(Body=(bytes(json.dumps(message).encode("utf-8"))))

load_to_S3()






