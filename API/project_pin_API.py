from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pintrest data producer",
    value_serializer=lambda pintrestmessage: dumps(pintrestmessage).encode("ascii"))



@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send("aicore_topic",data)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
