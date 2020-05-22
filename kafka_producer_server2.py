from confluent_kafka import Consumer, Producer
from kafka import KafkaProducer
import json
import time

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "productor"

p = Producer({"bootstrap.servers": BROKER_URL})
#p2 = KafkaProducer(bootstrap_servers='localhost:9092')
file = '/home/workspace/data/uber.json'
with open(file) as f:
    for line in f:
        message = json.dumps(line).encode('utf-8')
        p.produce(TOPIC_NAME, message)
        time.sleep(1)
