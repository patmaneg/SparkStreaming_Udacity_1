from kafka import KafkaProducer
import json
import time

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file) as f:
            for line in f:
                message = self.json.dumps(line).encode('utf-8')
                self.send(self.topic, message)
                time.sleep(1)

## Arrancar
"""
if __name__ == "__main__":
    file = '/home/workspace/data/uber.json'
    topic = 'mitopico'
    
    ProducerServer().generate_data(input_file = file, topic = mitopico)
"""
