import json
import random
import time
from datetime import datetime
from google.cloud import pubsub_v1
import os

project = 'PROJECT_ID'
pubsub_topic = 'TOPIC_NAME'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="PATH_TO_THE_JSON_FILE"

publisher = pubsub_v1.PublisherClient()


def generate_message():
    while True:       
        message = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "store_id": random.randint(1, 8),
            "green_salad": random.randint(0, 50),
            "greek_salad": random.randint(0, 50),
            "spinach_salad": random.randint(0, 50),
            "caesar_salad": random.randint(0, 50),
            "waldorf_salad": random.randint(0, 50),
            "nicoise_salad": random.randint(0, 50),
        }
        row = json.dumps(message)
        publisher.publish(pubsub_topic, row)
        time.sleep(random.randint(30, 900))

generate_message()