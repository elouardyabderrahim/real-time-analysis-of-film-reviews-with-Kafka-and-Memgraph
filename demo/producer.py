# from confluent_kafka import Producer, KafkaError
# from neo4j import GraphDatabase

# # Kafka configuration
# kafka_config = {
#     'bootstrap.servers': 'localhost:9092'
# }

# # Kafka producer
# def produce_message(producer, topic, message):
#     producer.produce(topic, value=message)
#     producer.flush()

# # Main function
# def main():
#     topic = 'demo_topic'
#     message_to_send = 'Hello, Kafka and Memgraph!'

#     producer = Producer(kafka_config)

#     # Produce a message to Kafka
#     produce_message(producer, topic, message_to_send)
#     print(f"Produced message to {topic}: {message_to_send}")

# if __name__ == "__main__":
#     main()


import json
import time
import requests
from confluent_kafka import Producer

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

# Kafka producer
def produce_message(producer, topic, message):
    producer.produce(topic, value=message)
    producer.flush()

# Main function
def main():
    topic = 'demo_topic'

    producer = Producer(kafka_config)

    api_url = 'http://127.0.0.1:5000/api/movies'

    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()

        for movie in data:
            message_to_send = json.dumps(movie)

            # Produce a message to Kafka
            produce_message(producer, topic, message_to_send)
            print(f"Produced message to {topic}: {message_to_send}")

            time.sleep(2)

if __name__ == "__main__":
    main()