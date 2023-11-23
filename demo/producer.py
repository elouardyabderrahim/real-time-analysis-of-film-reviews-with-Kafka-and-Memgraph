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
    message_to_send = 'Hello, Kafka and Memgraph Abdee!'

    producer = Producer(kafka_config)

    # Produce a message to Kafka
    produce_message(producer, topic, message_to_send)
    print(f"Produced message to {topic}: {message_to_send}")

if __name__ == "__main__":
    main()