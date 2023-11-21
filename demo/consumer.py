from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'foo'
}

# Memgraph configuration
mg_config = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'neo4j',
}

# Kafka consumer
def consume_message(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        return msg.value().decode('utf-8')

# Memgraph insertion
def insert_into_memgraph(uri, user, password, message):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        session.run("CREATE (m:Message {content: $content})", content=message)

# Main function
def main():
    topic = 'demo_topic'

    consumer = Consumer(kafka_config)

    # Consume the message from Kafka
    consumed_message = consume_message(consumer, topic)
    print(f"Consumed message from {topic}: {consumed_message}")

    # Insert the message into Memgraph
    insert_into_memgraph(mg_config['uri'], mg_config['user'], mg_config['password'], consumed_message)
    print(f"Inserted message into Memgraph: {consumed_message}")

    # Close connections
    consumer.close()

if __name__ == "__main__":
    main()