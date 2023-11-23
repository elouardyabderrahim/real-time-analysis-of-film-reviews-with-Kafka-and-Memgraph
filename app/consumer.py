from confluent_kafka import Consumer, KafkaError
from gqlalchemy import Memgraph, Field, Node, Relationship
import json
from datetime import datetime

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'foo'
}

# Memgraph configuration
memgraph = Memgraph("127.0.0.1", 7687)

# Removes all nodes and edges 
memgraph.drop_database()

# Define the graph schema
class User(Node):
    userId: int = Field(index=True, unique=True, db=memgraph)

class Movie(Node):
    movieId: int = Field(index=True, unique=True, db=memgraph)
    title: str = Field(index=True, unique=True, db=memgraph)

class Genre(Node):
    name: str = Field(index=True, unique=True, db=memgraph)

class Rated(Relationship):
    rating: float = Field(index=True, unique=True, db=memgraph)
    timestamp: int = Field(index=True, unique=True, db=memgraph)

class IsGenre(Relationship):
    pass

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

        try:
            # Decode the message value to a JSON object
            json_document = json.loads(msg.value().decode('utf-8'))
            return json_document
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

# Main function
def main():
    topic = 'demo_topic'

    consumer = Consumer(kafka_config)

    
    while True:

        # Consume the message from Kafka
        consumed_message = consume_message(consumer, topic)
        print(f"Consumed message from {topic}: {consumed_message}")

        # Memgraph process

        # User node
        userId = int(consumed_message['userId'])
        user = User(userId=userId)
        user.save(memgraph)

        # Movie node
        movieId = int(consumed_message['movie']['movieId'])
        title = consumed_message['movie']['title']
        movie = Movie(movieId=movieId, title=title)
        movie.save(memgraph)

        # Rated relationship
        rating = consumed_message['rating']
        timestamp = int(consumed_message['timestamp'])
        rated = Rated(rating=rating, timestamp=timestamp, _start_node_id=user._id, _end_node_id=movie._id)
        rated.save(memgraph)

        # Genres Node
        genre_names = consumed_message['movie']['genres']
        for genre_name in genre_names:
            genre = Genre(name=genre_name)
            genre.save(memgraph)

            # IsGenre relationship
            isGenre = IsGenre(_start_node_id=movie._id, _end_node_id=genre._id)
            isGenre.save(memgraph)


        # Insert the message into Memgraph
        print(f"Inserted message into Memgraph")

        # Close connections
    consumer.close()

if __name__ == "__main__":
    main()