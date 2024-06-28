from confluent_kafka import Producer
from settings import settings


def create_producer():
    kafka_config = {"bootstrap.servers": f"{settings.kafka_host}:{settings.kafka_port}"}
    producer = Producer(kafka_config)
    return producer
