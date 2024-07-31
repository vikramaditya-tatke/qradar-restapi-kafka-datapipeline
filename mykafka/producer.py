from confluent_kafka import Producer
from settings import settings
from pipeline_logger import logger
import os


def create_producer():
    try:
        producer = Producer(
            {
                "bootstrap.servers": "",
                "sasl.username": "",
                "sasl.password": "",
                "sasl.mechanism": "PLAIN",
                "security.protocol": "SASL_PLAINTEXT",
            }
        )
        return producer
    except Exception as e:
        print(e)
