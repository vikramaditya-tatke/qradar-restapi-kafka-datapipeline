from confluent_kafka import Producer
from settings import settings
from pipeline_logger import logger
import os


def create_producer():
    try:
        producer = Producer(
            {
                "bootstrap.servers": "pkc-41wq6.eu-west-2.aws.confluent.cloud:9092",
                "sasl.username": "DUWYAFC3JIUQYTVF",
                "sasl.password": "/NuBru/OSbaqTLjprp3D1V4DAlMmdCMu8WCYT1w9OYNcySC5YMGAZdcLRh8BjEtq",
                "sasl.mechanism": "PLAIN",
                "security.protocol": "SASL_SSL",
            }
        )
        return producer
    except Exception as e:
        print(e)
