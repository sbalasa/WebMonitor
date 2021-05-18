#!/usr/bin/env python3
"""
Database Aggregator from a Kafka Consumer.

Author: Santhosh Balasa
Email: santhosh.kbr@gmail.com
Date: 18/May/2021
"""

import logging

from kafka import KafkaConsumer

logging.basicConfig(
    format=f"%(asctime)s %(name)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# Global
BOOTSRAP_SERVER = "kafka-48ac8c2-santee-fabb.aivencloud.com:12059"
KAFKA_TOPIC = "website_checker"


# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSRAP_SERVER,
    security_protocol="SSL",
    ssl_cafile="kafkaCerts/ca.pem",
    ssl_certfile="kafkaCerts/service.cert",
    ssl_keyfile="kafkaCerts/service.key",
)


def main():
    """
    Main function to consume from Kafka topic and aggregate it to Postgres SQL.
    """
    logger.info("Kafka Consumption Begins...")
    for c in consumer:
        print(c.key.decode("utf-8"), "->", c.value.decode("utf-8"))
    consumer.close()


if __name__ == "__main__":
    main()
