#!/usr/bin/env python3
"""
Database Aggregator from a Kafka Consumer.

Author: Santhosh Balasa
Email: santhosh.kbr@gmail.com
Date: 18/May/2021
"""

import sys
import logging
import psycopg2

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
DATABASE_NAME = "metrics_aggregator"
SERVICE_URI = f"postgres://avnadmin:caerdfvhm59zfn7b@pg-1f19cc97-santee-fabb.aivencloud.com:12057/{DATABASE_NAME}?sslmode=require"


# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSRAP_SERVER,
    security_protocol="SSL",
    ssl_cafile="kafkaCerts/ca.pem",
    ssl_certfile="kafkaCerts/service.cert",
    ssl_keyfile="kafkaCerts/service.key",
)

# PostgreSQL
try:
    db_conn = psycopg2.connect(SERVICE_URI)
    cursor = db_conn.cursor()
    cursor.execute("SELECT current_database()")
    result = cursor.fetchone()
    logger.info(f"Successfully connected to Database: {result[0]}")
except:
    logger.error(f"Failed to connect Database: {DATABASE_NAME}")
    sys.exit(-1)

# SQL Tables
cursor.execute(
    """CREATE TABLE KEYS(
        ID INT PRIMARY KEY NOT NULL,
        DATETIME TEXT NOT NULL
    );"""
)
cursor.execute(
    """CREATE TABLE VALUES(
        ID INT PRIMARY KEY NOT NULL,
        URL TEXT NOT NULL,
        STATUS TEXT NOT NULL,
        ELAPSED_TIME DOUBLE PRECISION NOT NULL
    );"""
)


def main():
    """
    Main function to consume from Kafka topic and aggregate it to Postgres SQL.
    """
    logger.info("Connecting to Aiven PostgreSQL...")
    logger.info("Kafka Consumption Begins...")
    key_id = 1
    for c in consumer:
        print(
            c.key.decode("utf-8"),
            "->",
            c.value.decode("utf-8"),
        )
        key = eval(c.key.decode("utf-8"))["time"]  # Evaluate str to a dict
        values = eval(c.value.decode("utf-8"))
        url = values.get("url", "")
        status = values.get("status", "")
        elapsed_time = values.get("elapsed_time", 0)
        cursor.execute(
            f"""INSERT INTO KEYS (ID, DATETIME) \
                VALUES ({key_id}, '{key}');"""
        )
        cursor.execute(
            f"""INSERT INTO VALUES (ID, URL, STATUS, ELAPSED_TIME) \
                VALUES ({key_id}, '{url}', '{status}', {elapsed_time});"""
        )
        cursor.execute("""SELECT * FROM VALUES""")
        logger.info(cursor.fetchall())
        key_id += 1
    consumer.close()


if __name__ == "__main__":
    main()
