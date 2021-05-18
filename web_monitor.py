"""
Runner to monitor website for every 30 mints and generate metrics.

Author: Santhosh Balasa
Email: santhosh.kbr@gmail.com
Date: 17/May/2021
"""

import sys
import time
import json
import sched
import click
import logging

from datetime import datetime
from kafka import KafkaProducer
from urllib import request, error

logging.basicConfig(
    format=f"%(asctime)s %(name)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Global
HTML_FILE_NAME = "python.html"
TIME_INTERVAL = 1  # In minutes
BOOTSRAP_SERVER = "kafka-48ac8c2-santee-fabb.aivencloud.com:12059"
KAFKA_TOPIC = "website_checker"


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSRAP_SERVER,
    security_protocol="SSL",
    ssl_cafile="kafkaCerts/ca.pem",
    ssl_certfile="kafkaCerts/service.cert",
    ssl_keyfile="kafkaCerts/service.key",
    value_serializer=lambda v: json.dumps(v).encode("ascii"),
    key_serializer=lambda v: json.dumps(v).encode("ascii"),
)


def fetch_html(url_link, s):
    """
    Function to check the website and generate html file.

    Args:
        url_link (str): Website name to be monitored
        s (sched.scheduler): scheduler object
    """
    try:
        start = datetime.now()
        url = request.urlopen(f"http://{url_link}")
        end = datetime.now()
        delta = end - start
    except error.HTTPError as e:
        logger.error(f"HTTP Error: {e.code}")
    except error.URLError as e:
        logger.error(f"URL Error: {e.reason}")
        logger.info(f"Please verify the URL, Exiting...")
        sys.exit(-1)
    else:
        logger.info(f"Connection Successful, Status: {url.status}")
        elapsed_time = round(delta.microseconds * 0.000001, 6)
        logger.info(f"Elapsed Time: {elapsed_time}s")
        if url.status == 200:
            with open(HTML_FILE_NAME, "w") as f:
                for i in url.readlines():
                    f.write(i.decode("utf-8"))
            logger.info(f"Html File is successfully created: {HTML_FILE_NAME}")
        producer.send(
            KAFKA_TOPIC,
            key={"time": str(datetime.now())},
            value={
                "url": url_link,
                "status": url.status,
                "elapsed_time": elapsed_time,
            },
        )

    s.enter(
        60 * TIME_INTERVAL,
        1,
        fetch_html,
        (
            url_link,
            s,
        ),
    )


@click.command()
@click.option(
    "--url",
    required=True,
    help="Pass the URL to be monitored every 30 mints",
)
def main(url):
    """
    WBT: Website Monitoring Tool
        This tool is used to monitor a website for every 30 mints and generate metrics.
    Args:
        url (str): Website name to be monitored
    """
    s = sched.scheduler(time.time, time.sleep)
    fetch_html(url, s)
    # Run web monitoring evey 30 mints
    s.enter(
        60 * TIME_INTERVAL,
        1,
        fetch_html,
        (
            url,
            s,
        ),
    )
    s.run()


if __name__ == "__main__":
    main()
