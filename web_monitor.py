"""
Runner to monitor website for every 30 mints and generate metrics.

Author: Santhosh Balasa
Email: santhosh.kbr@gmail.com
Date: 17/May/2021
"""

import sys
import time
import sched
import click
import logging
import datetime

from urllib import request, error

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Global
HTML_FILE_NAME = "python.html"
TIME_INTERVAL = 30  # In minutes


def fetch_html(url_link, s):
    """
    Function to check the website and generate html file.

    Args:
        url_link (str): Website name to be monitored
        s (sched.scheduler): scheduler object
    """
    try:
        start = datetime.datetime.now()
        url = request.urlopen(f"http://{url_link}")
        end = datetime.datetime.now()
        delta = end - start
    except error.HTTPError as e:
        logging.error(f"HTTP Error: {e.code}")
    except error.URLError as e:
        logging.error(f"URL Error: {e.reason}")
        logging.info(f"Please verify the URL, Exiting...")
        sys.exit(-1)
    else:
        logging.info(f"Connection Successful, Status: {url.status}")
        logging.info(f"Elapsed Time: {round(delta.microseconds * .000001, 6)}s")
        if url.status == 200:
            with open(HTML_FILE_NAME, "w") as f:
                for i in url.readlines():
                    f.write(i.decode("utf-8"))
            logging.info(f"Html File is successfully created: {HTML_FILE_NAME}")
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
