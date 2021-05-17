import time
import sched
import click
import datetime

from urllib import request, error


HTML_FILE_NAME = "python.html"


def fetch_html(url_link, s):
    try:
        start = datetime.datetime.now()
        url = request.urlopen(f"http://{url_link}")
        end = datetime.datetime.now()
        delta = end - start
    except error.HTTPError as e:
        print(f"HTTP Error: {e.code}")
    except error.URLError as e:
        print(f"URL Error: {e.reason}")
    else:
        print(f"Connection Successful, Status: {url.status}")
        print(f"Elapsed Time: {round(delta.microseconds * .000001, 6)}s")
        if url.status == 200:
            with open(HTML_FILE_NAME, "w") as f:
                for i in url.readlines():
                    f.write(i.decode("utf-8"))
            print(f"Html File is successfully created: {HTML_FILE_NAME}")
    s.enter(
        60 * 30,
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
    s = sched.scheduler(time.time, time.sleep)
    fetch_html(url, s)
    # Run web monitoring evey 30 mints
    s.enter(
        60 * 30,
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