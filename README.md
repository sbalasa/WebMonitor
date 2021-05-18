# WebMonitor
## WBT: Website Monitoring Tool
This is a tool which monitors website availability over the network for every 30 minutes, produces metrics about this and passes these events through an Aiven Kafka instance into an Aiven PostgreSQL database.

## To Prepare Environment
#### Run:
- `pip3 install -r requirements.txt`

## How to Execute
#### Start Consuming metrics from Kafka and Aggregate the Database
- `python3 db_aggregator.py`

#### Open a new terminal and run website monitoring
- `python3 web_monitor.py --url <url_link>`

## For Help
`python3 web_monitor.py --help`
