import configparser
config = configparser.ConfigParser()
config.read(".env")
import asyncio
from unimeta.pipeline import MysqlSource, ClickHouseSink, Pipeline
from devtools import debug


def sync() -> None:
    mysql_url = config['mysql'].get('url')
    clickhouse_url = config['clickhouse'].get('url')
    source = MysqlSource(database_url=mysql_url)
    sink = ClickHouseSink(database_url = clickhouse_url)
    pipe = Pipeline(source, sink)
    pipe.sync_tables()
    pipe.sync()

if __name__ == "__main__":
    sync()