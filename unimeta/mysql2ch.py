import configparser
config = configparser.ConfigParser()
config.read(".env")
import asyncio
from unimeta.pipeline import MysqlSource, ClickHouseSink, KafkaSink, Pipeline
from devtools import debug


def sync() -> None:
    mysql_url = config['mysql'].get('url')
    kafka_url = config['kafka'].get('url')
    source = MysqlSource(database_url=mysql_url)
    sink = KafkaSink(database_url = kafka_url)
    pipe = Pipeline(source, sink)
    pipe.rebuild_table('employees')
    #pipe.sync()

if __name__ == "__main__":
    sync()