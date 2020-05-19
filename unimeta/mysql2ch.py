import configparser
config = configparser.ConfigParser()
config.read(".env")
import asyncio
from unimeta.pipeline import MysqlSource, ClickHouseSink, KafkaSink, Pipeline
from devtools import debug
from unimeta.pipeline import MetaServer
import uvloop

async def sync() -> None:
    source_url = config['source'].get('url')
    sink_url = config['sink'].get('url')
    meta_url = config['meta'].get('url')
    pipe = Pipeline(source_url, sink_url, meta_url)
    await pipe.start()
    pipe.sync_tables()
    await pipe.sync()

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(sync())