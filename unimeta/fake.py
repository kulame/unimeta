from sqlalchemy import *
from databases import Database
import aiomysql
import sqlalchemy
from sqlalchemy.engine import reflection
import pytest
import functools
import asyncio
from devtools import debug
from unimeta.table import Table
from aiochclient import ChClient
from aiohttp import ClientSession
from loguru import logger
import configparser
from unimeta.libs.liburl import parse_url
from loguru import logger
import pymysql
import time

config = configparser.ConfigParser()
config.read(".env")
  
async def fake() -> None:
    meta = sqlalchemy.MetaData()
    database_url = config['mysql'].get("url")
    debug(database_url)

    engine = sqlalchemy.create_engine(database_url)
    meta.reflect(bind=engine)
    sqltable = meta.tables['employees']
    debug(sqltable)
    table = Table.read_from_sqltable(sqltable,"employee")
    debug(table)
    hint = {
        "email":"ascii_free_email",
        "phone_number":"phone_number",
        "first_name":"first_name",
        "last_name":"last_name"
    }
    async with Database(database_url) as database:
        while True:
            try:
                primary_id = await table.mock_insert(database, hint)
                time.sleep(1)
                await table.mock_update(database, hint, primary_id)
            except pymysql.err.DataError:
                logger.exception("what")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(fake())
    loop.close()