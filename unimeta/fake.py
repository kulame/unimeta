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
from loguru import logger
import configparser
from unimeta.libs.liburl import parse_url
import pymysql
import time
import random

config = configparser.ConfigParser()
config.read(".env")
  
async def fake() -> None:
    database_url = config['source'].get("url")

    meta = Table.metadata(database_url)
    debug(meta)
    hint = {
        "email":"ascii_free_email",
        "phone_number":"phone_number",
        "first_name":"first_name",
        "last_name":"last_name"
    }
    async with Database(database_url) as database:
        while True:
            try:
                keys = list(meta.keys())
                key = random.choice(keys)
                debug(key)
                table = meta[key] 
                primary_id = await table.mock_insert(database, hint)
                await table.mock_update(database, hint, primary_id)
            except pymysql.err.DataError:
                logger.exception("what")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(fake())
    loop.close()