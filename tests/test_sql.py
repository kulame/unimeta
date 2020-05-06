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

config = configparser.ConfigParser()
config.read(".env")


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.new_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


def test_parse() -> None:
    database_url = config['mysql'].get("url")
    db = parse_url(database_url)
    debug(db)

@async_adapter
async def test_mock() -> None:
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
        id = await table.mock_insert(database, hint)
        update = await table.mock_update(database, hint, id)
        debug(update)


@async_adapter
async def test_meta() -> None:
    meta = sqlalchemy.MetaData()
    database_url = 'mysql://root:111111@127.0.0.1:3306/hr'
    engine = sqlalchemy.create_engine(database_url)
    meta.reflect(bind=engine)
    for sql_table_name, sql_table in meta.tables.items():
        table = Table.read_from_sqltable(sql_table,"employee")
        ddl = table.get_ch_ddl()