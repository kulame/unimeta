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


@async_adapter
async def test_meta() -> None:
    meta = sqlalchemy.MetaData()
    database_url = 'mysql://root:111111@127.0.0.1:3306/employees'
    engine = sqlalchemy.create_engine(database_url)
    meta.reflect(bind=engine)
    debug(meta.tables)
    async with ClientSession() as s:
        chclient = ChClient(s)
        for sql_table_name, sql_table in meta.tables.items():
            table = Table.read_from_sqltable(sql_table,"employee")
            ddl = table.get_ch_ddl()
            table.mock_insert()