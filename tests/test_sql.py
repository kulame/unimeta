from sqlalchemy import *
from databases import Database
import pytest
import functools
import asyncio
from devtools import debug


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
    async with Database('mysql://127.0.0.1:3306/hr') as database:
        debug(database)
    