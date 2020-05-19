
from unimeta.pipeline import KafkaSink
import configparser
import asyncio

import pytest


config = configparser.ConfigParser()
config.read(".env")

def test_kafkasink() -> None:
    database_url = config['sink'].get("url")
    sink = KafkaSink(database_url)
    