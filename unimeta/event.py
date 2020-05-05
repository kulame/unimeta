from __future__ import annotations
from pydantic.dataclasses import dataclass
from pydantic import BaseModel,BaseConfig, Json, create_model
from asyncio import BaseEventLoop
from enum import Enum, auto
from typing import List, Optional
import uuid
from aiokafka import AIOKafkaProducer
from loguru import logger
import inspect
from devtools import debug
import pydantic
import json
from unimeta.table import Table, DDLTemplate
from unimeta.libs.libformat import jsonity


class EventType(Enum):
    INSERT = auto()  
    UPDATE = auto()
    DELETE = auto()

class TopicType(Enum):
    FULL = 1
    LATEST = 2


class Context(BaseModel):
    trace_id: str
    span_id: str
    device_id: str
    user_id: int

class Config(BaseConfig):
    arbitrary_types_allowed = True

def none_field(item):
    return {k: (...,v) for k, v in item.items()}


class Meta(BaseModel):

    db_name:str
    table_name:str

class Event():
    type: EventType
    name: str
    version: str = '0'
    id: str
    table: Table
    data: dict = None
    ctx: Context = None

    def __init__(self, event_type:EventType,name:str,table:Table,data:dict,ctx:Context=None):
        self.id = uuid.uuid1().hex
        self.type = event_type
        self.name = name
        self.data = data
        self.ctx = ctx
        self.table = table


    @classmethod 
    def parse_binlog(cls,table:Table, event_type:EventType,raw:dict) -> Event:
        if event_type == EventType.INSERT:
            values = raw['values']
        elif event_type == EventType.UPDATE:
            values = raw['after_values']
        info = {
            'database': table.db_name,
            'table': table.name,
            'type': event_type.name
        }
        name = 'mysql://{database}/{table}/{type}'.format(**info)
        data = table.normalize(values)
        event = Event(event_type=event_type,name=name,data=data, table=table)
        return event

    def insert_ch(self,ch):
        tpl = """ INSERT INTO {table_name}
            ({columns})
            VALUES
        """
        columns = [column.name for column in self.table.columns]
        sql = tpl.format(table_name="{db}.{table}".format(db=self.table.db_name, table=self.table.name),
                   columns=",".join(columns))
        try:
            ch.execute(sql,[self.data])
        except:
            logger.exception("what?")
            raise

class Topic(BaseModel):
    name: str
    type: TopicType = TopicType.FULL
