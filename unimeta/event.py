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
from unimeta.table import Table


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

class Event(BaseModel):
    type: EventType
    name: str
    version: str = '0'
    id: str
    meta: Meta
    data: dict = None
    ctx: Context = None



    @classmethod 
    def parse_binlog(cls,table:Table, event_type:EventType,raw:dict) -> Event:
        values = raw['values']
        info = {
            'database': table.db_name,
            'table': table.name,
            'type': event_type.name
        }
        events = []
        name = 'mysql://{database}/{table}/{type}'.format(**info)
        
        meta = Meta(db_name=table.db_name, table_name=table.name)
        event = Event(id = uuid.uuid1().hex, type=event_type,name=name,data=values, meta=meta)
        return event




class Topic(BaseModel):
    name: str
    type: TopicType = TopicType.FULL
