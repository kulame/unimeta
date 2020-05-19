from __future__ import annotations
from pydantic.dataclasses import dataclass
from pydantic import BaseModel,BaseConfig, Json, create_model
from asyncio import BaseEventLoop
from enum import IntEnum, auto
from typing import List, Optional
import uuid
from loguru import logger
import inspect
from devtools import debug
import pydantic
import json
import requests
from unimeta.table import Table, DDLTemplate
from unimeta.libs.libformat import jsonity
from unimeta.model import MetaEventReq

from aiokafka.structs import ConsumerRecord

class EventType(IntEnum):
    INSERT = 1 
    UPDATE = 2
    DELETE = 3
    UPSERT = 4
class TopicType(IntEnum):
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

    def __init__(self, id, event_type:EventType,name:str,table:Table,data:dict,ctx:Context=None,version=0):
        self.id = id 
        self.type = event_type
        self.name = name
        self.data = data
        self.ctx = ctx
        self.table = table
        self.version = version

    @classmethod 
    def parse_binlog(cls,table:Table, event_type:EventType,raw:dict) -> Event:
        if event_type == EventType.INSERT:
            values = raw['values']
        elif event_type == EventType.UPDATE:
            values = raw['after_values']
        elif event_type == EventType.DELETE:
            logger.warning("delete event")
            return  None
        info = {
            'database': table.db_name,
            'table': table.name,
            'type': event_type
        }
        name = 'mysql://{database}/{table}/{type}'.format(**info)
        data = table.normalize(values)
        id = uuid.uuid1().hex 
        event = Event(id=id,event_type=event_type,name=name,data=data, table=table)
        return event
    

    def json(self) -> str:
        return json.dumps({
            'type': self.type,
            'name': self.name,
            'version': self.version,
            'id': self.id,
            'data': jsonity(self.data),
            'ctx': jsonity(self.ctx) if self.ctx else None
        })

    def get_primary_key(self):
        k = self.table.primary_key.name
        return self.data[k]


    def __repr__(self) -> str:
        return "{name}/{id}".format(name=self.name,id=self.get_primary_key())
    

    def avro(self):
        _schema = {
            'name': self.name,
            'namespace': 'gengmei.event.{type}'.format(type=self.type),
            'type': 'record',
        }
        fields = []
        for column in self.table.columns:
            field = {}
            field['name'] = column.name
            field['type'] = column.avro_types()
            field['primary_key'] = column.primary_key
            fields.append(field)
        _schema['fields'] = fields
        return _schema

class Topic(BaseModel):
    name: str
    type: TopicType = TopicType.FULL
