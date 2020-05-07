from pymysqlreplication import BinLogStreamReader
from unimeta.libs.liburl import parse_url
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from typing import Dict
from devtools import debug
from unimeta.event import Event, EventType
from unimeta.table import Table
from clickhouse_driver import Client

import asyncio
import json
from confluent_kafka import Producer
from loguru import logger

class Sink():
    
    def publish(self):
        pass

class Source():
    
    def subscribe(self):
        pass


class MysqlSource(Source):
    metatable:Dict[str,Table]
    stream: BinLogStreamReader

    def __init__(self,database_url):
        Source.__init__(self)
        settings = parse_url(database_url)
        settings['db'] = settings['name']
        del settings['name']
        self.metatable = Table.metadata(database_url)
        self.stream = BinLogStreamReader(connection_settings = settings, 
                                         server_id=100,
                                         blocking=True,
                                         only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
  

    def subscribe(self):
        for binlogevent in self.stream:
            key = "{db}/{table}".format(db=binlogevent.schema, table=binlogevent.table)
            table = self.metatable.get(key)
            if table is None:
                continue
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    event = Event.parse_binlog(table,EventType.DELETE,row)
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event = Event.parse_binlog(table,EventType.UPDATE,row)
                elif isinstance(binlogevent, WriteRowsEvent):
                    event = Event.parse_binlog(table,EventType.INSERT,row)
                else:
                    raise Exception("event type not support")
                debug(event)
                if event is None:
                    continue
                yield event

    def close(self):
        self.stream.close()



class ClickHouseSink(Sink):
    
    def __init__(self, database_url):
        Source.__init__(self)
        settings = parse_url(database_url)
        print(settings)
        self.ch = Client(host=settings['host'],
                         port=settings['port'],
                         database=settings['name'])
    
    def execute(self, query):
        return self.ch.execute(query)
    
    def publish(self, event):
        return event.insert_ch(self.ch)


def delivery_report(err, msg):
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.warning('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class KafkaSink(Sink):

    def __init__(self, database_url):
        Source.__init__(self)
        settings = parse_url(database_url)
        self.producer = Producer({
            'bootstrap.servers': '{host}:{port}'.format(host=settings['host'],port=settings['port'])
        })
        self.topic = settings['name']
        debug(self.producer)

    def execute(self, query):
        pass

    def publish(self, event):
        data = event.json()
        debug(data)
        self.producer.produce(self.topic, data.encode('utf-8'), callback=delivery_report)


class Pipeline():
    sink:Sink
    source:Source
    
    def __init__(self,source, sink):
        self.source = source
        self.sink = sink

    def sync_tables(self):
        tables = self.source.metatable.values()
        for table in tables:
            ddl = table.get_ch_ddl()
            if ddl is not None:
                self.sink.execute(ddl)
    
    def sync(self):
        for event in self.source.subscribe():
            self.sink.publish(event)


