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
import sys
import asyncio
import json
import random
from loguru import logger
import requests
from concurrent.futures import ProcessPoolExecutor
import aiohttp
from aiokafka import AIOKafkaProducer
from aioch import Client


class Sink():
    
    def publish(self):
        pass

class Source():

    def scan(self):
        pass

    def subscribe(self):
        pass


class MysqlSource(Source):
    metatable:Dict[str,Table]
    stream: BinLogStreamReader

    def __init__(self,database_url,server_id:int=None):
        if server_id is None:
            server_id = random.randint(1,1000)
        Source.__init__(self)
        settings = parse_url(database_url)
        settings['db'] = settings['name']
        del settings['name']
        del settings['scheme']
        self.metatable = Table.metadata(database_url)
        self.stream = BinLogStreamReader(connection_settings = settings, 
                                         server_id=server_id,
                                         blocking=True,
                                         only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
  

    async def subscribe(self):
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
                logger.info(event)
                if event is None:
                    continue
                yield event

    def close(self):
        self.stream.close()



class ClickHouseSink(Sink):
    
    def __init__(self, database_url):
        Source.__init__(self)
        self.client = Client.from_url(database_url)

    async def start(self):
        pass

    async def execute(self, query):
        self.client.execute(query)
    
    async def publish(self, event):
        tpl = """ INSERT INTO {table_name}
            ({columns})
            VALUES
        """
        columns = [column.name for column in event.table.columns]
        sql = tpl.format(table_name="{db}.{table}".format(db=event.table.db_name, table=event.table.name),
                   columns=",".join(columns))
        try:
            await self.client.execute(sql,[event.data])
        except:
            logger.error(event.data)
            logger.exception("what?")
        #logger.success("publish {event}　success".format(event=str(event)))


def delivery_report(err, msg):
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.warning('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class KafkaSink(Sink):

    def __init__(self, database_url):
        Source.__init__(self)
        settings = parse_url(database_url)
        loop = asyncio.get_event_loop()
        self.producer = AIOKafkaProducer(
            loop=loop,
            bootstrap_servers = '{host}:{port}'.format(host=settings['host'],port=settings['port']),
            
        )
        self.producer.start()
        self.topic = settings['name']
        debug(self.producer)

    async def start(self):
        await self.producer.start()
   
    def execute(self, query):
        pass

    async def publish(self, event):
        data = event.json()
        await self.producer.send(self.topic, data.encode('utf-8'))

        #logger.success("publish {event}　success".format(event=str(event)))



class MetaServer():
    name:str
    metaserver:str
    tables:dict

    def __init__(self,metaserver):
        self.meta = parse_url(metaserver)
        self.tables = {}
    
    async def reg(self,event):
        if event.name in self.tables:
            return 
        port = self.meta['port']
        if port is None:
            port = 80
        meta_url = "http://{host}:{port}/api/meta/events".format(
            host=self.meta['host'],
            port=self.meta['port'])
        data = {
            "name":event.name,
            "meta":event.avro(),
            "producer":self.meta['user']
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(meta_url, json=data) as response:
                self.tables[event.name] = event.table 



def actor_done(r):
    logger.success('actor done')

class Pipeline():
    sink:Sink
    source:Source
    meta:dict
    metaserver:str
    
    def __init__(self,source:str, sink:str, meta:str):
        sconf = parse_url(source)
        if sconf['scheme'] == 'mysql':
            self.source = MysqlSource(source)
        else:
            raise Exception("unregister source")
        
        dconf = parse_url(sink)
        if dconf['scheme'] == 'clickhouse':
            self.sink = ClickHouseSink(sink)
        elif dconf['scheme'] == 'kafka':
            self.sink = KafkaSink(sink)
        else:
            raise Exception("unregister sink")
        
        mconf = parse_url(meta)
        if mconf['scheme'] == 'unimetad':
            self.metaserver = MetaServer(meta)
        else:
            raise Exception("unregister meta")

    def sync_tables(self):
        tables = self.source.metatable.values()
        for table in tables:
            ddl = table.get_ch_ddl()
            if ddl is not None:
                self.sink.execute(ddl)

    def rebuild_table(self, table):
        source_tables = self.source.metatable.values()
        for item in source_tables:
            if table == item.name:
                delete_stmt = "drop table {db}.{table}".format(db=item.db_name,table=table)
                create_stmt = item.get_ch_ddl()
                self.sink.execute(delete_stmt)
                self.sink.execute(create_stmt)


    async def sync(self):
        await self.sink.start()
        async for event in self.source.subscribe():
            await self.metaserver.reg(event)
            await self.sink.publish(event)
