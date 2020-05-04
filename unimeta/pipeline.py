from pymysqlreplication import BinLogStreamReader
from unimeta.libs.liburl import parse_url
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from devtools import debug
from unimeta.event import Event, EventType
from unimeta.table import Table
class Sink():
    
    def publish(self):
        pass

class Source():
    
    def subscribe(self):
        pass


class MysqlSource(Source):
    
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
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    event = Event.parse_binlog(table,EventType.DELETE,row)
                    debug(event)
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event = Event.parse_binlog(table,EventType.UPDATE,row)
                    debug(event)
                elif isinstance(binlogevent, WriteRowsEvent):
                    event = Event.parse_binlog(table,EventType.INSERT,row)
                    debug(event)

    def close(self):
        self.stream.close()

class MysqlSink(Sink):

    def __init__(self,database_url):
        Sink.__init__(self)
        self.database = Database(database_url)
    
    def generate_fake_data(self,sql,data):
        self.database.execute(query=sql,values=data)


class ClickHouseSink(Sink):
    pass

class Pipeline():
    pass