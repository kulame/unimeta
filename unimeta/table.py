from __future__ import annotations
from typing import List, Dict,Optional
from sqlalchemy.sql.schema import Table as SQLTable
from sqlalchemy.sql.schema import Column as SQLColumn
import sqlalchemy
from devtools import debug
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
import json
from loguru import logger
from faker import Faker
from faker.providers import python
from databases import Database
from loguru import logger
from devtools import debug
from unimeta.libs.liburl import parse_url
from unimeta.libs.libformat import jsonity


_fake = Faker(['zh_CN'])

def fake(c:Column, hint:dict={}) -> str:
    if c.name in hint:
        f = hint[c.name]
        func = getattr(_fake,f)
        return func()
    elif isinstance(c,StringColumn):
        return _fake.pystr(max_chars=c.length)
    elif isinstance(c, TextColumn):
        return _fake.text()
    elif isinstance(c, IntegerColumn):
        return _fake.pyint()
    elif isinstance(c,DecimalColumn):
        fstr = "%%.%df" % c.scale
        left_digits = c.precision - c.scale 
        return Decimal(fstr %_fake.pyfloat(left_digits=left_digits,positive=True))
    elif isinstance(c, FloatColumn):
        return _fake.pyfloat()
    elif isinstance(c, BooleanColumn):
        return _fake.pybool()
    elif isinstance(c, DateTimeColumn):
        return _fake.date_time()
    elif isinstance(c, DateColumn):
        return _fake.date()
    elif isinstance(c, TimeColumn):
        return _fake.iso8601()
    elif isinstance(c, JSONColumn):
        return  json.dumps(_fake.pydict())
    else:
        return _fake.name()

class CHTableEngine(Enum):
    MergeTree = "MergeTree" 
    ReplacingMergeTree = "ReplacingMergeTree"
    SummingMergeTree = 'SummingMergeTree'
    AggregatingMergeTree = "AggregatingMergeTree"
    CollapsingMergeTree = "CollapsingMergeTree"
    VersionedCollapsingMergeTree = "VersionedCollapsingMergeTree"
    GraphiteMergeTree = "GraphiteMergeTree"
    TinyLog = "TinyLog"
    StripeLog = "StripeLog"
    Log = "Log"
    Kafka = "Kafka"
    MySQL = "MySQL"
    ODBC = "ODBC"
    JDBC = "JDBC"
    HDFS = "HDFS"
    Distributed = "Distributed"
    MaterializedView = "MaterializedView"
    Dictionary = "Dictionary"
    Merge = "Merge"
    File = "File"
    Null = "Null"
    Set = "Set"
    Join = "Join"
    URL = "URL"
    View = "View"
    Memory = "Memory"
    Buffer = "Buffer"



class Column:
    name:str
    nullable:bool
    autoincrement:bool = False
    primary_key:bool = False

    def to_ch_type(self) -> str:
        pass
    
    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> Column:
        column = Column()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def avro_types(cls) -> Optional[str,List[str]]:
        pass


class StringColumn(Column):
    length:int
    
    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> StringColumn:
        column = StringColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        column.length = sqlcolumn.type.length
        return column

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    
    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["string", "null"]
        else:
            return "string"




class TextColumn(Column):

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["string", "null"]
        else:
            return "string"


class IntegerColumn(Column):
    autoincrement:bool

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> IntegerColumn:
        column = IntegerColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        column.autoincrement = sqlcolumn.autoincrement
        return column

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(UInt64)"
        else:
            return "UInt64"

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["int", "null"]
        else:
            return "int"


class DecimalColumn(Column):
    precision:int
    scale:int

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> DecimalColumn:
        column = DecimalColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        column.precision = sqlcolumn.type.precision
        column.scale = sqlcolumn.type.scale
        return column

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["bytes", "null"]
        else:
            return "bytes"


class FloatColumn(Column):
    pass

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(Float32)"
        else:
            return "Float32"

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> FloatColumn:
        column = FloatColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["float", "null"]
        else:
            return "float"

class BooleanColumn(Column):
    pass

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(UInt8)"
        else:
            return "UInt8"

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> BooleanColumn:
        column = BooleanColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["boolean", "null"]
        else:
            return "boolean"


class DateTimeColumn(Column):
    pass
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(DateTime)"
        else:
            return "DateTime"

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> DateTimeColumn:
        column = DateTimeColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["int", "null"]
        else:
            return "int"



class DateColumn(Column):
    
    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> DateColumn:
        column = DateColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column
        
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(Date)"
        else:
            return "Date"

    
    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["int", "null"]
        else:
            return "int"



class TimeColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["int", "null"]
        else:
            return "int"


class JSONColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"



    def avro_types(self) -> Optional[str,List[str]]:
        if self.nullable:
            return ["string", "null"]
        else:
            return "string"




def get_column_from_sql(sqlcolumn:SQLColumn) -> Column:
    ptype = sqlcolumn.type.python_type
    if ptype is int:
        return IntegerColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is str:
        return StringColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is date:
        return DateColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is Decimal:
        return DecimalColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is datetime:
        return DateTimeColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is float:
        return FloatColumn.read_from_sqlcolumn(sqlcolumn)
    else:
        debug(sqlcolumn)

class ClickhouseTableTemplate():

    @classmethod
    def getVersionedCollapsingMergeTree(cls) -> str:
        return """
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                 Sign Int8,
                 Version UInt8,
                {create_column}
            )
            ENGINE = VersionedCollapsingMergeTree(Sign, Version)
            PARTITION BY toYYYYMM({primary_date})
            ORDER BY ({primary_key})
            PRIMARY KEY ({primary_key})
        """

    @classmethod
    def getReplacingMergeTree(cls) -> str:
        return """
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                {create_column}
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY toYYYYMM({primary_date})
            ORDER BY ({primary_key})
            PRIMARY KEY ({primary_key})
        """

    @classmethod
    def getDefault(cls) -> str:
        return """
            CREATE TABLE IF NOT EXISTS  {db_name}.{table_name}
            (
                {create_column}
            )
            ENGINE = {engine_name}()
            PARTITION BY toYYYYMM({primary_date})
            ORDER BY ({primary_key})
        """

    @classmethod
    def get(cls,engine:CHTableEngine)->str:
        if engine is CHTableEngine.VersionedCollapsingMergeTree:
            return cls.getVersionedCollapsingMergeTree()
        elif engine is CHTableEngine.ReplacingMergeTree:
            return cls.getReplacingMergeTree()
        else:
            return cls.getDefault()


class DDLTemplate():
    @classmethod
    def get_insert_template(cls) -> str:
        return """
            INSERT INTO {table_name}
            ({columns})
            VALUES
            ({values});
        """
    
    @classmethod
    def get_update_template(cls) -> str:
        return """
            UPDATE {table_name}
            set {dml}
            where {primary_key}={value}
        """
class Table:
    db_name:str
    name:str
    primary_key:Column
    columns:List[Column]
    meta:Dict[str,Column]

    @classmethod
    def read_from_sqltable(cls,sqltable:SQLTable, db_name:str) -> Table:
        table = Table()
        table.db_name = db_name
        table.name = sqltable.name
        columns:List[Column] = []
        meta = {}
        for sqlcolumn in sqltable.columns:
            column = get_column_from_sql(sqlcolumn)
            meta[column.name] = column
        table.columns = meta.values()
        table.meta = meta
        for key in sqltable.primary_key:
            table.primary_key = get_column_from_sql(key)
            if key.name in meta.keys():
                meta[key.name].primary_key = True
        
        return table

    def get_primary_date_column(self) -> Column:
        for column in self.columns:
            if column.nullable:
                continue
            elif isinstance(column,DateColumn):
                return column
            elif isinstance(column,DateTimeColumn):
                return column
        return None

    async def mock_insert(self,conn, hint) -> int: 
        tpl = DDLTemplate.get_insert_template() 
        debug(self.columns)
        columns = [column.name for column in self.columns if column.autoincrement is False]
        data = {column.name:fake(column, hint) for column in self.columns if column.autoincrement is False}
        sql = tpl.format(table_name=self.name,
                   columns=",".join(columns),
                   values=",".join(map(lambda value: ":{value}".format(value=value),columns)))

        debug(sql)
        data = jsonity(data)
        debug(data)
        r = await conn.execute(query=sql,values=data)
        return r

    async def mock_update(self, conn, hint, primary_id) ->  None:
        tpl = DDLTemplate.get_update_template()
        columns = [column.name for column in self.columns if column.autoincrement is False]
        data = {column.name:fake(column, hint) for column in self.columns if column.primary_key is False}
        items = []
        for k, v in data.items():
            column = self.meta[k]
            if isinstance(column,StringColumn) or \
                isinstance(column, DateColumn) or \
                    isinstance(column, DateTimeColumn):
                items.append("{k}='{v}'".format(k=k,v=v))
            else:
                items.append("{k}={v}".format(k=k, v=v))
        
        sql = tpl.format(table_name=self.name,
            dml = ",".join(items),
            primary_key = self.primary_key.name,
            value = primary_id
        )    
        debug(sql)
        r = await conn.execute(sql)
        return r        

    def get_ch_ddl(self) -> str:
        ch_columns = []
        for column in self.columns:
            line = "`{table_name}` {type}".format(table_name = column.name, type=column.to_ch_type())
            ch_columns.append(line)
        primary_date_column = self.get_primary_date_column()
        if primary_date_column is None:
            logger.error("{table_name} no primary date column".format(table_name=self.name))
            return None
        else:
            tpl = ClickhouseTableTemplate.get(CHTableEngine.ReplacingMergeTree)
            ddl = tpl.format(db_name=self.db_name,
                             table_name=self.name,
                             create_column=",".join(ch_columns),
                             primary_date = primary_date_column.name,
                             primary_key = self.primary_key.name)
            return ddl

    @classmethod
    def metadata(cls, database_url) -> dict:
        meta = sqlalchemy.MetaData()
        engine = sqlalchemy.create_engine(database_url)
        config = parse_url(database_url)
        meta.reflect(bind=engine)
        metatable = {}
        for sql_table_name, sql_table in meta.tables.items():
            table = Table.read_from_sqltable(sql_table,config['name'])
            key = "{db}/{table}".format(db=table.db_name,table=table.name)
            metatable[key] = table
        return metatable

    def normalize(self, data:dict) -> dict:
        r = {}
        for k, c in self.meta.items():
            v = data.get(k)
            
            if isinstance(c,DateColumn):
                if v is None:
                    r[k] = date.min
                elif isinstance(v,date):
                    r[k] = v
                else:
                    r[k] = date.fromisoformat(v)
            elif isinstance(c,DateTimeColumn):
                if v is None:
                    r[k] = datetime(2000,1,1)
                elif isinstance(v, datetime):
                    r[k] = v
                else:
                    r[k] = datetime.fromisoformat(v)
            elif isinstance(c, DecimalColumn):
                r[k] = str(v)
            elif isinstance(c, IntegerColumn):
                if c.nullable:  
                    r[k] = v
                else: 
                    if v is None: #当值为空且schema要求非空 设置默认值为0
                        r[k] = 0
                    else:
                        r[k] = v
            else:
                r[k] = v
        return r


    def get_default(self):
        default = {}
        for column in self.columns:
            if isinstance(column,DateTimeColumn):
                default[column.name] = datetime.now()
            else:
                default[column.name] = None
        return default
    

