from __future__ import annotations
from typing import List
from sqlalchemy.sql.schema import Table as SQLTable
from sqlalchemy.sql.schema import Column as SQLColumn
from devtools import debug
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
from loguru import logger
from faker import Faker
from faker.providers import python

_fake = Faker()
_fake.add_provider(python)



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

    def to_ch_type(self) -> str:
        pass
    
    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> Column:
        column = Column()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def fake(self) -> str:
        return ""

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

    def fake(self) -> str:
        return _fake.pystr()

class TextColumn(Column):

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def fake(self) -> str:
        return _fake.text()

class IntegerColumn(Column):

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> IntegerColumn:
        column = IntegerColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(UInt64)"
        else:
            return "UInt64"

    def fake(self) -> int:
        return _fake.pyint()


class DecimalColumn(Column):

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> DecimalColumn:
        column = DecimalColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def fake(self) -> Decimal:
        return _fake.pydecimal()

class FloatColumn(Column):
    pass

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(Float32)"
        else:
            return "Float32"

    def fake(self) -> float:
        return _fake.pyfloat()

class BooleanColumn(Column):
    pass

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(UInt8)"
        else:
            return "UInt8"

    def fake(self) -> bool:
        return _fake.pybool()

class DateTimeColumn(Column):
    pass
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(DateTime)"
        else:
            return "DateTime"

    def fake(self) -> datetime:
        return _fake.date_time()

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

    def fake(self) -> date:
        return _fake.date()

class TimeColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def fake(self) -> str:
        return _fake.iso8601()

class JSONColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

    def fake(self) -> str:
        return json.dumps(_fake.pydict())



def get_column_from_sql(sqlcolumn:SQLColumn) -> Column:
    ptype = sqlcolumn.type.python_type
    debug(ptype)
    if ptype is int:
        return IntegerColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is str:
        return StringColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is date:
        return DateColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is Decimal:
        return DecimalColumn.read_from_sqlcolumn(sqlcolumn)
    

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
        
class Table:
    db_name:str
    name:str
    primary_key:Column
    columns:List[Column]

    @classmethod
    def read_from_sqltable(cls,sqltable:SQLTable, db_name:str) -> Table:
        table = Table()
        debug(sqltable.info)
        table.db_name = db_name
        table.name = sqltable.name
        columns:List[Column] = []
        for sqlcolumn in sqltable.columns:
            column = get_column_from_sql(sqlcolumn)
            columns.append(column)
        table.columns = columns
        for key in sqltable.primary_key:
            table.primary_key = get_column_from_sql(key)
        return table

    def get_primary_date_column(self):
        for column in self.columns:
            debug(column)
            if column.nullable:
                continue
            elif isinstance(column,DateColumn):
                return column
            elif isinstance(column,DateTimeColumn):
                return column
        return None

    def mock_insert(self):
        tpl = DDLTemplate.get_insert_template()
        columns = [column.name for column in self.columns]
        values = [column.fake() for column in self.columns]
        sql = tpl.format(table_name=self.name,
                   columns=",".join(map(lambda column: '`%s`' % column,columns)),
                   values=",".join(map(lambda column: '`%s`' % column,values)))
        debug(sql)
        logger.info(sql)

                

    def get_ch_ddl(self) -> str:
        ch_columns = []
        for column in self.columns:
            line = "`{table_name}` {type}".format(table_name = column.name, type=column.to_ch_type())
            ch_columns.append(line)
        primary_date_column = self.get_primary_date_column()
        if primary_date_column is None:
            logger.error("no primary date column")
            return None
        else:
            tpl = ClickhouseTableTemplate.get(CHTableEngine.VersionedCollapsingMergeTree)
            ddl = tpl.format(db_name=self.db_name,
                             table_name=self.name,
                             create_column=",".join(ch_columns),
                             primary_date = primary_date_column.name,
                             primary_key = self.primary_key.name)
            return ddl