from __future__ import annotations
from typing import List
from sqlalchemy.sql.schema import Table as SQLTable
from sqlalchemy.sql.schema import Column as SQLColumn
from devtools import debug
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

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

class TextColumn(Column):

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

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

class FloatColumn(Column):
    pass

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(Float32)"
        else:
            return "Float32"

class BooleanColumn(Column):
    pass

    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(UInt8)"
        else:
            return "UInt8"

class DateTimeColumn(Column):
    pass
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(DateTime)"
        else:
            return "DateTime"

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

class TimeColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String" 

class JSONColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(String)"
        else:
            return "String"

class EnumColumn(Column):
    
    def to_ch_type(self) -> str:
        if self.nullable:
            return "Nullable(Enum16)"
        else:
            return "Enum16"


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
    


class Table:
    db_name:str
    table_name:str
    primary_key:Column
    columns:List[Column]
    
    @classmethod
    def read_from_sqltable(cls,sqltable:SQLTable) -> Table:
        table = Table()
        debug(sqltable.info)
        table.name = sqltable.name
        columns:List[Column] = []
        for sqlcolumn in sqltable.columns:
            column = get_column_from_sql(sqlcolumn)
            columns.append(column)
        table.columns = columns
        for key in sqltable.primary_key:
            table.primary_key = get_column_from_sql(key)
        return table

    def get_ch_ddl(self,db_name) -> str:
        ch_columns = []
        for column in self.columns:
            debug(column)
            line = "`{table_name}` {type}".format(table_name = column.name, type=column.to_ch_type())
            ch_columns.append(line)
        ddl = """
        CREATE TABLE {db_name}.{table_name}
        (
            {create_column}
        )
        ENGINE = {engine_name}
        ORDER BY ({primary_key})
        """.format(db_name=db_name,
                   table_name=self.name,
                   create_column=",".join(ch_columns),
                   engine_name = CHTableEngine.CollapsingMergeTree,
                   primary_key = self.primary_key.name)
        return ddl