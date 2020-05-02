from __future__ import annotations
from typing import List
from sqlalchemy.sql.schema import Table as SQLTable
from sqlalchemy.sql.schema import Column as SQLColumn
from devtools import debug
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

class CHTableEngine(Enum):
    MergeTree = 1 
    ReplacingMergeTree = 2
    SummingMergeTre = 3 
    AggregatingMergeTree = 4 
    CollapsingMergeTree = 5
    VersionedCollapsingMergeTree = 6
    GraphiteMergeTree = 7
    TinyLog = 8
    StripeLog = 9 
    Log = 10
    Kafka = 11
    MySQL = 12
    ODBC = 13
    JDBC = 14
    HDFS = 15
    Distributed = 16
    MaterializedView = 17
    Dictionary = 18
    Merge = 19
    File = 20
    Null = 21
    Set = 22
    Join = 23
    URL = 24
    View = 25
    Memory = 26
    Buffer = 27



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
    table_name:str
    primary_key:Column
    columns:List[Column]
    
    @classmethod
    def read_from_sqltable(cls,sqltable:SQLTable) -> Table:
        table = Table()
        table.name = sqltable.name
        for sqlcolumn in sqltable.columns:
            column = get_column_from_sql(sqlcolumn)
            debug(sqlcolumn)
            debug(column)
            print("----------------")
        return table

    def get_ch_ddl(self) -> str:
        pass