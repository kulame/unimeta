from __future__ import annotations
from typing import List
from sqlalchemy.sql.schema import Table as SQLTable
from sqlalchemy.sql.schema import Column as SQLColumn
from devtools import debug

class Column:
    name:str
    nullable:bool

    def to_clickhouse_type(self) -> str:
        pass
    
    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> Column:
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


class TextColumn(Column):
    pass

class IntegerColumn(Column):

    @classmethod
    def read_from_sqlcolumn(cls,sqlcolumn: SQLColumn) -> IntegerColumn:
        column = IntegerColumn()
        column.nullable = sqlcolumn.nullable
        column.name = sqlcolumn.name
        return column



class FloatColumn(Column):
    pass

class BooleanColumn(Column):
    pass

class DateTimeColumn(Column):
    pass

class DateColumn(Column):
    pass

class TimeColumn(Column):
    pass

class JSONColumn(Column):
    pass

class EnumColumn(Column):
    pass


def get_column_from_sql(sqlcolumn:SQLColumn) -> Column:
    ptype = sqlcolumn.type.python_type
    debug(ptype)
    if ptype is int:
        return IntegerColumn.read_from_sqlcolumn(sqlcolumn)
    elif ptype is str:
        return StringColumn.read_from_sqlcolumn(sqlcolumn)

    


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

