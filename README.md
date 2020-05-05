## unimeta

realtime database sync tools

## Installation

unimeta is distributed on `PyPI <https://pypi.org>`_ as a universal
wheel and is available on Linux/macOS and Windows and supports
Python 2.7/3.5+ and PyPy.

```shell

    $ pip install unimeta
```

```pycon
    from unimeta.pipeline import MysqlSource, ClickHouseSink, Pipeline
    mysql_url = "mysql://root:111111@127.0.0.1:3306/hr"
    clickhouse_url = "clickhouse://127.0.0.1:9000/hr"
    
    source = MysqlSource(database_url = mysql_url)
    sink = ClickHouseSink(database_url = clickhouse_url)
    pipe = Pipeline()
    pipe.sync_tables()
    pipe.sync()
```

## License

unimeta is distributed under the terms of both

- `MIT License <https://choosealicense.com/licenses/mit>`_
- `Apache License, Version 2.0 <https://choosealicense.com/licenses/apache-2.0>`_

at your option.
