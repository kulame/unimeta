unimeta
=======

.. contents:: **Table of Contents**
    :backlinks: none

Installation
------------

unimeta is distributed on `PyPI <https://pypi.org>`_ as a universal
wheel and is available on Linux/macOS and Windows and supports
Python 2.7/3.5+ and PyPy.

.. code-block:: bash

    $ pip install unimeta

.. code-block:: Python
    from unimeta.pipeline import MysqlSource, ClickHouseSink, Pipeline
    source = MysqlSource()
    sink = ClickHouseSink()
    pipe = Pipeline(source, sink)
    pipe.sync_tables()
    pipe.sync()

License
-------

unimeta is distributed under the terms of both

- `MIT License <https://choosealicense.com/licenses/mit>`_
- `Apache License, Version 2.0 <https://choosealicense.com/licenses/apache-2.0>`_

at your option.
