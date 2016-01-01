PyDataColl
==========

.. image:: https://travis-ci.org/timercrack/pydatacoll.svg
    :target: https://travis-ci.org/timercrack/pydatacoll?branch=master

.. image:: https://coveralls.io/repos/timercrack/pyalgotrade/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/timercrack/pyalgotrade?branch=master

.. image:: https://badges.gitter.im/timercrack/pydatacoll.svg
    :alt: Join the chat at https://gitter.im/timercrack/pydatacoll
    :target: https://gitter.im/timercrack/pydatacoll?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

PyDataColl is a `SCADA <Ahttps://en.wikipedia.org/wiki/SCADA>`_-like system which use Python as the main language. It
originally inspired by an old program I made as for a core part of a large
`EMS <https://en.wikipedia.org/wiki/Energy_management_system>`_ when I worked as an engineer at GDT(A
`SGCC <https://en.wikipedia.org/wiki/State_Grid_Corporation_of_China>`_ 's subsidiary).

PyDataColl can be roughly divided into three parts:

* An APIServer provides `RESTful Services <https://en.wikipedia.org/wiki/Representational_state_transfer>`_
  for client to pull/push data from/to devices and perform generic CRUD on devices, terms and items.

* A DeviceManager that manages all devices and terms connected to the system, listens messages send by APIServer
  that perform CRUD on devices and terms. It may be combined with some plugins to perform generic operation such as
  data checking, database saving and formula calculation.

* Many devices and terms under control of DeviceManager operate with coded data over communication channels(TCP/IP)
  so as to provide control of remote equipment(meter or sensor). Each type of Device can communicate with one type of
  meter with specify protocol, such as `Modbus <https://en.wikipedia.org/wiki/Modbus>`_,
  `IEC 60870-5-104 <https://en.wikipedia.org/wiki/IEC_60870-5>`_.

Installation
------------

**Prerequisites**: PyDataColl runs on Python 3.5+. In addition to the requirements showed in requirements.txt,
the following optional packages may be useful:

* `Redis <http://redis.io/>`_ is heavily used by PyDataColl as NoSQL databases and
  `IPC <https://en.wikipedia.org/wiki/Inter-process_communication>`_. If you
  deploy PyDataColl in local, make sure you have installed and started the Redis server.
* `MySQL <https://www.mysql.com/>`_ is used by DbSaver plugin to store device data in real-time. If you
  deploy PyDataColl in local, make sure you have installed and started the MySQL server.
* `ujson <https://pypi.python.org/pypi/ujson>`_ is an ultra fast JSON encoder and decoder written in pure C with
  bindings for Python. This is an alternative json library and PyDataColl will use it automatically if possible.
* `hiredis <https://pypi.python.org/pypi/hiredis>`_ is a C library which can provide up to a 10x speed improvement in
  parsing responses from the Redis server. This is an alternative redis client and
  PyDataColl will use it automatically if possible.

1.  Download the latest source code `hosted on github <https://github.com/timercrack/pydatacoll>`_, open a shell
    (*CMD* in Windows, *Terminal* in Linux and Mac OS X) and go to the source code directory, install required
    package using pip::

        pip install -r requirements.txt

2.  Then run the following to start PyDataColl server::

        python -m pydatacoll.api_server

.. note::
    to stop server, press CTRL+C to exit

3.  Congratulations! The server is running now. Visit http://localhost:8080 in browser to see the server information, if
    success, you will find something like this::

        PyDataColl is running, available API:
        method: GET      URL: http://localhost:8080/
        method: GET      URL: http://localhost:8080/api/v1/device_protocols
        method: GET      URL: http://localhost:8080/api/v1/devices
        (...more omitted)

4.  Now you can send request to server with your favorite http client! check :doc:`restapi` to see the API list.

**Platforms**: PyDataColl should run on any Unix-like platform, although for the best performance and scalability only
Linux (with ``epoll``) and BSD (with ``kqueue``) are recommended for production deployment (even though Mac OS X is
derived from BSD and supports kqueue, its networking performance is generally poor so it is recommended only for
development use).  PyDataColl will also run on Windows, although this configuration is not officially supported and is
recommended only for development use.

Quick links
-----------

* `Source (github) <https://github.com/timercrack/pydatacoll>`_
* `Docs <http://pydatacoll.readthedocs.org/>`_
