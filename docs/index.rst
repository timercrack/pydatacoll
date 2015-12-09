.. pydatacoll documentation master file, created by
   sphinx-quickstart on Tue Dec  8 16:19:04 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pydatacoll
==========

pydatacoll is a server based data acquisition system written in Python, include:

* web server, offer general CRUD operation and REST API
* device manager, send/coll data from data provider(meter)
* multi Industrial Control Protocol support(IEC60870, Modbus..)

*WARNING: this project is under alpha develop status!*

Installation
------------

1.  pydatacoll use `Redis <http://redis.io/download>`_ to store collected data and need `Python <https://www.python.org/downloads/>`_ >=3.5,
make sure you have installed them.(if you're under Windows, use `Redis for win <https://github.com/MSOpenTech/redis/releases/>`_)

2.  download the latest source code, open a shell and enter the source dir, install required package using pip::

        pip install -r requirements

3.  open two shell, run following cmd separately::

        python -m pydatacoll.plugins.device_manage
        python -m pydatacoll.api_server
        *(note: above cmd block the shell, use CTRL+C to exit each)*

4.  open a web browser, visit this url: http://localhost:8080 if success, you will find something like this::

        pydatacoll server is running, API is:
        method: GET      URL: http://localhost:8080/
        method: GET      URL: http://localhost:8080/api/v1/device_protocols
        method: GET      URL: http://localhost:8080/api/v1/devices
        (...more omitted)

5.  now you can make HTTP request with your favorite http client! check :doc:`restapi` to see the
current available API list.

Quick links
-----------

* `Source (github) <https://github.com/timercrack/pydatacoll>`_


Documentation
-------------

This documentation is also available in `PDF formats
<https://readthedocs.org/projects/pydatacoll/downloads/>`_.

.. toctree::
   :maxdepth: 2
   :titlesonly:

   guide
   develop
   restapi


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

