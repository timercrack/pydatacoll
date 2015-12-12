.. pydatacoll documentation master file, created by
   sphinx-quickstart on Tue Dec  8 16:19:04 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PyDataColl
==========

PyDataColl is a `SCADA <Ahttps://en.wikipedia.org/wiki/SCADA>`_-like system which used Python as the main language
originally inspired by an old program I made as for a core part of a large
`EMS <https://en.wikipedia.org/wiki/Energy_management_system>`_ when I'm an employee of GDT(A
`SGCC <https://en.wikipedia.org/wiki/State_Grid_Corporation_of_China>`_ 's subsidiary).

PyDataColl can be roughly divided into three part:

* An APIServer providing `RESTful Services <https://en.wikipedia.org/wiki/Representational_state_transfer>`_
  for client to pull/push data from/to devices and perform generic CRUD on devices, terms and items.

* A DeviceManager that manage all of devices and terms connected to the system, listening messages send by APIServer
  that perform CRUD on device and term. It may be combined with some plugins to perform generic operation such as
  data checking, database save and formula calculation.

* Many of Devices and Terms under control of DeviceManager operate with coded data over communication channels(TCP/IP)
  so as to provide control of remote equipment(meter or sensor). Each type of Device can communicate with one type of
  meter with specify protocol, such as `Modbus-RTU <https://en.wikipedia.org/wiki/Modbus>`_,
  `IEC 60870-5-104 <https://en.wikipedia.org/wiki/IEC_60870-5>`_.

Installation
------------

1.  Pydatacoll use `Redis <http://redis.io/download>`_ to store collected data and need `Python <https://www.python.org/downloads/>`_ >=3.5,
make sure you have installed them.(if you're under Windows, use `Redis for win <https://github.com/MSOpenTech/redis/releases/>`_)

2.  download the latest source code, open a shell and enter the source dir, install required package using pip::

        pip install -r requirements

3.  open two shells, run following cmd separately::

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

