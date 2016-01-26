Introduction
------------

PyDataColl is a `SCADA <Ahttps://en.wikipedia.org/wiki/SCADA>`_-like system which use Python as the main language. It
originally inspired by an old program I made as for a core part of a large
`EMS <https://en.wikipedia.org/wiki/Energy_management_system>`_.

PyDataColl can be roughly divided into three parts:

* An APIServer provides `RESTful Services <https://en.wikipedia.org/wiki/Representational_state_transfer>`_
  for client to pull/push data from/to devices and perform generic CRUD on devices, terms and items.

* A DeviceManager that manages all devices and terms connected to the system, listens messages send by APIServer
  that perform CRUD on devices and terms. It may be combined with some plugins to perform generic operation such as
  data checking, database saving and formula calculation.

* Many devices and terms under control of DeviceManager operate with coded data over communication channels(TCP/IP)
  so as to provide control of remote equipment(meter or sensor). Each type of Device can communicate with one type of
  meter with specify protocol, such as `Modbus TCP <https://en.wikipedia.org/wiki/Modbus>`_,
  `IEC 60870-5-104 <https://en.wikipedia.org/wiki/IEC_60870-5#IEC_60870-5-104>`_.
