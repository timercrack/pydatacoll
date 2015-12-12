Introduction
------------

PyDataColl is a `SCADA <Ahttps://en.wikipedia.org/wiki/SCADA>`_-like system which used Python as the main language
originally inspired by an old program I made as for a core part of a large
`EMS <https://en.wikipedia.org/wiki/Energy_management_system>`_ when I'm an employee of GDT(A
`SGCC <https://en.wikipedia.org/wiki/State_Grid_Corporation_of_China>`_ 's subsidiary).

PyDataColl can be roughly divided into three part:

* An APIServer providing `RESTful Services <https://en.wikipedia.org/wiki/Representational_state_transfer>`_
  for client to pull/push data from/to devices and perform generic CRUD on devices, terms and items.

* A DeviceManager that manage all of devices and terms connected to the system, listening messages send by APIServer
  that do CRUD on device and term. It may be combined with some plugins to perform generic operation such as
  data checking, database save and formula calculation.

* Many of Devices and Terms under control of DeviceManager operate with coded data over communication channels(TCP/IP)
  so as to provide control of remote equipment(meter or sensor). Each type of Device can communicate with one type of
  meter with specify protocol, such as `Modbus-RTU <https://en.wikipedia.org/wiki/Modbus>`_,
  `IEC 60870-5-104 <https://en.wikipedia.org/wiki/IEC_60870-5>`_.
