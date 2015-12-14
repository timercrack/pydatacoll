RESTful API
-----------

currently support API:

======   ===========================================================================
Method   URL(parameter surrounded by curly braces should replaced by real value)
======   ===========================================================================
GET      /
GET      /api/v1/device_protocols
GET      /api/v1/devices
GET      /api/v1/devices/{device_id}
GET      /api/v1/devices/{device_id}/terms
GET      /api/v1/devices/{device_id}/terms/{term_id}/items/{item_id}/datas
GET      /api/v1/devices/{device_id}/terms/{term_id}/items/{item_id}/datas/{index}
GET      /api/v1/items
GET      /api/v1/items/{item_id}
GET      /api/v1/term_protocols
GET      /api/v1/terms
GET      /api/v1/terms/{term_id}
GET      /api/v1/terms/{term_id}/items
GET      /api/v1/terms/{term_id}/items/{item_id}
POST     /api/v1/device_call
POST     /api/v1/device_ctrl
POST     /api/v1/devices
POST     /api/v1/items
POST     /api/v1/terms
POST     /api/v1/terms/{term_id}/items
PUT      /api/v1/devices/{device_id}
PUT      /api/v1/items/{item_id}
PUT      /api/v1/terms/{term_id}
PUT      /api/v1/terms/{term_id}/items/{item_id}
DELETE   /api/v1/devices/{device_id}
DELETE   /api/v1/items/{item_id}
DELETE   /api/v1/terms/{term_id}
DELETE   /api/v1/terms/{term_id}/items/{item_id}
======   ===========================================================================