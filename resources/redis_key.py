REDIS_KEY = {
    "str": "",

    "hash": [
        "HS:DEVICE:{device_id}",
        "HS:TERM:{term_id}",
        "HS:ITEM:{item_id}",
        "HS:TERM_ITEM:{term_id}:{item_id}",
        "HS:MAPPING:{protocol_name}:{device_id}:{protocol_code}"
    ],

    "set": [
        "SET:DEVICE",
        "SET:TERM",
        "SET:ITEM",
        "SET:DEVICE_TERM:{device_id}",
        "SET:TERM_ITEM:{term_id}"
    ],

    "list": [
        "LST:FRAME:{device_id}",
        "LST:DATA:{device_id}:{term_id}:{item_id}"
    ],

    "channel": [
        "CHANNEL:DEVICE_ADD",
        "CHANNEL:DEVICE_FRESH",
        "CHANNEL:DEVICE_DEL",
        "CHANNEL:TERM_ADD",
        "CHANNEL:TERM_DEL",
        "CHANNEL:TERM_ITEM_ADD",
        "CHANNEL:TERM_ITEM_DEL",
        "CHANNEL:DEVICE_CALL",
        "CHANNEL:DEVICE_CTRL",
        "CHANNEL:DEVICE_CALL:{device_id}:{term_id}:{item_id}",
        "CHANNEL:DEVICE_CTRL:{device_id}:{term_id}:{item_id}",
        "CHANNEL:DEVICE_DATA:{device_id}:{term_id}:{item_id}",
        "CHANNEL:DATA_WARN:{device_id}:{term_id}:{item_id}",
        "CHANNEL:DATA_CALC:{device_id}:{term_id}:{item_id}"
    ]
}

