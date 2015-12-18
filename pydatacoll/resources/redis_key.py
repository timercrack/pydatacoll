REDIS_KEY = {
    "str": {},

    "hash": {
        "HS:DEVICE:{device_id}": {
            'id': '主键',
            'name': '设备名称',
            'ip': 'IP地址',
            'port': '端口',
            'identify': '唯一标识',
            'status': '在线状态：值=[on, off]',
            'protocol': '协议, 值=DEVICE_PROTOCOLS',
        },
        "HS:TERM:{term_id}": {

        },
        "HS:ITEM:{item_id}": {

        },
        "HS:TERM_ITEM:{term_id}:{item_id}": {

        },
        "HS:MAPPING:{protocol_name}:{device_id}:{protocol_code}": {

        },
        "HS:DATA:{device_id}:{term_id}:{item_id}": {

        },
    },

    "set": [
        "SET:DEVICE",
        "SET:TERM",
        "SET:ITEM",
        "SET:DEVICE_TERM:{device_id}",
        "SET:TERM_ITEM:{term_id}",
        "SET:PARAM:{device_id}:{term_id}:{item_id}",
    ],

    "list": [
        "LST:FRAME:{device_id}",
        "LST:DATA_TIME:{device_id}:{term_id}:{item_id}",
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
        "CHANNEL:DATA_CALC:{device_id}:{term_id}:{item_id}",
    ]
}
