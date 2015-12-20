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
            'id': '主键',
            'name': '终端名称',
            'address': '终端地址',
            'identify': '唯一标识',
            'protocol': '协议,值=TERM_PROTOCOLS',
            'device_id': '所属设备ID',
        },
        "HS:ITEM:{item_id}": {
            'id': '主键',
            'name': '指标名称',
            'view_code': '显示名称',
            'func_type': '指标类型',
        },
        "HS:TERM_ITEM:{term_id}:{item_id}": {
            'id': '主键',
            'term_id': '终端ID',
            'item_id': '指标ID',
            'protocol_code': '协议代码',
            'base_val': '基值',
            'coefficient': '系数',
            'db_save_sql': '数据库存储SQL',
        },
        "HS:MAPPING:{protocol_name}:{device_id}:{protocol_code}": {
            '与HS:TERM_ITEM:{term_id}:{item_id}相同',
        },
        "HS:DATA:{device_id}:{term_id}:{item_id}": {
            'datetime.isoformat': 'value',
        },

        "HS:FORMULA:{formula_id}": {
            'id': '主键',
            'formula': '计算公式',
            'p1': '计算参数1(共8个参数,2~7省略)',
            'device_id': '设备ID',
            'term_id': '终端ID',
            'item_id': '指标ID',
            # 'calc_type': '计算方式',
        }
    },

    "set": {
        "SET:DEVICE":
            '设备类的主键id列表, eg: [1,2,3]',

        "SET:TERM":
            '终端类的主键id列表, eg: [1,2,3]',

        "SET:ITEM":
            '指标类的主键id列表, eg: [1,2,3]',

        "SET:DEVICE_TERM:{device_id}":
            '连接到特定设备类的终端主键id列表, eg: [1,2,3]',

        "SET:TERM_ITEM:{term_id}":
            '连接到特定终端类的指标主键id列表, eg: [1,2,3]',

        "SET:FORMULA_PARAM:{device_id}:{term_id}:{item_id}":
            '存储本指标被哪些计算公式作为参数引用,存储计算公式id列表, eg: [1,2,3]',

        "SET:FORMULA":
            '计算公式主键id列表, eg: [1,2,3]',
    },

    "list": {
        "LST:FRAME:{device_id}":
            '存储设备发送接收的原始帧,格式: 时间,send/recv,帧',

        "LST:DATA_TIME:{device_id}:{term_id}:{item_id}":
            '存储数据时间,格式: datetime.isoformat',
    },

    "channel": {
        "CHANNEL:DEVICE_ADD":
            '添加设备,消息内容: HS:DEVICE:{device_id}的值',

        "CHANNEL:DEVICE_FRESH":
            '更新设备,消息内容: HS:DEVICE:{device_id}的值',

        "CHANNEL:DEVICE_DEL":
            '删除设备,消息内容: device_id',

        "CHANNEL:TERM_ADD":
            '添加终端,消息内容: HS:TERM:{term_id}的值',

        "CHANNEL:TERM_DEL":
            '删除终端,消息内容: {device_id:xxx, term_id:xxx}',

        "CHANNEL:TERM_ITEM_ADD":
            '终端指标关联,消息内容: HS:TERM_ITEM:{term_id}:{item_id}的值',

        "CHANNEL:TERM_ITEM_DEL":
            '终端指标解除关联,消息内容: {device_id:xxx, term_id:xxx, item_id:xxx}',

        "CHANNEL:DEVICE_CALL":
            '设备数据招测,消息内容: {device_id:xxx, term_id:xxx, item_id:xxx}',

        "CHANNEL:DEVICE_CTRL":
            '设备控制,消息内容: {device_id:xxx, term_id:xxx, item_id:xxx, value:xxx}',

        "CHANNEL:DEVICE_CALL:{device_id}:{term_id}:{item_id}":
            '招测返回,消息内容: {device_id:xxx, term_id:xxx, item_id:xxx, time:xxx, value:xxx}',

        "CHANNEL:DEVICE_CTRL:{device_id}:{term_id}:{item_id}":
            '控制返回,消息内容: 同上',

        "CHANNEL:DEVICE_DATA:{device_id}:{term_id}:{item_id}":
            '采集数据,消息内容: 同上',

        "CHANNEL:WARNING:{device_id}:{term_id}:{item_id}":
            '报警数据,消息内容: 同上+{warn_msg:xxx}',

        "CHANNEL:FORMULA_ADD":
            '添加计算公式,消息内容: HS:FORMULA:{formula_id}的值',

        "CHANNEL:FORMULA_FRESH":
            '更新计算公式,消息内容: HS:FORMULA:{formula_id}的值',

        "CHANNEL:FORMULA_DEL":
            '删除计算公式,消息内容: formula_id',
    }
}
