import redis
import sys


def prepare_data(device_count=100, term_count=100, item_count=100):
    print('cleaning data...')
    s = redis.StrictRedis(db=1, decode_responses=True)
    print('prepare new data...')
    s.flushdb()
    for item_id in range(item_count):
        s.hmset('HS:ITEM:{}'.format(item_id), {'id': item_id, 'name': '测试指标{}'.format(item_id)})
        s.sadd("SET:ITEM", item_id)
    for device_id in range(device_count):
        s.hmset('HS:DEVICE:{}'.format(device_id), {'id': device_id, 'name': '测试集中器{}'.format(device_id),
                                                   'ip': '127.0.0.1', 'port': device_id+2404, 'protocol': 'iec104'})
        s.sadd('SET:DEVICE', device_id)
        for term_id in range(term_count):
            s.hmset('HS:TERM:{}'.format(term_id), {'id': term_id, 'name': '测试终端{}'.format(term_id),
                                                   'device_id': device_id})
            s.sadd('SET:TERM', term_id)
            s.sadd('SET:DEVICE_TERM:{}'.format(device_id), term_id)
            for item_id in range(item_count):
                term_item = {
                    'id': item_id, 'device_id': device_id, 'term_id': term_id, 'item_id': item_id,
                    'protocol_code': term_id*100+item_id, 'code_type': 36, 'base_val': 0, 'coefficient': 1,
                    'down_limit': 100, 'up_limit': 1000,
                    'db_save_sql':
                        "insert into test_db_save(device_id,term_id,item_id,time,value) VALUES"
                        "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value})",
                    'db_warn_sql':
                        "insert into test_data_check(device_id,term_id,item_id,time,value,warn_msg) VALUES"
                        "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value},"
                        "'{PARAM.warn_msg}')",
                    'warn_msg': 'value error!',
                    'do_verify': 'param.down_limit <= value <= str(900)'
                }
                s.hmset('HS:TERM_ITEM:{}:{}'.format(term_id, item_id), term_item)
                s.hmset('HS:MAPPING:IEC104:{}:{}'.format(device_id, term_item['protocol_code']), term_item)
                s.sadd('SET:TERM_ITEM:{}'.format(term_id), item_id)
            sys.stdout.write("\r%02d%%" % (term_id * 100 / term_count))
            sys.stdout.flush()
    print('done!')

if __name__ == '__main__':
    prepare_data(1, 100, 100)
