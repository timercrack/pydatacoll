import asyncio
import datetime
try:
    import ujson as json
except ImportError:
    import json
import asynctest
import pymysql
import redis
import pydatacoll.utils.logger as my_logger
import pydatacoll.plugins.db_save as db_save
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('DataCheckTest')


class DataCheckTest(asynctest.TestCase):
    loop = None  # make pycharm happy

    def setUp(self):
        super(DataCheckTest, self).setUp()
        self.conn = pymysql.Connect(**db_save.PLUGIN_PARAM)
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS test_data_check")
        self.cursor.execute("""
CREATE TABLE test_data_check(
  id INT AUTO_INCREMENT PRIMARY KEY,
  device_id VARCHAR(32),
  term_id VARCHAR(32),
  item_id VARCHAR(32),
  time DATETIME,
  value FLOAT,
  warn_msg VARCHAR(1000)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
""")
        self.conn.commit()
        self.redis_client = redis.StrictRedis(db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.redis_client.flushdb()
        self.db_saver = db_save.DBSaver(self.loop)
        self.loop.run_until_complete(self.db_saver.install())

    def tearDown(self):
        self.loop.run_until_complete(self.db_saver.uninstall())
        self.conn.close()

    async def test_data_check(self):
        term_item = {
            'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 36, 'down_limit': 50, 'up_limit': 100,
            'db_warn_sql': "insert into test_data_check(device_id,term_id,item_id,time,value,warn_msg) VALUES "
                           "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value},"
                           "'{PARAM.warn_msg}')",
            'warn_msg': 'value error!',
            'do_verify': 'param.down_limit <= value <= param.up_limit',
        }
        self.redis_client.hmset('HS:TERM_ITEM:10:20', term_item)
        time_str = datetime.datetime.now().isoformat()
        pub_data = {'device_id': 1, 'term_id': 10, 'item_id': 20, 'time': time_str, 'value': 123.4}
        self.redis_client.rpush('LST:DATA_TIME:1:10:20', time_str)
        self.redis_client.hset('HS:DATA:1:10:20', time_str, 123.4)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', json.dumps(pub_data))
        await asyncio.sleep(1)
        pub_data['value'] = 42
        term_item['do_verify'] = 'int(value) == 42'
        term_item['db_warn_msg'] = 'I want 42!!'
        self.redis_client.hmset('HS:TERM_ITEM:10:20', term_item)
        time_str = datetime.datetime.now().isoformat()
        self.redis_client.rpush('LST:DATA_TIME:1:10:20', time_str)
        self.redis_client.hset('HS:DATA:1:10:20', time_str, 42)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', json.dumps(pub_data))
        await asyncio.sleep(1)
        self.cursor.execute("SELECT * FROM test_data_check")
        rst = self.cursor.fetchall()
        self.assertEqual(len(rst), 1)
        self.assertEqual(rst[0][5], 123.4)
        self.assertEqual(rst[0][6], 'value error!')
