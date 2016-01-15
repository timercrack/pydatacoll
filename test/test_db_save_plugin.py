import asyncio
import datetime
try:
    import ujson as json
except ImportError:
    import json
import asynctest
import pymysql
import redis
import aioredis
import pydatacoll.utils.logger as my_logger
import pydatacoll.plugins.db_save as db_save
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('DBSaverTest')


class DBSaverTest(asynctest.TestCase):
    loop = None  # make pycharm happy

    def setUp(self):
        super(DBSaverTest, self).setUp()
        self.conn = pymysql.Connect(**db_save.PLUGIN_PARAM)
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS test_db_save")
        self.cursor.execute("""
CREATE TABLE test_db_save(
  id INT AUTO_INCREMENT PRIMARY KEY,
  device_id VARCHAR(32),
  term_id VARCHAR(32),
  item_id VARCHAR(32),
  time DATETIME,
  value FLOAT
) ENGINE=MyISAM DEFAULT CHARSET=utf8
""")
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
        self.conn.close()
        self.loop.run_until_complete(self.db_saver.uninstall())

    async def test_data_save(self):
        self.redis_client.hmset('HS:TERM_ITEM:10:20', {
            'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 36,
            'db_save_sql': "insert into test_db_save(device_id,term_id,item_id,time,value) VALUES "
                           "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value})"
        })
        pub_data = {
            'device_id': 1, 'term_id': 10, 'item_id': 20,
            'time': datetime.datetime.now().isoformat(), 'value': 123.4,
        }
        time_str = datetime.datetime.now().isoformat()
        self.redis_client.rpush('LST:DATA_TIME:1:10:20', time_str)
        self.redis_client.hset('HS:DATA:1:10:20', time_str, 123.4)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', json.dumps(pub_data))
        await asyncio.sleep(1)
        time_str = datetime.datetime.now().isoformat()
        self.redis_client.rpush('LST:DATA_TIME:1:10:20', time_str)
        self.redis_client.hset('HS:DATA:1:10:20', time_str, 123.4)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', json.dumps(pub_data))
        time_str = datetime.datetime.now().isoformat()
        self.redis_client.rpush('LST:DATA_TIME:1:10:20', time_str)
        self.redis_client.hset('HS:DATA:1:10:20', time_str, 123.4)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', json.dumps(pub_data))
        await asyncio.sleep(1)
        self.cursor.execute("SELECT * FROM test_db_save")
        rst = self.cursor.fetchall()
        self.assertEqual(len(rst), 1)
        self.assertEqual(rst[0][5], 123.4)

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

    async def test_sql_check(self):
        term_item_dict = {
            'device_id': 1, 'time': datetime.datetime.now().isoformat(), 'value': 123.4,
            'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 36, 'warn_msg': 'oops',
            'db_save_sql': "insert into test_db_save(device_id,term_id,item_id,time,value) VALUES "
                           "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value})",
            'db_warn_sql': "insert into test_data_check(device_id,term_id,item_id,time,value,warn_msg) VALUES "
                           "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value},"
                           "'{PARAM.warn_msg}')",
        }

        sub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='localhost'),
                                                  config.getint('REDIS', 'port', fallback=6379)),
                                                 db=config.getint('REDIS', 'db', fallback=1))
        channel_name = 'CHANNEL:SQL_CHECK_RESULT:{}'.format(len(repr(term_item_dict)))
        res = await sub_client.subscribe(channel_name)
        cb = asyncio.futures.Future(loop=self.loop)

        async def reader(ch):
            while await ch.wait_message():
                msg = await ch.get(encoding='utf-8')
                if not cb.done():
                    cb.set_result(msg)

        tsk = asyncio.ensure_future(reader(res[0]), loop=self.loop)
        self.redis_client.publish('CHANNEL:SQL_CHECK', json.dumps(term_item_dict))
        rst = await asyncio.wait_for(cb, 10, loop=self.loop)
        await sub_client.unsubscribe(channel_name)
        sub_client.close()
        await tsk
        self.assertEqual(rst, 'OK')
        self.cursor.execute("SELECT * FROM test_db_save")
        rst = self.cursor.fetchall()
        self.assertEqual(rst[0][5], 123.4)
        self.cursor.execute("SELECT * FROM test_data_check")
        rst = self.cursor.fetchall()
        self.assertEqual(rst[0][5], 123.4)
