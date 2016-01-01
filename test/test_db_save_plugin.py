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

logger = my_logger.get_logger('DBSaverTest')


class DBSaverTest(asynctest.TestCase):
    loop = None  # make pycharm happy

    def setUp(self):
        super(DBSaverTest, self).setUp()
        self.conn = pymysql.Connect(host='127.0.0.1', port=3306, user='pydatacoll', password='pydatacoll', db='test')
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS test_db_save")
        self.cursor.execute("""
CREATE TABLE test_db_save(
  id int AUTO_INCREMENT PRIMARY KEY,
  device_id int,
  term_id int,
  item_id int,
  time datetime,
  value float
) ENGINE=MyISAM DEFAULT CHARSET=utf8
""")
        self.conn.commit()
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        self.redis_client.flushdb()
        self.db_saver = db_save.DBSaver(self.loop)
        self.loop.run_until_complete(self.db_saver.install())

    def tearDown(self):
        self.conn.close()
        self.loop.run_until_complete(self.db_saver.uninstall())

    async def test_save(self):
        self.redis_client.hmset('HS:TERM_ITEM:10:20', {
            'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 36,
            'db_save_sql': "insert into test_db_save(device_id,term_id,item_id,time,value) VALUES "
                           "({PARAM.device_id},{PARAM.term_id},{PARAM.item_id},'{PARAM.time}',{PARAM.value})"
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
