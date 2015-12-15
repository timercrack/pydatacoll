import asyncio
import datetime
try:
    import ujson as json
except ImportError:
    import json
import functools
import aioredis
import asynctest
import pymysql
import redis
import pydatacoll.utils.logger as my_logger
import pydatacoll.plugins.db_save as db_save

logger = my_logger.get_logger('DBSaverTest')


class DBSaverTest(asynctest.TestCase):
    def setUp(self):
        super(DBSaverTest, self).setUp()
        self.conn = pymysql.Connect(host='127.0.0.1', port=3306, user='pydatacoll', password='pydatacoll', db='test')
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS test_db_save")
        self.cursor.execute("CREATE TABLE test_db_save(device_id INTEGER, term_id INTEGER, item_id INTEGER,"
                            "time DATETIME, value FLOAT)")
        self.conn.commit()
        self.redis_pool = asyncio.get_event_loop().run_until_complete(
                functools.partial(aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=10,
                                  encoding='utf-8')())
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        self.redis_client.flushdb()
        self.db_saver = db_save.DBSaver(self.loop, self.redis_pool)
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
        pub_data = json.dumps({
            'device_id': 1, 'term_id': 10, 'item_id': 20,
            'time': datetime.datetime.now().isoformat(sep=' '), 'value': 123.4,
        })
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', pub_data)
        await asyncio.sleep(1)
        self.redis_client.rpush('LST:DATA:1:10:20', json.dumps((datetime.datetime.now().isoformat(sep=' '), 123.4)))
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', pub_data)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:20', pub_data)
        await asyncio.sleep(1)
        self.cursor.execute("SELECT * FROM test_db_save")
        rst = self.cursor.fetchall()
        self.assertEqual(len(rst), 1)
        self.assertEqual(rst[0][4], 123.4)
