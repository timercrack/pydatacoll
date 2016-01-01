import asyncio
import datetime
try:
    import ujson as json
except ImportError:
    import json
import functools
import aioredis
import asynctest
import redis
import pandas as pd
import numpy as np
import pydatacoll.utils.logger as my_logger
import pydatacoll.plugins.formula_calc as formula_calc
from test.mock_device import mock_data

logger = my_logger.get_logger('FormulaCalcTest')


class FormulaCalcTest(asynctest.TestCase):
    loop = None  # make pycharm happy

    def setUp(self):
        super(FormulaCalcTest, self).setUp()
        self.redis_pool = asyncio.get_event_loop().run_until_complete(
                functools.partial(aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=10,
                                  encoding='utf-8')())
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        mock_data.generate()
        self.formula_calc = formula_calc.FormulaCalc(self.loop, self.redis_pool)
        self.loop.run_until_complete(self.formula_calc.install())

    def tearDown(self):
        self.loop.run_until_complete(self.formula_calc.uninstall())

    async def test_formula(self):
        time_str1 = datetime.datetime.now().isoformat()
        pub_data = {
            'device_id': 1, 'term_id': 10, 'item_id': 1000,
            'time': time_str1, 'value': 123.4,
        }
        self.redis_client.rpush('LST:DATA_TIME:1:10:1000', time_str1)
        self.redis_client.hset('HS:DATA:1:10:1000', time_str1, 123.4)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:1000', json.dumps(pub_data))
        await asyncio.sleep(1)
        time_str2 = datetime.datetime.now().isoformat()
        self.redis_client.rpush('LST:DATA_TIME:1:10:1000', time_str2)
        self.redis_client.hset('HS:DATA:1:10:1000', time_str2, 123.4)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:1:10:1000', json.dumps(pub_data))
        time_str3 = datetime.datetime.now().isoformat()
        pub_data = {
            'device_id': 2, 'term_id': 30, 'item_id': 1000,
            'time': time_str3, 'value': 999,
        }
        self.redis_client.rpush('LST:DATA_TIME:2:30:1000', time_str3)
        self.redis_client.hset('HS:DATA:2:30:1000', time_str3, 999)
        self.redis_client.publish('CHANNEL:DEVICE_DATA:2:30:1000', json.dumps(pub_data))
        await asyncio.sleep(1)
        rst = self.redis_client.hgetall('HS:DATA:3:40:1000')
        self.assertEqual(len(rst), 3)
        lst = sorted(rst.keys())
        self.assertEqual(rst[lst[0]], '102.0')
        self.assertEqual(rst[lst[1]], '123.4')
        self.assertEqual(rst[lst[2]], '999.0')

        # 统计06年1月份的累计值
        formula_dict = {
            'id': '2', 'device_id': '3', 'term_id': '40', 'item_id': '1000',
            'formula': "p1.resample('M', how='sum')['2016-01']",
            'p1': '9:90:9000'}
        self.redis_client.hmset('HS:FORMULA:2', formula_dict)

        rng = pd.date_range('1/1/2016', periods=1000, freq='H')
        ts = pd.Series(np.random.randn(1000), index=rng)
        for d in rng.to_pydatetime():
            time_str = d.isoformat()
            self.redis_client.hset('HS:DATA:9:90:9000', time_str, ts[time_str])
            self.redis_client.rpush('LST:DATA_TIME:9:90:9000', time_str)
        self.redis_client.sadd('SET:FORMULA', 2)
        self.redis_client.sadd('SET:FORMULA_PARAM:9:90:9000', 2)
        self.redis_client.delete('HS:DATA:3:40:1000')
        self.redis_client.delete('LST:DATA_TIME:3:40:1000')
        self.redis_client.publish('CHANNEL:FORMULA_ADD', json.dumps(formula_dict))
        await asyncio.sleep(1)
        rst = self.redis_client.hgetall('HS:DATA:3:40:1000')
        self.assertEqual(len(rst), 1)
        lst = sorted(rst.keys())
        self.assertAlmostEqual(
                float(rst[lst[-1]]),
                ts.resample('M', how='sum')['2016-01'][0],
                delta=0.0001)
