import asyncio
import aiohttp
import aioredis
import asynctest
import redis
import multiprocessing
import time
try:
    import ujson as json
except ImportError:
    import json

import pydatacoll.utils.logger as my_logger
from pydatacoll.resources.protocol import *
from test.mock_device import mock_data, iec104device
from pydatacoll import api_server

logger = my_logger.get_logger('TestInterface')


class RedisTest(asynctest.TestCase):
    async def test_connect_timeout(self):
        try:
            loop = asyncio.get_event_loop()
            reader, writer = await asyncio.open_connection('127.0.0.1', 6379)
            loop.call_later(1, lambda w: w.close(), writer)
            data = await reader.readexactly(100)
            logger.debug('Received: %r', data.decode())
            # print("data={}".format(data))
            self.assertEqual(len(data), 0)
        except asyncio.IncompleteReadError:
            logger.debug('stream closed!')
        except Exception as e:
            logger.error('e=', repr(e))

    async def test_redis_listen(self):
        pub_client = await aioredis.create_redis(('localhost', 6379), db=1)
        sub_client = await aioredis.create_redis(('localhost', 6379), db=1)
        res = await sub_client.subscribe('channel:foo')
        ch1 = res[0]

        async def reader(ch):
            while await ch.wait_message():
                msg = await ch.get_json()
                logger.debug("channel[%s] Got Message:%s", ch.name.decode(), msg)
            logger.debug('quit reader!')

        tsk = asyncio.ensure_future(reader(ch1))

        res = await pub_client.publish_json('channel:foo', ["Hello", "world"])
        self.assertEqual(res, 1)

        await sub_client.unsubscribe('channel:foo')
        await tsk
        sub_client.close()
        pub_client.close()


class InterfaceTest(asynctest.TestCase):
    # use_default_loop = True
    @classmethod
    def setUpClass(cls):
        cls.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        cls.mock_device = multiprocessing.Process(target=iec104device.run_server)
        cls.api_server = multiprocessing.Process(target=api_server.run_server)
        cls.mock_device.start()
        time.sleep(1)
        cls.api_server.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.api_server.terminate()
        cls.api_server.join()
        cls.mock_device.terminate()
        cls.mock_device.join()

    async def test_get_protocol_list(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/device_protocols') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, DEVICE_PROTOCOLS)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/term_protocols') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, TERM_PROTOCOLS)

    async def test_formula_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/formulas') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['1'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/formulas/1') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.formula1)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/formulas/99') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'formula_id not found!')

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formulas', data=json.dumps(mock_data.test_formula)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:FORMULA:9')
            self.assertEqual(rst['formula'], '1+1')
            rst = self.redis_client.sismember('SET:FORMULA', 9)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formulas', data=json.dumps(mock_data.test_formula)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'formula already exists!')

        mock_data.test_formula['formula'] = '2+2'
        mock_data.test_formula['id'] = 5
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/formulas/4', data=json.dumps(mock_data.test_formula)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:FORMULA:9')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:FORMULA', 9)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:FORMULA:5')
            self.assertEqual(rst['formula'], '2+2')
            rst = self.redis_client.sismember('SET:FORMULA', 5)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/formulas/99', data=json.dumps(mock_data.test_formula)) as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'formula_id not found!')

        async with aiohttp.delete('http://127.0.0.1:8080/api/v1/formulas/5') as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:FORMULA:5')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:FORMULA', 5)
            self.assertFalse(rst)

        formula_check = {'formula': 'p1+10', 'p1': 'HS:DATA:1:10:1000'}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formula_check', data=json.dumps(formula_check)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.text()
            self.assertEqual(rst, 'OK')
        formula_check['formula'] = 'p1+p2'
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formula_check', data=json.dumps(formula_check)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.text()
            logger.debug('rst=%s', rst)
            self.assertNotEqual(rst, 'OK')

    async def test_device_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['1', '2', '3'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.device1)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/99') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'device_id not found!')

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/devices', data=json.dumps(mock_data.test_device)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:DEVICE:4')
            self.assertEqual(rst['name'], '测试集中器4')
            rst = self.redis_client.sismember('SET:DEVICE', 4)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/devices', data=json.dumps(mock_data.test_device)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'device already exists!')

        mock_data.test_device['name'] = '测试集中器5'
        mock_data.test_device['id'] = 5
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/devices/4', data=json.dumps(mock_data.test_device)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:DEVICE:4')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:DEVICE', 4)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:DEVICE:5')
            self.assertEqual(rst['name'], '测试集中器5')
            rst = self.redis_client.sismember('SET:DEVICE', 5)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/devices/99', data=json.dumps(mock_data.test_device)) as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'device_id not found!')

        async with aiohttp.delete('http://127.0.0.1:8080/api/v1/devices/5') as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:DEVICE:5')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:DEVICE', 5)
            self.assertFalse(rst)

    async def test_term_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['10', '20', '30', '40'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/10') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.term10)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/99') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'term_id not found!')
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1/terms') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['10', '20'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/99/terms') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'device_id not found!')

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms', data=json.dumps(mock_data.test_term)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:TERM:90')
            self.assertEqual(rst['name'], '测试终端9')
            rst = self.redis_client.sismember('SET:TERM', 90)
            self.assertTrue(rst)
            rst = self.redis_client.sismember('SET:DEVICE_TERM:1', 90)
            self.assertEqual(rst, True)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms', data=json.dumps(mock_data.test_term)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'term already exists!')

        mock_data.test_term['name'] = '测试终端5'
        mock_data.test_term['id'] = 50
        mock_data.test_term['device_id'] = 2
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/90', data=json.dumps(mock_data.test_term)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:TERM:90')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:TERM', 90)
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:DEVICE_TERM:1', 90)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:TERM:50')
            self.assertEqual(rst['name'], '测试终端5')
            rst = self.redis_client.sismember('SET:TERM', 50)
            self.assertTrue(rst)
            rst = self.redis_client.sismember('SET:DEVICE_TERM:2', 50)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/99', data=json.dumps(mock_data.test_term)) as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'term_id not found!')

        async with aiohttp.delete('http://127.0.0.1:8080/api/v1/terms/50') as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:TERM:50')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:TERM', 50)
            self.assertFalse(rst)

    async def test_item_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/items') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['1000', '2000'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/items/1000') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.item1000)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/items/99') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'item_id not found!')
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/10/items') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['1000', '2000'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/99/items') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'term_id not found!')

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/items', data=json.dumps(mock_data.test_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:ITEM:3000')
            self.assertEqual(rst['name'], 'C相电压')
            rst = self.redis_client.sismember('SET:ITEM', 3000)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/items', data=json.dumps(mock_data.test_item)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'item already exists!')

        mock_data.test_item['name'] = '功率因数'
        mock_data.test_item['id'] = 4000
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/items/3000', data=json.dumps(mock_data.test_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:ITEM:3000')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:ITEM', 3000)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:ITEM:4000')
            self.assertEqual(rst['name'], '功率因数')
            rst = self.redis_client.sismember('SET:ITEM', 4000)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/items/99', data=json.dumps(mock_data.test_item)) as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'item_id not found!')

        async with aiohttp.delete('http://127.0.0.1:8080/api/v1/items/4000') as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:ITEM:4000')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:ITEM', 4000)
            self.assertFalse(rst)

    async def test_get_data(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1/terms/10/items/1000/datas') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.device1_term10_item1000)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1/terms/10/items/1000/datas/-1') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, {'2015-12-01T08:50:15.000003': '102'})
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/99/terms/99/items/99/datas') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertEqual(len(rst), 0)

    async def test_term_item_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/10/items/1000') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.term10_item1000)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/99/items/1000') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'term_id not found!')
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/10/items/99') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'item_id not found!')
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/20/items/2000') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'term_item not found!')

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms/20/items',
                                data=json.dumps(mock_data.test_term_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:TERM_ITEM:20:2000')
            mock_data.test_term_item.update({'device_id': '1'})
            self.assertDictEqual(rst, mock_data.test_term_item)
            rst = self.redis_client.hgetall('HS:MAPPING:IEC104:1:400')
            self.assertDictEqual(rst, mock_data.test_term_item)
            rst = self.redis_client.sismember('SET:TERM_ITEM:20', 2000)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms/20/items',
                                data=json.dumps(mock_data.test_term_item)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'term_item already exists!')

        mock_data.test_term_item['protocol_code'] = '401'
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/20/items/2000',
                               data=json.dumps(mock_data.test_term_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.sismember('SET:TERM_ITEM:20', 2000)
            self.assertTrue(rst)
            rst = self.redis_client.exists('HS:MAPPING:IEC104:1:400')
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:TERM_ITEM:20:2000')
            self.assertEqual(rst['protocol_code'], '401')
            rst = self.redis_client.hgetall('HS:MAPPING:IEC104:1:401')
            self.assertDictEqual(rst, mock_data.test_term_item)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/30/items/2000',
                               data=json.dumps(mock_data.test_term_item)) as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'term_item not found!')

        async with aiohttp.delete('http://127.0.0.1:8080/api/v1/terms/20/items/2000') as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:TERM_ITEM:20:2000')
            self.assertFalse(rst)
            rst = self.redis_client.exists('HS:MAPPING:IEC104:1:401')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:TERM_ITEM:20', 2000)
            self.assertFalse(rst)
        del mock_data.test_term_item['device_id']

    async def test_device_call(self):
        call_dict = {'device_id': '1', 'term_id': '10', 'item_id': 1000}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_call', data=json.dumps(call_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertEqual(rst['value'], 123)

    async def test_device_ctrl(self):
        ctrl_dict = {'device_id': '2', 'term_id': '30', 'item_id': '1000', 'value': 123.4}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_ctrl', data=json.dumps(ctrl_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertAlmostEqual(rst['value'], 123.4, delta=0.0001)
