import asyncio
import aiohttp
import aioredis
import asynctest
import redis
try:
    import ujson as json
except ImportError:
    import json

import pydatacoll.utils.logger as my_logger
from pydatacoll.resources.protocol import *
from pydatacoll.resources.redis_key import *
from test.mock_device import mock_data, iec104device
from pydatacoll import api_server
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('TestInterface')


class RedisTest(asynctest.TestCase):
    async def test_connect_timeout(self):
        try:
            loop = asyncio.get_event_loop()
            reader, writer = await asyncio.open_connection(config.get('REDIS', 'host', fallback='127.0.0.1'),
                                                           config.getint('REDIS', 'port', fallback=6379), loop=loop)
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
        pub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='127.0.0.1'),
                                                  config.getint('REDIS', 'port', fallback=6379)),
                                                 db=config.getint('REDIS', 'db', fallback=1))
        sub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='127.0.0.1'),
                                                  config.getint('REDIS', 'port', fallback=6379)),
                                                 db=config.getint('REDIS', 'db', fallback=1))
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
    loop = None  # make pycharm happy

    def setUp(self):
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        mock_data.generate()
        self.server_list = list()
        self.server_list = iec104device.create_servers(self.loop)
        self.api_server = api_server.APIServer(io_loop=self.loop, port=8080)

    def tearDown(self):
        self.api_server.stop_server()
        for server in self.server_list:
            server.close()
            self.loop.run_until_complete(server.wait_closed())

    async def test_get_redis_key(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/redis_key') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst['channel'], REDIS_KEY['channel'])

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
            self.assertEqual(rst['formula'], mock_data.test_formula['formula'])
            rst = self.redis_client.sismember('SET:FORMULA', 9)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formulas', data=json.dumps(mock_data.test_formula)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'formula already exists!')

        mock_data.test_formula['formula'] = '2+2'
        mock_data.test_formula['id'] = 5
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/formulas/9', data=json.dumps(mock_data.test_formula)) as r:
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

        formula_check = {'formula': 'p1[-1]+10', 'p1': 'HS:DATA:1:10:1000'}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formula_check', data=json.dumps(formula_check)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.text()
            self.assertEqual(rst, 'OK')
        formula_check['formula'] = 'p1[-1]+p2[-2]'
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formula_check', data=json.dumps(formula_check)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.text()
            self.assertEqual(rst, """NameError
   p1[-1]+p2[-2]
           ^^^
name 'p2' is not defined
""")

        formulas = [1]
        async with aiohttp.post('http://127.0.0.1:8080/api/v2/formulas/del', data=json.dumps(formulas)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.smembers('SET:FORMULA')
            self.assertEqual(len(rst), 0)

    async def test_device_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSetEqual(set(rst), {'1', '2', '3'})
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

        devices = [1, 2, 3]
        async with aiohttp.post('http://127.0.0.1:8080/api/v2/devices/del', data=json.dumps(devices)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.smembers('SET:DEVICE')
            self.assertEqual(len(rst), 0)

    async def test_term_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSetEqual(set(rst), {'10', '20', '30', '40'})
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
            self.assertSetEqual(set(rst), {'10', '20'})
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

        terms = [10, 20, 30, 40]
        async with aiohttp.post('http://127.0.0.1:8080/api/v2/terms/del', data=json.dumps(terms)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.smembers('SET:TERM')
            self.assertEqual(len(rst), 0)

    async def test_item_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/items') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSetEqual(set(rst), {'1000', '2000'})
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
            self.assertSetEqual(set(rst), {'1000', '2000'})
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

        items = [1000, 2000]
        async with aiohttp.post('http://127.0.0.1:8080/api/v2/items/del', data=json.dumps(items)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.smembers('SET:ITEM')
            self.assertEqual(len(rst), 0)

    async def test_get_data(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1/terms/10/items/1000/datas') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, mock_data.device1_term10_item1000)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1/terms/10/items/1000/datas/-1') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, {'2015-12-01T08:50:15.000003': '102.0'})
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

        async with aiohttp.post('http://127.0.0.1:8080/api/v2/term_items/del',
                                  data=json.dumps(mock_data.term_item_list)) as r:
            self.assertEqual(r.status, 200)
            rst = list(self.redis_client.scan_iter('HS:TERM_ITEMS:*'))
            self.assertEqual(len(rst), 0)

    async def test_device_call(self):
        call_dict = {'device_id': '1', 'term_id': '10', 'item_id': 1000}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_call', data=json.dumps(call_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertAlmostEqual(rst['value'], 102, delta=0.0001)

    async def test_device_ctrl(self):
        ctrl_dict = {'device_id': '2', 'term_id': '30', 'item_id': '1000', 'value': 123.4}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_ctrl', data=json.dumps(ctrl_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertAlmostEqual(rst['value'], 123.4, delta=0.0001)
