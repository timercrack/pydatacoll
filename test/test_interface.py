import asyncio
import datetime
import random
import aioredis
import aiohttp
import asynctest
import redis
import ujson as json
# from plugins.device_manage import DeviceManager
# from test.mock_device.iec104device import IEC104Device as MockDevice
from utils import logger as my_logger
from resources.protocol import *

logger = my_logger.getLogger('TestInterface')


@asynctest.skip
class RedisTest(asynctest.TestCase):
    async def test_connect_timeout(self):
        try:
            loop = asyncio.get_event_loop()
            reader, writer = await asyncio.open_connection('127.0.0.1', 6379)
            loop.call_later(1, lambda w: w.close(), writer)
            data = await reader.readexactly(100)
            print('Received: %r' % data.decode())
            # print("data={}".format(data))
            self.assertEqual(len(data), 0)
        except asyncio.IncompleteReadError:
            print('stream closed!')
        except Exception as e:
            print('e=', repr(e))

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
    """
    测试之前先开启三个独立的shell，都进入coll_api的根目录,三个shell分别运行如下命令（按照先后顺序）：
    python api_server.py
    python -m test.mock_device.iec104device
    python -m plugins.device_manage
    """
    use_default_loop = True

    @classmethod
    def setUpClass(cls):
        """
        模拟数据结构：
            设备1：（在线）
                终端10：（在线）
                    指标1000（104地址=100） 、 指标2000（104地址=200）
                终端20：（在线）
                    指标1000（104地址=300）
            设备2：（在线）
                终端30：（离线）
                    指标1000（104地址=100）
            设备3：（离线）
                无
        """
        # add new
        cls.test_device = {'id': '4', 'name': '测试集中器4', 'ip': '127.0.0.1', 'port': '2407', 'identify': '444',
                           'protocol': 'iec104'}
        cls.test_term = {'id': '40', 'name': '测试终端4', 'address': '44', 'identify': 'term40',
                         'protocol': 'dlt645', 'device_id': '1'}
        cls.test_item = {'id': '3000', 'name': 'C相电压', 'view_code': '3000', 'func_type': '遥测量'}
        cls.test_term_item = {'id': '5', 'term_id': '20', 'item_id': '2000', 'protocol_code': '400', 'base_val': '0',
                              'coefficient': '1'}

        cls.device1 = {'id': '1', 'name': '测试集中器1', 'ip': '127.0.0.1', 'port': '2404',
                       'identify': '111', 'protocol': 'iec104'}
        cls.device2 = {'id': '2', 'name': '测试集中器2', 'ip': '127.0.0.1', 'port': '2405',
                       'identify': '222', 'protocol': 'iec104'}
        cls.device3 = {'id': '3', 'name': '测试集中器3', 'ip': '127.0.0.1', 'port': '2406',
                       'identify': '333', 'protocol': 'iec104'}
        cls.device_list = [cls.device1, cls.device2, cls.device3, cls.test_device]
        cls.term10 = {'id': '10', 'name': '测试终端1', 'address': '5', 'identify': 'term10', 'protocol': 'dlt645',
                      'device_id': '1'}
        cls.term20 = {'id': '20', 'name': '测试终端2', 'address': '6', 'identify': 'term20', 'protocol': 'dlt645',
                      'device_id': '1'}
        cls.term30 = {'id': '30', 'name': '测试终端3', 'address': '7', 'identify': 'term30', 'protocol': 'dlt645',
                      'device_id': '2'}
        cls.item1000 = {'id': '1000', 'name': 'A相电压', 'view_code': '1000', 'func_type': '遥测量'}
        cls.item2000 = {'id': '2000', 'name': '继电器开关', 'view_code': '200', 'func_type': '遥控量'}
        cls.term10_item1000 = {'id': '1', 'term_id': '10', 'item_id': '1000', 'protocol_code': '100', 'code_type': '36',
                               'base_val': '0', 'coefficient': '1'}
        cls.term10_item2000 = {'id': '2', 'term_id': '10', 'item_id': '2000', 'protocol_code': '200', 'code_type': '63',
                               'base_val': '0', 'coefficient': '1'}
        cls.term20_item1000 = {'id': '3', 'term_id': '20', 'item_id': '1000', 'protocol_code': '100', 'code_type': '63',
                               'base_val': '0', 'coefficient': '1'}
        cls.term30_item1000 = {'id': '4', 'term_id': '30', 'item_id': '1000', 'protocol_code': '100', 'code_type': '63',
                               'base_val': '0', 'coefficient': '1'}
        cls.device1_term10_item1000 = []
        cls.device1_term10_item2000 = []
        cls.device1_term20_item1000 = []
        cls.device2_term30_item1000 = []
        cls.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        cls.value_count = 3
        cls.redis_client.flushdb()
        cls.redis_client.hmset('HS:DEVICE:1', cls.device1)
        cls.redis_client.hmset('HS:DEVICE:2', cls.device2)
        cls.redis_client.hmset('HS:DEVICE:3', cls.device3)
        cls.redis_client.hmset('HS:TERM:10', cls.term10)
        cls.redis_client.hmset('HS:TERM:20', cls.term20)
        cls.redis_client.hmset('HS:TERM:30', cls.term30)
        cls.redis_client.hmset('HS:ITEM:1000', cls.item1000)
        cls.redis_client.hmset('HS:ITEM:2000', cls.item2000)
        cls.redis_client.sadd('SET:DEVICE', 1, 2, 3)
        cls.redis_client.sadd('SET:TERM', 10, 20, 30)
        cls.redis_client.sadd('SET:ITEM', 1000, 2000)
        cls.redis_client.sadd('SET:DEVICE_TERM:1', 10, 20)
        cls.redis_client.sadd('SET:DEVICE_TERM:2', 30)
        cls.redis_client.sadd('SET:TERM_ITEM:10', 1000, 2000)
        cls.redis_client.sadd('SET:TERM_ITEM:20', 1000)
        cls.redis_client.sadd('SET:TERM_ITEM:30', 1000)
        cls.redis_client.hmset('HS:TERM_ITEM:10:1000', cls.term10_item1000)
        cls.redis_client.hmset('HS:TERM_ITEM:10:2000', cls.term10_item2000)
        cls.redis_client.hmset('HS:TERM_ITEM:20:1000', cls.term20_item1000)
        cls.redis_client.hmset('HS:TERM_ITEM:30:1000', cls.term30_item1000)
        cls.redis_client.hmset('HS:MAPPING:IEC104:{DEVICE_ID}:{PROTOCOL_CODE}'.format(
            DEVICE_ID=1, PROTOCOL_CODE=100), cls.term10_item1000)
        cls.redis_client.hmset('HS:MAPPING:IEC104:{DEVICE_ID}:{PROTOCOL_CODE}'.format(
            DEVICE_ID=1, PROTOCOL_CODE=200), cls.term10_item2000)
        cls.redis_client.hmset('HS:MAPPING:IEC104:{DEVICE_ID}:{PROTOCOL_CODE}'.format(
            DEVICE_ID=1, PROTOCOL_CODE=300), cls.term20_item1000)
        cls.redis_client.hmset('HS:MAPPING:IEC104:{DEVICE_ID}:{PROTOCOL_CODE}'.format(
            DEVICE_ID=2, PROTOCOL_CODE=100), cls.term30_item1000)

        cls.device1_term10_item1000.clear()
        cls.device1_term10_item2000.clear()
        cls.device1_term20_item1000.clear()
        cls.device2_term30_item1000.clear()
        data_time = datetime.datetime.now()
        for idx in range(cls.value_count):
            data_time += datetime.timedelta(seconds=1)
            data_time_str = data_time.isoformat()
            data_value1 = random.uniform(200, 230)
            data_value2 = random.randint(0, 1)
            cls.redis_client.rpush("LST:DATA:1:10:1000", json.dumps((data_time_str, data_value1)))
            cls.redis_client.rpush("LST:DATA:1:10:2000", json.dumps((data_time_str, data_value2)))
            cls.redis_client.rpush("LST:DATA:1:20:1000", json.dumps((data_time_str, data_value1)))
            cls.redis_client.rpush("LST:DATA:2:30:1000", json.dumps((data_time_str, data_value1)))
            cls.device1_term10_item2000.append(json.dumps((data_time_str, data_value1)))
            cls.device1_term20_item1000.append(json.dumps((data_time_str, data_value2)))
            cls.device1_term10_item1000.append(json.dumps((data_time_str, data_value1)))
            cls.device2_term30_item1000.append(json.dumps((data_time_str, data_value1)))
        # cls.server_list = []
        # for device in cls.device_list:
        #     cls.redis_client.hmset('HS:DEVICE:{}'.format(device['id']), device)
        #     cls.server_list.append(
        #         asyncio.get_event_loop().run_until_complete(
        #             asyncio.get_event_loop().create_server(MockDevice, '127.0.0.1', device['port'])))
        # # cls.api_server = Process(target=run_server)
        # cls.device_manager = DeviceManager()
        # # cls.api_server.start()
        # # time.sleep(3)
        # asyncio.get_event_loop().run_until_complete(cls.device_manager.install())

    # @classmethod
    # def tearDownClass(cls):
    #     asyncio.get_event_loop().run_until_complete(cls.device_manager.uninstall())
    #     # cls.api_server.terminate()
    #     # cls.api_server.join()

    async def test_get_protocol_list(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/device_protocols') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, DEVICE_PROTOCOLS)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/term_protocols') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, TERM_PROTOCOLS)

    async def test_device_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertSequenceEqual(rst, ['1', '2', '3'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, self.device1)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/99') as r:
            self.assertEqual(r.status, 404)
            rst = await r.text()
            self.assertEqual(rst, 'device_id not found!')

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/devices', data=json.dumps(self.test_device)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:DEVICE:4')
            self.assertEqual(rst['name'], '测试集中器4')
            rst = self.redis_client.sismember('SET:DEVICE', 4)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/devices', data=json.dumps(self.test_device)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'device already exists!')

        self.test_device['name'] = '测试集中器5'
        self.test_device['id'] = 5
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/devices/4', data=json.dumps(self.test_device)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:DEVICE:4')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:DEVICE', 4)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:DEVICE:5')
            self.assertEqual(rst['name'], '测试集中器5')
            rst = self.redis_client.sismember('SET:DEVICE', 5)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/devices/99', data=json.dumps(self.test_device)) as r:
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
            self.assertSequenceEqual(rst, ['10', '20', '30'])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/10') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, self.term10)
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

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms', data=json.dumps(self.test_term)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:TERM:40')
            self.assertEqual(rst['name'], '测试终端4')
            rst = self.redis_client.sismember('SET:TERM', 40)
            self.assertTrue(rst)
            rst = self.redis_client.sismember('SET:DEVICE_TERM:1', 40)
            self.assertEqual(rst, True)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms', data=json.dumps(self.test_term)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'term already exists!')

        self.test_term['name'] = '测试终端5'
        self.test_term['id'] = 50
        self.test_term['device_id'] = 2
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/40', data=json.dumps(self.test_term)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:TERM:40')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:TERM', 40)
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:DEVICE_TERM:1', 40)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:TERM:50')
            self.assertEqual(rst['name'], '测试终端5')
            rst = self.redis_client.sismember('SET:TERM', 50)
            self.assertTrue(rst)
            rst = self.redis_client.sismember('SET:DEVICE_TERM:2', 50)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/99', data=json.dumps(self.test_term)) as r:
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
            self.assertDictEqual(rst, self.item1000)
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

        async with aiohttp.post('http://127.0.0.1:8080/api/v1/items', data=json.dumps(self.test_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:ITEM:3000')
            self.assertEqual(rst['name'], 'C相电压')
            rst = self.redis_client.sismember('SET:ITEM', 3000)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/items', data=json.dumps(self.test_item)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'item already exists!')

        self.test_item['name'] = '功率因数'
        self.test_item['id'] = 4000
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/items/3000', data=json.dumps(self.test_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.exists('HS:ITEM:3000')
            self.assertFalse(rst)
            rst = self.redis_client.sismember('SET:ITEM', 3000)
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:ITEM:4000')
            self.assertEqual(rst['name'], '功率因数')
            rst = self.redis_client.sismember('SET:ITEM', 4000)
            self.assertTrue(rst)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/items/99', data=json.dumps(self.test_item)) as r:
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
            self.assertSequenceEqual(rst, self.device1_term10_item2000)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/1/terms/10/items/1000/datas/-1') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertEqual(rst, self.device1_term10_item2000[-1])
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/devices/99/terms/99/items/99/datas') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertEqual(len(rst), 0)

    async def test_term_item_CRUD(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/terms/10/items/1000') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, self.term10_item1000)
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
                                data=json.dumps(self.test_term_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.hgetall('HS:TERM_ITEM:20:2000')
            self.test_term_item.update({'device_id': '1'})
            self.assertDictEqual(rst, self.test_term_item)
            rst = self.redis_client.hgetall('HS:MAPPING:IEC104:1:400')
            self.assertDictEqual(rst, self.test_term_item)
            rst = self.redis_client.sismember('SET:TERM_ITEM:20', 2000)
            self.assertTrue(rst)
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms/20/items',
                                data=json.dumps(self.test_term_item)) as r:
            self.assertEqual(r.status, 409)
            rst = await r.text()
            self.assertEqual(rst, 'term_item already exists!')

        self.test_term_item['protocol_code'] = '401'
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/20/items/2000',
                               data=json.dumps(self.test_term_item)) as r:
            self.assertEqual(r.status, 200)
            rst = self.redis_client.sismember('SET:TERM_ITEM:20', 2000)
            self.assertTrue(rst)
            rst = self.redis_client.exists('HS:MAPPING:IEC104:1:400')
            self.assertFalse(rst)
            rst = self.redis_client.hgetall('HS:TERM_ITEM:20:2000')
            self.assertEqual(rst['protocol_code'], '401')
            rst = self.redis_client.hgetall('HS:MAPPING:IEC104:1:401')
            self.assertDictEqual(rst, self.test_term_item)
        async with aiohttp.put('http://127.0.0.1:8080/api/v1/terms/30/items/2000',
                               data=json.dumps(self.test_term_item)) as r:
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
        del self.test_term_item['device_id']

    async def test_device_call(self):
        call_dict = {'device_id': '1', 'term_id': '10', 'item_id': 1000}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_call', data=json.dumps(call_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertEqual(rst[1], 123)

    async def test_device_ctrl(self):
        ctrl_dict = {'device_id': '2', 'term_id': '30', 'item_id': '1000', 'value': 123.4}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_ctrl', data=json.dumps(ctrl_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertAlmostEqual(rst[1], 123.4, delta=0.0001)
