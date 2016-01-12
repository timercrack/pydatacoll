import asyncio
import functools
import aioredis
import asynctest
import redis

import pydatacoll.utils.logger as my_logger
from pydatacoll.protocols.iec104.device import IEC104Device
from test.mock_device.iec104device import create_servers
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('LoadTest')


class LoadTest(asynctest.TestCase):
    loop = asyncio.get_event_loop()  # make pycharm happy

    def setUp(self):
        device_count = 1
        term_count = 2000
        item_count = 50
        self.redis_pool = self.loop.run_until_complete(
                functools.partial(aioredis.create_pool, (config.get('REDIS', 'host', fallback='127.0.0.1'),
                                                         config.getint('REDIS', 'port', fallback=6379)),
                                  db=config.getint('REDIS', 'db', fallback=1),
                                  minsize=config.getint('REDIS', 'minsize', fallback=5),
                                  maxsize=config.getint('REDIS', 'maxsize', fallback=10),
                                  encoding=config.get('REDIS', 'encoding', fallback='utf-8'))())
        s = redis.StrictRedis(db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        s.flushdb()
        for item_id in range(item_count):
            s.hmset('HS:ITEM:{}'.format(item_id), {'id': item_id, 'name': '测试指标{}'.format(item_id)})
            s.sadd("SET:ITEM", item_id)
        for device_id in range(device_count):
            s.hmset('HS:DEVICE:{}'.format(device_id), {
                'id': device_id, 'name': '测试集中器{}'.format(device_id), 'ip': '127.0.0.1',
                'port': device_id+2404, 'protocol': 'iec104'})
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
                    }
                    s.hmset('HS:TERM_ITEM:{}:{}'.format(term_id, item_id), term_item)
                    s.hmset('HS:MAPPING:IEC104:{}:{}'.format(device_id, term_item['protocol_code']), term_item)
                    s.sadd('SET:TERM_ITEM:{}'.format(term_id), item_id)
        self.server_list = list()
        self.server_list = create_servers(self.loop)

    def tearDown(self):
        self.loop.run_until_complete(self.redis_pool.clear())
        for server in self.server_list:
            server.close()
            self.loop.run_until_complete(server.wait_closed())

    async def test_heavy_load(self):
        device = IEC104Device({'id': 0, 'ip': '127.0.0.1', 'port': 2404, 'protocol': 'iec104'},
                              self.loop, self.redis_pool)
        await asyncio.sleep(2)
        await device.run_task()
        while device.coll_count < 1:
            await asyncio.sleep(1, self.loop)
        self.assertEqual(device.coll_count, 1)
