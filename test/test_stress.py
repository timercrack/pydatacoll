import asyncio
import aiohttp
import aioredis
import asynctest
import functools
import redis
try:
    import ujson as json
except ImportError:
    import json

import pydatacoll.utils.logger as my_logger
from pydatacoll.resources.protocol import *
from test.mock_device import iec104device, mock_data
from pydatacoll import api_server

logger = my_logger.get_logger('TestStress')


class StressingTest(asynctest.TestCase):
    loop = None  # make pycharm happy

    def setUp(self):
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        mock_data.generate()
        self.server_list = list()
        for device in mock_data.device_list:
            if 'port' not in device:
                continue
            self.server_list.append(
                self.loop.run_until_complete(
                        self.loop.create_server(iec104device.IEC104Device, '127.0.0.1', device['port'])))
        self.api_server = api_server.APIServer(8080, self.loop)

    def tearDown(self):
        self.api_server.stop_server()
        for server in self.server_list:
            server.close()
            self.loop.run_until_complete(server.wait_closed())

    async def test_stress(self):
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/device_protocols') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, DEVICE_PROTOCOLS)
        async with aiohttp.get('http://127.0.0.1:8080/api/v1/term_protocols') as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertDictEqual(rst, TERM_PROTOCOLS)
        logger.debug("after test_stress")

    async def test_device_call(self):
        call_dict = {'device_id': '1', 'term_id': '10', 'item_id': 1000}
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/device_call', data=json.dumps(call_dict)) as r:
            self.assertEqual(r.status, 200)
            rst = await r.json()
            self.assertAlmostEqual(rst['value'], 102, delta=0.0001)
