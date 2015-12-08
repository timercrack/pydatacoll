import asyncio
import functools

import aioredis
import asynctest
import redis

from protocols.IEC104.device import IEC104Device
from protocols.IEC104.frame import *
from test.mock_device.iec104device import IEC104Device as MockDevice
from utils import logger as my_logger

logger = my_logger.getLogger('IEC104DeviceTest')


class IEC104DeviceTest(asynctest.TestCase):
    def setUp(self):
        super(IEC104DeviceTest, self).setUp()
        self.redis_pool = asyncio.get_event_loop().run_until_complete(
            functools.partial(aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=10, encoding='utf-8')())
        self.device_list = [
            {'id': 1, 'name': '测试集中器1', 'status': 'on', 'ip': '127.0.0.1', 'port': 2404,
             'identify': '111', 'protocol': 'iec104'},
            {'id': 2, 'name': '测试集中器2', 'status': 'on', 'ip': '127.0.0.1', 'port': 2405,
             'identify': '222', 'protocol': 'iec104'},
            {'id': 3, 'name': '测试集中器3', 'status': 'off', 'ip': '127.0.0.1', 'port': 2406,
             'identify': '333', 'protocol': 'iec104'},
            {'id': 4, 'name': '测试集中器4', 'status': 'off', 'ip': '127.0.0.1', 'port': 2407,
             'identify': '444', 'protocol': 'iec104'}
        ]
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        self.redis_client.flushdb()
        self.server_list = []
        for device in self.device_list:
            self.redis_client.hmset('HS:DEVICE:{}'.format(device['id']), device)
            self.server_list.append(
                self.loop.run_until_complete(self.loop.create_server(MockDevice, '127.0.0.1', device['port'])))

    def tearDown(self):
        self.loop.run_until_complete(self.redis_pool.clear())
        for server in self.server_list:
            server.close()
            self.loop.run_until_complete(server.wait_closed())

    async def test_connect(self):
        device = IEC104Device(self.loop, self.redis_pool, self.device_list[0])
        await asyncio.sleep(3)
        self.assertEqual(device.connected, True)
        status = self.redis_client.hget('HS:DEVICE:1', 'status')
        self.assertEqual(status, 'on')
        self.assertEqual(self.redis_client.llen('LST:FRAME:1'), 2)
        recv_frame = MockDevice.frame_list[1][0]
        self.assertEqual(recv_frame[0], 'recv')
        self.assertEqual(recv_frame[1].APCI1, UFrame.STARTDT_ACT)
        recv_frame = self.redis_client.rpop('LST:FRAME:1').split(',')
        self.assertEqual(recv_frame[1], 'recv')
        self.assertEqual(recv_frame[2], '68040b000000')  # STARTDT_CON

        device.disconnect(reconnect=True)
        self.assertEqual(device.user_canceled, False)
        self.assertEqual(device.connected, False)
        status = self.redis_client.hget('HS:DEVICE:1', 'status')
        self.assertEqual(status, 'off')
        await asyncio.sleep(4)
        self.assertEqual(device.connected, True)
        self.assertEqual(device.connect_retry_count, 1)
        status = self.redis_client.hget('HS:DEVICE:1', 'status')
        self.assertEqual(status, 'on')

        device.disconnect()
        self.assertEqual(device.user_canceled, True)

        wrong_device = IEC104Device(self.loop, self.redis_pool, {'id': 9, 'ip': '127.0.0.1', 'port': 9999})
        await asyncio.sleep(7)
        self.assertEqual(wrong_device.connect_retry_count, 2)
        device.disconnect()

    async def test_time_sync(self):
        device = IEC104Device(self.loop, self.redis_pool, self.device_list[1])
        await asyncio.sleep(3)
        send_data = iec_104.init_frame(device.ssn, device.rsn, TYP.C_CS_NA_1, Cause.act)  # 103 时钟同步命令
        await device.send_frame(send_data)
        self.assertEqual(device.send_list[0].ASDU.TYP, TYP.C_CS_NA_1)
        await asyncio.sleep(2)
        self.assertEqual(self.redis_client.llen('LST:FRAME:2'), 4)
        recv_frame = MockDevice.frame_list[2][2]
        self.assertEqual(recv_frame[1].ASDU.TYP, TYP.C_CS_NA_1)
        recv_frame = self.redis_client.lindex('LST:FRAME:2', -1).split(',')
        self.assertEqual(recv_frame[1], 'recv')
        recv_frame = iec_104.parse(bytearray.fromhex(recv_frame[2]))
        self.assertEqual(recv_frame.ASDU.TYP, TYP.C_CS_NA_1)
        self.assertEqual(recv_frame.ASDU.Cause, Cause.actcon)
        device.disconnect()

    async def test_call_all(self):
        for code in range(100):
            self.redis_client.hmset('HS:MAPPING:IEC104:3:{}'.format(code),
                                    {'term_id': 10, 'item_id': 20, 'protocol_code': code, 'code_type': code})
            self.redis_client.hmset('HS:TERM_ITEM:10:20',
                                    {'term_id': 10, 'item_id': 20, 'protocol_code': code, 'code_type': code})
        device = IEC104Device(self.loop, self.redis_pool, self.device_list[2])
        await asyncio.sleep(3)
        # 100 总召唤
        send_data = iec_104.init_frame(device.ssn, device.rsn, TYP.C_IC_NA_1, Cause.act)
        await device.send_frame(send_data)
        self.assertEqual(device.send_list[0].ASDU.TYP, TYP.C_IC_NA_1)
        await asyncio.sleep(3)
        self.assertEqual(len(MockDevice.frame_list[3]), 35)  # 2U + 3S + 3I(call_all) + 27I(all data) = 35
        device.disconnect()

    async def test_call_power(self):
        for code in range(10):
            self.redis_client.hmset('HS:MAPPING:IEC104:4:{}'.format(code),
                                    {'term_id': 10, 'item_id': 20, 'protocol_code': code, 'code_type': 15})
            self.redis_client.hmset('HS:TERM_ITEM:10:20',
                                    {'term_id': 10, 'item_id': 20, 'protocol_code': code, 'code_type': 15})
        device = IEC104Device(self.loop, self.redis_pool, self.device_list[3])
        await asyncio.sleep(3)
        # 101 电能量召唤
        send_data = iec_104.init_frame(device.ssn, device.rsn, TYP.C_CI_NA_1, Cause.act)
        await device.send_frame(send_data)
        self.assertEqual(device.send_list[0].ASDU.TYP, TYP.C_CI_NA_1)
        await asyncio.sleep(3)
        self.assertEqual(len(MockDevice.frame_list[4]), 16)  # 2U + 1S + 3I(call_power) + 10I(power data) = 16
        device.disconnect()

    async def test_send_data(self):
        self.redis_client.hmset('HS:MAPPING:IEC104:1:100',
                                {'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 63})
        self.redis_client.hmset('HS:TERM_ITEM:10:20',
                                {'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 63})
        device = IEC104Device(self.loop, self.redis_pool, self.device_list[0])
        await asyncio.sleep(2)
        with (await self.redis_pool) as sub_client:
            res = await sub_client.subscribe('CHANNEL:DEVICE_CTRL:1:10:20')
            ch1 = res[0]
            cb = asyncio.futures.Future()

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get_json()
                    cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(ch1))
            await device.ctrl_data(10, 20, 123.4)
            rst = await cb
            await sub_client.unsubscribe('CHANNEL:DEVICE_CTRL:1:10:20')
            await tsk

        self.assertAlmostEqual(rst[1], 123.4, delta=0.0001)
        device.disconnect()

    async def test_call_data(self):
        self.redis_client.hmset('HS:MAPPING:IEC104:1:100',
                                {'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 36})
        self.redis_client.hmset('HS:TERM_ITEM:10:20',
                                {'term_id': 10, 'item_id': 20, 'protocol_code': 100, 'code_type': 36})
        device = IEC104Device(self.loop, self.redis_pool, self.device_list[0])
        await asyncio.sleep(2)
        with (await self.redis_pool) as sub_client:
            res = await sub_client.subscribe('CHANNEL:DEVICE_CALL:1:10:20')
            cb = asyncio.futures.Future()

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get_json()
                    logger.debug('got msg: %s', msg)
                    cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(res[0]))
            await device.call_data(10, 20)
            rst = await cb
            await sub_client.unsubscribe('CHANNEL:DEVICE_CALL:1:10:20')
            await tsk

        self.assertEqual(rst[1], 123)
        device.disconnect()
