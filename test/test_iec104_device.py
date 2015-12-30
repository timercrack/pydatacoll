import asyncio
import functools
import aioredis
import asynctest
import redis

import pydatacoll.utils.logger as my_logger
from pydatacoll.protocols.iec104.device import IEC104Device
from pydatacoll.protocols.iec104.frame import *
from test.mock_device.iec104device import IEC104Device as MockDevice
from test.mock_device import mock_data

logger = my_logger.get_logger('IEC104DeviceTest')


class IEC104DeviceTest(asynctest.TestCase):
    def setUp(self):
        super(IEC104DeviceTest, self).setUp()
        self.redis_pool = asyncio.get_event_loop().run_until_complete(
            functools.partial(aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=10, encoding='utf-8')())
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        self.server_list = list()
        mock_data.generate()
        for device in mock_data.device_list:
            if 'port' not in device:
                continue
            self.server_list.append(
                self.loop.run_until_complete(self.loop.create_server(MockDevice, '127.0.0.1', device['port'])))

    def tearDown(self):
        self.loop.run_until_complete(self.redis_pool.clear())
        for server in self.server_list:
            server.close()
            self.loop.run_until_complete(server.wait_closed())

    async def test_connect(self):
        device = IEC104Device(mock_data.device_list[0], self.loop, self.redis_pool)
        # asyncio.wait(device.reconnect_handler)
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
        # asyncio.wait(device.reconnect_handler)
        await asyncio.sleep(5)
        self.assertEqual(device.connected, True)
        self.assertEqual(device.connect_retry_count, 1)
        status = self.redis_client.hget('HS:DEVICE:1', 'status')
        self.assertEqual(status, 'on')

        device.disconnect()
        self.assertEqual(device.user_canceled, True)

        wrong_device = IEC104Device({'id': 9, 'ip': '127.0.0.1', 'port': 9999}, self.loop, self.redis_pool)
        await asyncio.sleep(6)
        self.assertEqual(wrong_device.connect_retry_count, 2)
        device.disconnect()

    async def test_time_sync(self):
        device = IEC104Device(mock_data.device_list[1], self.loop, self.redis_pool)
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
        device = IEC104Device(mock_data.device_list[0], self.loop, self.redis_pool)
        await asyncio.sleep(3)
        # 100 总召唤
        send_data = iec_104.init_frame(device.ssn, device.rsn, TYP.C_IC_NA_1, Cause.act)
        await device.send_frame(send_data)
        self.assertEqual(device.send_list[0].ASDU.TYP, TYP.C_IC_NA_1)
        await asyncio.sleep(3)
        self.assertEqual(len(MockDevice.frame_list[1]), 8)  # 2U + 3I(call_all) + 3(call_all_data) = 8
        device.disconnect()

    @asynctest.skip('skip unused mock method')
    async def test_call_power(self):
        for code in range(10):
            self.redis_client.hmset('HS:MAPPING:iec104:3:{}'.format(code),
                                    {'term_id': 10, 'item_id': 20, 'protocol_code': code, 'code_type': 15})
            self.redis_client.hmset('HS:TERM_ITEM:10:20',
                                    {'term_id': 10, 'item_id': 20, 'protocol_code': code, 'code_type': 15})
        device = IEC104Device(mock_data.device_list[0], self.loop, self.redis_pool)
        await asyncio.sleep(3)
        # 101 电能量召唤
        send_data = iec_104.init_frame(device.ssn, device.rsn, TYP.C_CI_NA_1, Cause.act)
        await device.send_frame(send_data)
        self.assertEqual(device.send_list[0].ASDU.TYP, TYP.C_CI_NA_1)
        await asyncio.sleep(3)
        self.assertEqual(len(MockDevice.frame_list[1]), 16)  # 2U + 1S + 3I(call_power) + 10I(power data) = 16
        device.disconnect()

    async def test_send_data(self):
        device = IEC104Device(mock_data.device_list[0], self.loop, self.redis_pool)
        await asyncio.sleep(2)
        with (await self.redis_pool) as sub_client:
            res = await sub_client.subscribe('CHANNEL:DEVICE_CTRL:1:20:1000')
            ch1 = res[0]
            cb = asyncio.futures.Future()

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get_json()
                    if not cb.done():
                        cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(ch1))
            await device.ctrl_data({'term_id': 20, 'item_id': 1000, 'value': 123.4})
            rst = await cb
            await sub_client.unsubscribe('CHANNEL:DEVICE_CTRL:1:20:1000')
            await tsk

        self.assertAlmostEqual(rst['value'], 123.4, delta=0.0001)
        device.disconnect()

    async def test_call_data(self):
        device = IEC104Device(mock_data.device_list[0], self.loop, self.redis_pool)
        await asyncio.sleep(2)
        with (await self.redis_pool) as sub_client:
            res = await sub_client.subscribe('CHANNEL:DEVICE_CALL:1:10:1000')
            cb = asyncio.futures.Future()

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get_json()
                    logger.debug('got msg: %s', msg)
                    if not cb.done():
                        cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(res[0]))
            await device.call_data({'term_id': 10, 'item_id': 1000})
            rst = await cb
            await sub_client.unsubscribe('CHANNEL:DEVICE_CALL:1:10:1000')
            await tsk

        self.assertAlmostEqual(rst['value'], 102, delta=0.0001)
        device.disconnect()
