import asyncio

import aioredis

from protocols.IEC104.device import IEC104Device
from protocols import logger as my_logger

logger = my_logger.getLogger('DeviceManager')


class DataChecker(object):
    def __init__(self, io_loop: asyncio.base_events.BaseEventLoop, redis_pool: aioredis.RedisPool):
        self.io_loop = io_loop
        self.redis_pool = redis_pool
        self.initialized = False
        self.sub_client = None
        self.sub_channels = None
        self.sub_task = None

    async def install(self):
        try:
            self.sub_client = await self.redis_pool.acquire()
            self.sub_channels = await self.sub_client.subscribe(
                "CHANNEL:DEVICE_ADD", "CHANNEL:DEVICE_DEL", "CHANNEL:DEVICE_FRESH",
                "CHANNEL:TERM_ADD", "CHANNEL:TERM_DEL", "CHANNEL:TERM_FRESH",
                "CHANNEL:TERM_ITEM_ADD", "CHANNEL:TERM_ITEM_DEL", "CHANNEL:TERM_ITEM_FRESH",
                "CHANNEL:DEVICE_CALL", "CHANNEL:DEVICE_CTRL", "CHANNEL:SYSTEM")
            for channel in self.sub_channels:
                asyncio.ensure_future(self.msg_reader(channel))
            self.init_devices()
            self.initialized = True
        except Exception as e:
            logger.error('install failed: %s', repr(e))

    async def uninstall(self):
        try:
            await self.sub_client.unsubscribe(
                "CHANNEL:DEVICE_ADD", "CHANNEL:DEVICE_DEL", "CHANNEL:DEVICE_FRESH",
                "CHANNEL:TERM_ADD", "CHANNEL:TERM_DEL", "CHANNEL:TERM_FRESH",
                "CHANNEL:TERM_ITEM_ADD", "CHANNEL:TERM_ITEM_DEL", "CHANNEL:TERM_ITEM_FRESH",
                "CHANNEL:DEVICE_CALL", "CHANNEL:DEVICE_CTRL", "CHANNEL:SYSTEM")
            self.redis_pool.release(self.sub_client)
            self.del_device()
            self.initialized = False
        except Exception as e:
            logger.error('uninstall failed: %s', repr(e))

    async def msg_reader(self, ch):
        while await ch.wait_message():
            msg = await ch.get_json()
            channel = ch.name.decode()
            logger.debug("channel[%s] Got Message:%s", channel, msg)
            if channel in ("CHANNEL:DEVICE_FRESH", "CHANNEL:DEVICE_ADD"):
                self.io_loop.create_task(self.fresh_device(msg['id']))
            elif channel == "CHANNEL:DEVICE_DEL":
                self.del_device(msg['id'])
            elif channel in ("CHANNEL:TERM_FRESH", "CHANNEL:TERM_ADD"):
                self.fresh_term(msg)
            elif channel == "CHANNEL:TERM_DEL":
                self.del_term(msg)
            elif channel in ("CHANNEL:TERM_ITEM_FRESH", "CHANNEL:TERM_ITEM_ADD"):
                self.fresh_term_item(msg)
            elif channel == "CHANNEL:TERM_ITEM_DEL":
                self.del_term_item(msg)
            elif channel == "CHANNEL:CALL_DATA":
                self.io_loop.create_task(self.device_call(msg))
            elif channel == "CHANNEL:SEND_DATA":
                self.io_loop.create_task(self.device_ctrl(msg))
            elif channel == 'CHANNEL:SYSTEM':
                await self.uninstall()
        logger.debug('quit msg_reader!')

    async def init_devices(self):
        try:
            redis = await self.redis_pool.acquire()
            device_list = await redis.smembers('SET:DEVICE')
            self.redis_pool.release(redis)
            for device_id in device_list:
                self.fresh_device(device_id)
        except Exception as e:
            logger.exception('init_devices failed: %s', repr(e))

    def del_device(self, device_id=None):
        try:
            if device_id is None:
                for device in self.device_list.values():
                    device.disconnect()
                self.device_list.clear()
                return

            if device_id in self.device_list:
                device = self.device_list.pop(device_id)
                device.disconnect()
        except Exception as e:
            logger.exception('del_device failed: %s', repr(e))

    async def fresh_device(self, device_id):
        try:
            with (await self.redis_pool) as redis:
                device_info = await redis.hgetall('HS:DEVICE:{}'.format(device_id))
                device = self.device_list.get(device_id)
                if device is not None:
                    if device.info != device_info:
                        await self.device_list.pop(device_id).device.disconnect()
                    else:
                        return
                protocol = device_info['protocols']
                # TODO 增加协议支持
                if protocol == "iec104":
                    device = IEC104Device(self.io_loop, self.redis_pool, device_info)
                elif protocol == "gdw130":
                    device = IEC104Device(self.io_loop, self.redis_pool, device_info)
                else:
                    logger.error("unknown device protocols: %s", protocol)
                    return

                self.device_list[device_id] = device
        except Exception as e:
            logger.exception('fresh_device failed: %s', repr(e))

    def fresh_term(self, call_dict):
        device = self.device_list.get(call_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=call_dict['term_id'])

    def del_term(self, call_dict):
        device = self.device_list.get(call_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=call_dict['term_id'], item_id=None, delete=True)

    def fresh_term_item(self, call_dict):
        device = self.device_list.get(call_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=call_dict['term_id'], item_id=call_dict['item_id'])

    def del_term_item(self, call_dict):
        device = self.device_list.get(call_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=call_dict['term_id'], item_id=call_dict['item_id'], delete=True)

    async def device_call(self, call_dict):
        try:
            device_id = call_dict['device_id']
            term_id = call_dict['term_id']
            item_id = call_dict['item_id']
            device = self.device_list.get(device_id)
            await device.device_call(term_id, item_id)
        except Exception as e:
            logger.exception('call_data failed: %s', repr(e))

    async def device_ctrl(self, call_dict):
        try:
            device_id = call_dict['device_id']
            term_id = call_dict['term_id']
            item_id = call_dict['item_id']
            value = call_dict['value']
            device = self.device_list.get(device_id)
            await device.ctrl_data(term_id, item_id, value)
        except Exception as e:
            logger.exception('ctrl_data failed: %s', repr(e))
