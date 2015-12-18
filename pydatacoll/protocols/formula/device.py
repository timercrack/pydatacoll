import asyncio
import aioredis

from pydatacoll.protocols import BaseDevice
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('FORMULADevice')


class FORMULADevice(BaseDevice):
    def __init__(self, device_info: dict, io_loop: asyncio.AbstractEventLoop,
                 redis_pool: aioredis.RedisPool):
        super(FORMULADevice, self).__init__(device_info, io_loop, redis_pool)

    def fresh_task(self, term_dict, term_item_dict, delete=False):
        pass

    def send_frame(self, frame, check=True):
        pass

    def disconnect(self, reconnect=False):
        pass

    def prepare_ctrl_frame(self, term_item_dict, value):
        pass

    def prepare_call_frame(self, term_item_dict):
        pass
