import asyncio
import functools
from abc import abstractmethod, ABCMeta
import aioredis

import pydatacoll.utils.logger as my_logger
from pydatacoll.utils.func_container import ParamFunctionContainer

logger = my_logger.get_logger('BaseModule')


class BaseModule(ParamFunctionContainer, metaclass=ABCMeta):
    def __init__(self, io_loop: asyncio.AbstractEventLoop = None,
                 redis_pool: aioredis.RedisPool = None):
        super().__init__()
        self.io_loop = io_loop or asyncio.get_event_loop()
        self._redis_pool = redis_pool
        self.redis_pool = redis_pool or self.io_loop.run_until_complete(
                functools.partial(
                        aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=20, encoding='utf-8')())
        self.initialized = False
        self.sub_client = None
        self.sub_channels = list()
        self.channel_router = dict()
        self._register_channel()
        # logger.info('plugin %s initialized', type(self).__name__)

    def _register_channel(self):
        for fun_name, args in self.module_arg_dict.items():
            if 'channel' not in args:
                raise Exception("wrong param_function prototype, need param: 'channel'")
            self.channel_router[args['channel']] = getattr(self, fun_name)

    async def install(self):
        try:
            self.sub_client = await self.redis_pool.acquire()
            self.sub_channels = await self.sub_client.psubscribe(*[a['channel'] for a in self.module_arg_dict.values()])
            for channel in self.sub_channels:
                asyncio.ensure_future(self._msg_reader(channel), loop=self.io_loop)
            await self.start()
            self.initialized = True
            logger.info('%s plugin installed', type(self).__name__)
        except Exception as e:
            logger.error('%s plugin install failed: %s', type(self).__name__, repr(e), exc_info=True)

    async def uninstall(self):
        try:
            await self.stop()
            await self.sub_client.punsubscribe(*[a['channel'] for a in self.module_arg_dict.values()])
            self.redis_pool.release(self.sub_client)
            if self._redis_pool is None:  # release the pool created by self
                await self.redis_pool.clear()
            self.initialized = False
            logger.info('%s plugin uninstalled', type(self).__name__)
        except Exception as e:
            logger.error('%s plugin uninstall failed: %s', type(self).__name__, repr(e), exc_info=True)

    async def _msg_reader(self, ch):
        while await ch.wait_message():
            real_channel, msg = await ch.get_json()
            channel = ch.name.decode()
            # logger.debug("%s channel[%s] Got Message:%s", type(self).__name__, channel, msg)
            self.io_loop.create_task(self.channel_router[channel](real_channel, msg))
        logger.debug('%s quit msg_reader!', type(self).__name__)

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass
