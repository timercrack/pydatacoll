import asyncio
import datetime

import aioredis
import functools
import redis

try:
    import ujson as json
except ImportError:
    import json
from abc import ABCMeta, abstractmethod

from pydatacoll.utils import logger as my_logger

logger = my_logger.get_logger('BaseDevice')


class BaseDevice(object, metaclass=ABCMeta):
    def __init__(self, device_info: dict, io_loop: asyncio.AbstractEventLoop,
                 redis_pool: aioredis.RedisPool):
        self.connected = False
        self.device_info = device_info
        self.device_id = self.device_info['id']
        self.io_loop = io_loop or asyncio.get_event_loop()
        self.redis_pool = redis_pool or self.io_loop.run_until_complete(
                functools.partial(
                        aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=10, encoding='utf-8')())
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)

    async def save_frame(self, frame, send=True):
        try:
            with (await self.redis_pool) as redis_client:
                await redis_client.rpush("LST:FRAME:{}".format(self.device_id),
                                         '{time},{type},{frame}'.format(
                                                 time=datetime.datetime.now().isoformat(),
                                                 type="send" if send is True else "recv", frame=frame.hex()))
        except Exception as e:
            logger.error("device[%s] save_frame failed: %s", self.device_id, repr(e))

    # 召测
    async def call_data(self, call_dict):
        try:
            if not self.connected:
                raise Exception('device not connected!')
            term_id = call_dict['term_id']
            item_id = call_dict['item_id']
            with (await self.redis_pool) as redis_client:
                term_item = await redis_client.hgetall('HS:TERM_ITEM:{term_id}:{item_id}'.format(
                        term_id=term_id, item_id=item_id))
                logger.debug('device[%s] call_data, term_item=%s', self.device_id, term_item)
                if not term_item:
                    logger.error('device[%s] HS:TERM_ITEM:{%s}:{%s} not found!', self.device_id, term_id, item_id)
                    return
                frame = self.prepare_call_frame(term_item)
                await self.send_frame(frame)
        except Exception as e:
            logger.error('device[%s] call_data failed: %s', self.device_id, repr(e))

    # 控制
    async def ctrl_data(self, ctrl_dict):
        try:
            if not self.connected:
                raise Exception('device not connected!')
            term_id = ctrl_dict['term_id']
            item_id = ctrl_dict['item_id']
            value = ctrl_dict['value']
            with (await self.redis_pool) as redis_client:
                term_item = await redis_client.hgetall('HS:TERM_ITEM:{term_id}:{item_id}'.format(
                                term_id=term_id, item_id=item_id))
                logger.debug('device[%s] ctrl_data, term_item=%s, value=%s', self.device_id, term_item, value)
                if not term_item:
                    logger.error('device[%s] HS:TERM_ITEM:{%s}:{%s} not found!', self.device_id, term_id, item_id)
                    return
                frame = self.prepare_ctrl_frame(term_item, value)
                await self.send_frame(frame)
        except Exception as e:
            logger.error('device[%s] ctrl_data failed: %s', self.device_id, repr(e))

    # TODO: fixme
    def change_device_status(self, on_line):
        """
        :param on_line: boolean
        :return: None
        """
        if self.redis_client.exists('HS:DEVICE:{}'.format(self.device_id)):
            self.redis_client.hset('HS:DEVICE:{}'.format(self.device_id), 'status', 'on' if on_line else 'off')
        self.connected = on_line

    async def process_data(self, data_pairs, method='data'):
        """
        :param data_pairs: data tuple->(time, protocol_code, value)
        :param method: data process method: 'normal', 'call', 'ctrl'
        :return: None
        """
        if not data_pairs:
            return
        try:
            with (await self.redis_pool) as redis_client:
                for data_time, protocol_code, data_value in data_pairs:
                    map_key = 'HS:MAPPING:{}:{}:{}'.format(self.device_info['protocol'].upper(),
                                                           self.device_id, protocol_code)
                    term_item = await redis_client.hgetall(map_key)
                    if not term_item:
                        logger.debug("DEVICE[%s] precess_data: can't found term_item, key=%s", self.device_id, map_key)
                        continue
                    if 'coefficient' in term_item and 'base_val' in term_item:
                        data_value = data_value * float(term_item['coefficient']) + float(term_item['base_val'])
                    json_data = json.dumps({
                        'device_id': self.device_id, 'term_id': term_item['term_id'], 'item_id': term_item['item_id'],
                        'time': data_time.isoformat(), 'value': data_value,
                    })
                    pub_channel = 'CHANNEL:DEVICE_{}:{}:{}:{}'.format(
                            method.upper(), self.device_id, term_item['term_id'], term_item['item_id'])
                    if method == 'data':
                        data_key = "{}:{}:{}".format(
                                self.device_id, term_item['term_id'], term_item['item_id'])
                        time_str = data_time.isoformat()
                        await redis_client.hset("HS:DATA:{}".format(data_key), time_str, data_value)
                        await redis_client.rpush("LST:DATA_TIME:{}".format(data_key), time_str)
                        # if check_result != 'OK':
                        #     warn_msg = json.dumps(
                        #         {'warn_msg': check_result, 'device_id': self.device_id, 'term_id': term_item['term_id'],
                        #          'item_id': term_item['item_id'], 'time': data_time, 'value': data_value})
                        #     await redis_client.publish('CHANNEL:WARNING', warn_msg)
                    rst = await redis_client.publish(pub_channel, json_data)
                    logger.debug('pub to %s, val=%s, rst=%s', pub_channel, json_data, rst)
        except Exception as e:
            logger.exception(e)

    @abstractmethod
    async def send_frame(self, frame, check=True):
        """
        :param frame: frame send to remote device
        :param check: directly send or save to send buffer
        :return: None
        """
        pass

    @abstractmethod
    def fresh_task(self, term_dict, term_item_dict, delete=False):
        pass

    @abstractmethod
    def prepare_call_frame(self, term_item_dict):
        """
        :param term_item_dict: redis data, key is HS:TERM_ITEM:{term_id}:{item_id}
        :return: frame used in call self.send_frame
        """
        pass

    @abstractmethod
    def prepare_ctrl_frame(self, term_item_dict, value):
        """
        :param term_item_dict: redis data, key is HS:TERM_ITEM:{term_id}:{item_id}
        :param value: ctrl value
        :return: frame used in call self.send_frame
        """
        pass

    @abstractmethod
    def disconnect(self, reconnect=False):
        pass
