import importlib

from pydatacoll.plugins import BaseModule
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('DeviceManager')


class DeviceManager(BaseModule):
    device_dict = dict()
    protocol_dict = dict()

    async def start(self):
        try:
            with (await self.redis_pool) as redis_client:
                device_dict = await redis_client.smembers('SET:DEVICE')
                for device_id in device_dict:
                    device_dict = await redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                    if device_dict:
                        await self.add_device(None, device_dict)
        except Exception as ee:
            logger.error('init_devices failed: %s', repr(ee), exc_info=True)

    async def stop(self):
        await self.del_device(None)

    @param_function(channel='CHANNEL:DEVICE_FRESH')
    async def fresh_device(self, _, device_dict):
        try:
            device_id = str(device_dict['id'])
            device = self.device_dict.get(device_id)
            if device is not None:
                if str(device.device_info['id']) != str(device_dict['id']) or \
                                device.device_info['protocol'] != device_dict['protocol'] or \
                                device.device_info['ip'] != device_dict['ip'] or \
                                str(device.device_info['port']) != str(device_dict['port']):
                    await self.device_dict.pop(device_id).disconnect()
                else:
                    return
            protocol = device_dict['protocol']
            protocol_class = self.protocol_dict.get(protocol)
            if protocol_class is None:
                importlib.invalidate_caches()
                module = importlib.import_module('pydatacoll.protocols.{}.device'.format(protocol))
                protocol_class = self.protocol_dict[protocol] = getattr(module, '{}Device'.format(protocol.upper()))
                logger.info('fresh_device new protocol registered: %s ', protocol_class.__name__)
            self.device_dict[device_id] = protocol_class(device_dict, self.io_loop, self.redis_pool)
        except Exception as ee:
            logger.error('fresh_device failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:DEVICE_ADD')
    async def add_device(self, _, device_dict):
        await self.fresh_device(_, device_dict)

    @param_function(channel='CHANNEL:DEVICE_DEL')
    async def del_device(self, _, device_id=None):
        try:
            if device_id is None:
                for device in self.device_dict.values():
                    device.disconnect()
                self.device_dict.clear()
                return

            if device_id in self.device_dict:
                device = self.device_dict.pop(device_id)
                device.disconnect()
        except Exception as ee:
            logger.error('del_device failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:TERM_ADD')
    async def add_term(self, _, term_dict):
        device = self.device_dict.get(term_dict['device_id'])
        if device is not None:
            device.fresh_task(term_dict=term_dict, term_item_dict=None, delete=False)

    @param_function(channel='CHANNEL:TERM_DEL')
    async def del_term(self, _, term_dict):
        device = self.device_dict.get(term_dict['device_id'])
        if device is not None:
            device.fresh_task(term_dict=term_dict, term_item_dict=None, delete=True)

    @param_function(channel='CHANNEL:TERM_ITEM_ADD')
    async def add_term_item(self, _, term_item_dict):
        device = self.device_dict.get(term_item_dict['device_id'])
        if device is not None:
            device.fresh_task(term_dict=None, term_item_dict=term_item_dict, delete=False)

    @param_function(channel='CHANNEL:TERM_ITEM_DEL')
    async def del_term_item(self, _, term_item_dict):
        device = self.device_dict.get(term_item_dict['device_id'])
        if device is not None:
            device.fresh_task(term_dict=None, term_item_dict=term_item_dict, delete=True)

    @param_function(channel='CHANNEL:DEVICE_CALL')
    async def device_call(self, _, call_dict):
        try:
            device = self.device_dict.get(call_dict['device_id'])
            await device.call_data(call_dict)
        except Exception as ee:
            logger.error('device_call failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:DEVICE_CTRL')
    async def device_ctrl(self, _, ctrl_dict):
        try:
            device = self.device_dict.get(ctrl_dict['device_id'])
            await device.ctrl_data(ctrl_dict)
        except Exception as ee:
            logger.error('device_ctrl failed: %s', repr(ee), exc_info=True)


if __name__ == '__main__':
    import asyncio

    loop = asyncio.get_event_loop()
    device_manager = DeviceManager(loop)
    try:
        loop.create_task(device_manager.install())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error('run device_manager failed: %s', repr(e), exc_info=True)
    finally:
        loop.run_until_complete(device_manager.uninstall())
    loop.close()
