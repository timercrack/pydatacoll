import importlib
from plugins import BaseModule, param_function
from utils import logger as my_logger

logger = my_logger.getLogger('DeviceManager')


class DeviceManager(BaseModule):
    device_list = dict()
    protocol_list = dict()

    async def start(self):
        try:
            with (await self.redis_pool) as redis_client:
                device_list = await redis_client.smembers('SET:DEVICE')
                for device_id in device_list:
                    device_dict = await redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                    if device_dict:
                        await self.add_device(device_dict)
        except Exception as ee:
            logger.error('init_devices failed: %s', repr(ee), exc_info=True)

    async def stop(self):
        await self.del_device()

    @param_function(channel='CHANNEL:DEVICE_DEL')
    async def del_device(self, device_id=None):
        try:
            if device_id is None:
                for device in self.device_list.values():
                    device.disconnect()
                self.device_list.clear()
                return

            if device_id in self.device_list:
                device = self.device_list.pop(device_id)
                device.disconnect()
        except Exception as ee:
            logger.error('del_device failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:DEVICE_ADD')
    async def add_device(self, device_dict):
        await self.fresh_device(device_dict)

    @param_function(channel='CHANNEL:DEVICE_FRESH')
    async def fresh_device(self, device_dict):
        try:
            device_id = str(device_dict['id'])
            device = self.device_list.get(device_id)
            if device is not None:
                if str(device.info['id']) != str(device_dict['id']) or \
                                device.info['protocol'] != device_dict['protocol'] or \
                                device.info['ip'] != device_dict['ip'] or \
                                str(device.info['port']) != str(device_dict['port']):
                    await self.device_list.pop(device_id).disconnect()
                else:
                    return
            protocol = device_dict['protocol']
            protocol_class = self.protocol_list.get(protocol)
            if protocol_class is None:
                importlib.invalidate_caches()
                module = importlib.import_module('protocols.{}.device'.format(protocol.upper()))
                protocol_class = self.protocol_list[protocol] = getattr(module, '{}Device'.format(protocol.upper()))
                logger.info('fresh_device new protocol %s registered', protocol_class.__name__)
            self.device_list[device_id] = protocol_class(self.io_loop, self.redis_pool, device_dict)
        except Exception as ee:
            logger.error('fresh_device failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:TERM_ADD')
    async def add_term(self, term_dict):
        device = self.device_list.get(term_dict['device_id'])
        logger.debug('add_term device_id=%s, term_id=%s', term_dict['device_id'], term_dict['id'])
        if device is not None:
            device.fresh_task(term_id=term_dict['id'])

    @param_function(channel='CHANNEL:TERM_DEL')
    async def del_term(self, term_dict):
        device = self.device_list.get(term_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=term_dict['term_id'], item_id=None, delete=True)

    @param_function(channel='CHANNEL:TERM_ITEM_ADD')
    async def add_term_item(self, term_item_dict):
        device = self.device_list.get(term_item_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=term_item_dict['term_id'], item_id=term_item_dict['item_id'])

    @param_function(channel='CHANNEL:TERM_ITEM_DEL')
    async def del_term_item(self, term_item_dict):
        device = self.device_list.get(term_item_dict['device_id'])
        if device is not None:
            device.fresh_task(term_id=term_item_dict['term_id'], item_id=term_item_dict['item_id'], delete=True)

    @param_function(channel='CHANNEL:DEVICE_CALL')
    async def device_call(self, call_dict):
        try:
            device_id = call_dict['device_id']
            term_id = call_dict['term_id']
            item_id = call_dict['item_id']
            device = self.device_list.get(device_id)
            await device.call_data(term_id, item_id)
        except Exception as ee:
            logger.error('device_call failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:DEVICE_CTRL')
    async def device_ctrl(self, ctrl_dict):
        try:
            device_id = ctrl_dict['device_id']
            term_id = ctrl_dict['term_id']
            item_id = ctrl_dict['item_id']
            value = ctrl_dict['value']
            device = self.device_list.get(device_id)
            await device.ctrl_data(term_id, item_id, value)
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
