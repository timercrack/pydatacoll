import argparse
import pkgutil
from collections import defaultdict
import sys
from multiprocessing import Process
try:
    import ujson as json
except ImportError:
    import json
import asyncio
import functools
import aioredis
from aiohttp import web
import redis
import pydatacoll.utils.logger as my_logger
from pydatacoll.utils.json_response import JSON
from pydatacoll.resources.protocol import *
from pydatacoll.resources.redis_key import *
from pydatacoll.utils.func_container import ParamFunctionContainer, param_function
from pydatacoll import plugins
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('APIServer')
HANDLER_TIME_OUT = config.getint('SERVER', 'web_timeout', fallback=10)


class APIServer(ParamFunctionContainer):
    def __init__(self, port: int = None, production: bool = None, io_loop: asyncio.AbstractEventLoop = None,
                 redis_pool: aioredis.RedisPool = None):
        super().__init__()
        self.port = port or config.getint('SERVER', 'web_port', fallback=8080)
        self.io_loop = io_loop
        if self.io_loop is None:
            self.io_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.io_loop)
        self._redis_pool = redis_pool
        self.redis_pool = redis_pool or self.io_loop.run_until_complete(
                functools.partial(aioredis.create_pool, (config.get('REDIS', 'host', fallback='127.0.0.1'),
                                                         config.getint('REDIS', 'port', fallback=6379)),
                                  db=config.getint('REDIS', 'db', fallback=1),
                                  minsize=config.getint('REDIS', 'minsize', fallback=5),
                                  maxsize=config.getint('REDIS', 'maxsize', fallback=10),
                                  encoding=config.get('REDIS', 'encoding', fallback='utf-8'))())
        self.redis_client = redis.StrictRedis(db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.web_app = web.Application()
        self._add_router()
        self.web_handler = self.web_app.make_handler()
        self.web_server = self.io_loop.run_until_complete(
                self.io_loop.create_server(self.web_handler, '127.0.0.1', self.port))
        self.plugin_list = list()
        self.single_process = production or config.getboolean('SERVER', 'single_process', fallback=True)
        self._install_plugins()
        logger.info('ApiServer started, listening on port %s', self.port)

    def _add_router(self):
        for fun_name, fun_args in self.module_arg_dict.items():
            self.web_app.router.add_route(fun_args['method'], fun_args['url'], getattr(self, fun_name), name=fun_name)

    def _install_plugins(self):
        try:
            plugin_list = [item.strip() for item in config.get('SERVER', 'plugins').split(',')]
            for loader, module_name, is_pkg in pkgutil.iter_modules(plugins.__path__):
                if module_name in plugin_list and module_name not in sys.modules:  # prevent load twice
                    loader.find_module(module_name).load_module(module_name)
            for plugin_class in plugins.BaseModule.__subclasses__():
                if not hasattr(plugin_class, 'not_implemented'):
                    if self.single_process:
                        plugin = plugin_class(self.io_loop, self.redis_pool)
                        self.io_loop.run_until_complete(plugin.install())
                    else:
                        plugin = Process(target=plugin_class.run)
                        plugin.start()
                    self.plugin_list.append(plugin)
            logger.info("%s plugins founded: %s",
                        len(self.plugin_list), [type(plugin).__name__ for plugin in self.plugin_list])
        except Exception as e:
            logger.error("_install_plugins failed: %s", repr(e), exc_info=True)

    def _uninstall_plugins(self):
        for plugin in self.plugin_list:
            if self.single_process:
                self.io_loop.run_until_complete(plugin.uninstall())

    def stop_server(self):
        self._uninstall_plugins()
        if self._redis_pool is None:  # release the pool created by self
            self.io_loop.run_until_complete(self.redis_pool.clear())
        self.web_server.close()
        self.io_loop.run_until_complete(self.web_server.wait_closed())
        self.io_loop.run_until_complete(self.web_handler.finish_connections(1.0))
        self.io_loop.run_until_complete(self.web_app.finish())
        self.io_loop.run_until_complete(self.redis_pool.clear())
        logger.info('ApiServer stopped')

    @staticmethod
    async def _find_keys(redis_client, match: str):
        cursor = None
        all_keys = set()
        try:
            while cursor != 0:
                res = await redis_client.scan(cursor or b'0', match=match)
                cursor, keys = res
                all_keys.update(keys)
        except Exception as e:
            logger.error('_find_keys failed: %s', repr(e), exc_info=True)
        return all_keys

    @staticmethod
    async def _read_data(request):
        data = await request.read()
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return data

    @param_function(method='GET', url=r'/')
    async def get_index(self, request):
        doc_list = ['PyDataColl is running, available API:\n']
        method_dict = defaultdict(list)
        for route in self.web_app.router.routes():
            method_dict[route.method].append('method: {:<8} URL: {}://{}{}'.format(
                    route.method, request.scheme, request.host, route._formatter if hasattr(route, '_formatter') else
                    route._path))
        doc_list.append('\n'.join(sorted(method_dict['GET'])))
        doc_list.append('\n'.join(sorted(method_dict['POST'])))
        doc_list.append('\n'.join(sorted(method_dict['PUT'])))
        doc_list.append('\n'.join(sorted(method_dict['DELETE'])))
        return web.Response(text='\n'.join(doc_list))

    @param_function(method='GET', url=r'/api/v1/redis_key')
    async def get_redis_key(self, _):
        return JSON(REDIS_KEY)

    @param_function(method='GET', url=r'/api/v1/device_protocols')
    async def get_device_protocol_list(self, _):
        return JSON(DEVICE_PROTOCOLS)

    @param_function(method='GET', url=r'/api/v1/term_protocols')
    async def get_term_protocol_list(self, _):
        return JSON(TERM_PROTOCOLS)

    @param_function(method='GET', url=r'/api/v1/formulas')
    async def get_formula_list(self, _):
        try:
            with (await self.redis_pool) as redis_client:
                formula_list = await redis_client.smembers('SET:FORMULA')
                return JSON(formula_list)
        except Exception as e:
            logger.error('get_formula_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/formulas/{formula_id}')
    async def get_formula(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                formula = await redis_client.hgetall('HS:FORMULA:{}'.format(request.match_info['formula_id']))
                if not formula:
                    return web.Response(status=404, text='formula_id not found!')
                return JSON(formula)
        except Exception as e:
            logger.error('get_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices')
    async def get_device_list(self, _):
        try:
            with (await self.redis_pool) as redis_client:
                device_list = await redis_client.smembers('SET:DEVICE')
                return JSON(device_list)
        except Exception as e:
            logger.error('get_device_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}')
    async def get_device(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device = await redis_client.hgetall('HS:DEVICE:{}'.format(request.match_info['device_id']))
                if not device:
                    return web.Response(status=404, text='device_id not found!')
                return JSON(device)
        except Exception as e:
            logger.error('get_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms')
    async def get_term_list(self, _):
        try:
            with (await self.redis_pool) as redis_client:
                term_list = await redis_client.smembers('SET:TERM')
                return JSON(term_list)
        except Exception as e:
            logger.error('get_term_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms/{term_id}')
    async def get_term(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_id = request.match_info['term_id']
                term = await redis_client.hgetall('HS:TERM:{}'.format(term_id))
                if not term:
                    return web.Response(status=404, text='term_id not found!')
                return JSON(term)
        except Exception as e:
            logger.error('get_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/items')
    async def get_item_list(self, _):
        try:
            with (await self.redis_pool) as redis_client:
                item_list = await redis_client.smembers('SET:ITEM')
                return JSON(item_list)
        except Exception as e:
            logger.error('get_item_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/items/{item_id}')
    async def get_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                item_id = request.match_info['item_id']
                item = await redis_client.hgetall('HS:ITEM:{}'.format(item_id))
                if not item:
                    return web.Response(status=404, text='item_id not found!')
                return JSON(item)
        except Exception as e:
            logger.error('get_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}/terms')
    async def get_device_term_list(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device_id = request.match_info['device_id']
                found = await redis_client.exists('SET:DEVICE_TERM:{}'.format(device_id))
                if not found:
                    return web.Response(status=404, text='device_id not found!')
                term_list = await redis_client.smembers('SET:DEVICE_TERM:{}'.format(device_id))
                return JSON(term_list)
        except Exception as e:
            logger.error('get_device_term_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms/{term_id}/items')
    async def get_term_item_list(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_id = request.match_info['term_id']
                found = await redis_client.exists('SET:TERM_ITEM:{}'.format(term_id))
                if not found:
                    return web.Response(status=404, text='term_id not found!')
                item_list = await redis_client.smembers('SET:TERM_ITEM:{}'.format(term_id))
                return JSON(item_list)
        except Exception as e:
            logger.error('get_term_item_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms/{term_id}/items/{item_id}')
    async def get_term_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_id = request.match_info['term_id']
                item_id = request.match_info['item_id']
                found = await redis_client.exists('HS:TERM:{}'.format(term_id))
                if not found:
                    return web.Response(status=404, text='term_id not found!')
                found = await redis_client.exists('HS:ITEM:{}'.format(item_id))
                if not found:
                    return web.Response(status=404, text='item_id not found!')
                term_item = await redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                if not term_item:
                    return web.Response(status=404, text='term_item not found!')
                return JSON(term_item)
        except Exception as e:
            logger.error('get_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}/terms/{term_id}/items/{item_id}/datas')
    async def get_data_list(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device_id = request.match_info['device_id']
                term_id = request.match_info['term_id']
                item_id = request.match_info['item_id']
                data_list = await redis_client.hgetall('HS:DATA:{}:{}:{}'.format(device_id, term_id, item_id))
                return JSON(data_list)
        except Exception as e:
            logger.error('get_data_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}/terms/{term_id}/items/{item_id}/datas/{index}')
    async def get_data(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device_id = request.match_info['device_id']
                term_id = request.match_info['term_id']
                item_id = request.match_info['item_id']
                index = int(request.match_info['index'])
                idx_key = await redis_client.lindex('LST:DATA_TIME:{}:{}:{}'.format(device_id, term_id, item_id), index)
                data_val = await redis_client.hget('HS:DATA:{}:{}:{}'.format(device_id, term_id, item_id), idx_key)
                return JSON({idx_key: data_val})
        except Exception as e:
            logger.error('get_data failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/formulas')
    async def create_formula(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                formula_data = await self._read_data(request)
                formula_dict = json.loads(formula_data)
                logger.debug('new formula arg=%s', formula_dict)
                found = await redis_client.exists('HS:FORMULA:{}'.format(formula_dict['id']))
                if found:
                    return web.Response(status=409, text='formula already exists!')
                self.redis_client.hmset('HS:FORMULA:{}'.format(formula_dict['id']), formula_dict)
                await redis_client.sadd('SET:FORMULA', formula_dict['id'])
                for param, param_value in formula_dict.items():
                    if param.startswith('p'):
                        await redis_client.sadd('SET:FORMULA_PARAM:{}'.format(param_value), formula_dict['id'])
                await redis_client.publish('CHANNEL:FORMULA_ADD', formula_data)
                return web.Response()
        except Exception as e:
            logger.error('create_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/formulas/{formula_id}')
    async def update_formula(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                formula_id = request.match_info['formula_id']
                old_formula = await redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
                if not old_formula:
                    return web.Response(status=404, text='formula_id not found!')
                await self.del_formula(request)
                await self.create_formula(request)
                return web.Response()
        except Exception as e:
            logger.error('update_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/formulas/{formula_id}')
    async def del_formula(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                formula_id = request.match_info['formula_id']
                formula_dict = await redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
                if not formula_dict:
                    return web.Response(status=404, text='formula_id not found!')
                await redis_client.publish('CHANNEL:FORMULA_DEL', json.dumps(formula_id))
                for param, param_value in formula_dict.items():
                    if param.startswith('p'):
                        await redis_client.srem('SET:FORMULA_PARAM:{}'.format(param_value), formula_id)
                await redis_client.delete('HS:FORMULA:{}'.format(formula_id))
                await redis_client.srem('SET:FORMULA', formula_id)
                return web.Response()
        except Exception as e:
            logger.error('del_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/devices')
    async def create_device(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device_data = await self._read_data(request)
                device_dict = json.loads(device_data)
                logger.debug('new device arg=%s', device_dict)
                found = await redis_client.exists('HS:DEVICE:{}'.format(device_dict['id']))
                if found:
                    return web.Response(status=409, text='device already exists!')
                self.redis_client.hmset('HS:DEVICE:{}'.format(device_dict['id']), device_dict)
                await redis_client.sadd('SET:DEVICE', device_dict['id'])
                await redis_client.publish('CHANNEL:DEVICE_ADD', device_data)
                return web.Response()
        except Exception as e:
            logger.error('create_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/devices/{device_id}')
    async def update_device(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device_id = request.match_info['device_id']
                old_device = await redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                if not old_device:
                    return web.Response(status=404, text='device_id not found!')
                device_data = await self._read_data(request)
                device_dict = json.loads(device_data)
                if str(device_dict['id']) != device_id:
                    await self.del_device(request)
                    await self.create_device(request)
                else:
                    self.redis_client.hmset('HS:DEVICE:{}'.format(device_id), device_dict)
                    await redis_client.publish('CHANNEL:DEVICE_FRESH', device_data)
                return web.Response()
        except Exception as e:
            logger.error('update_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/devices/{device_id}')
    async def del_device(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                device_id = request.match_info['device_id']
                device_dict = await redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                if not device_dict:
                    return web.Response(status=404, text='device_id not found!')
                await redis_client.publish('CHANNEL:DEVICE_DEL', json.dumps(device_id))
                await redis_client.delete('HS:DEVICE:{}'.format(device_id))
                await redis_client.srem('SET:DEVICE', device_id)
                # delete all terms connected to that device
                term_list = await redis_client.smembers('SET:DEVICE_TERM:{}'.format(device_id))
                for term_id in term_list:
                    await redis_client.delete('HS:TERM:{}'.format(term_id))
                    await redis_client.srem('SET:TERM', term_id)
                    keys = await self._find_keys(redis_client, 'HS:TERM_ITEM:{}:*'.format(term_id))
                    if keys:
                        self.redis_client.delete(*keys)
                    await redis_client.delete('SET:TERM_ITEM:{}'.format(term_id))
                await redis_client.delete('SET:DEVICE_TERM:{}'.format(device_id))
                await redis_client.delete('LST:FRAME:{}'.format(device_id))
                # delete values
                keys = await self._find_keys(redis_client, 'LST:DATA_TIME:{}:*'.format(device_id))
                if keys:
                    self.redis_client.delete(*keys)
                keys = await self._find_keys(redis_client, 'HS:DATA:{}:*'.format(device_id))
                if keys:
                    self.redis_client.delete(*keys)
                # delete mapping
                keys = await self._find_keys(redis_client, 'HS:MAPPING:*:{}:*'.format(device_id))
                if keys:
                    self.redis_client.delete(*keys)
                return web.Response()
        except Exception as e:
            logger.error('del_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/terms')
    async def create_term(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_data = await self._read_data(request)
                term_dict = json.loads(term_data)
                logger.debug('new term arg=%s', term_dict)
                found = await redis_client.exists('HS:TERM:{}'.format(term_dict['id']))
                if found:
                    return web.Response(status=409, text='term already exists!')
                self.redis_client.hmset('HS:TERM:{}'.format(term_dict['id']), term_dict)
                await redis_client.sadd('SET:TERM', term_dict['id'])
                await redis_client.sadd('SET:DEVICE_TERM:{}'.format(term_dict['device_id']), term_dict['id'])
                await redis_client.publish('CHANNEL:TERM_ADD"', term_data)
                return web.Response()
        except Exception as e:
            logger.error('create_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/terms/{term_id}')
    async def update_term(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_id = request.match_info['term_id']
                old_term = await redis_client.hgetall('HS:TERM:{}'.format(term_id))
                if not old_term:
                    return web.Response(status=404, text='term_id not found!')
                term_data = await self._read_data(request)
                term_dict = json.loads(term_data)
                if str(term_dict['id']) != term_id:
                    await self.del_term(request)
                    await self.create_term(request)
                else:
                    self.redis_client.hmset('HS:TERM:{}'.format(term_id), term_dict)
                    if term_dict['device_id'] != old_term['device_id']:
                        await redis_client.publish('CHANNEL:TERM_DEL', json.dumps(old_term))
                        await redis_client.publish('CHANNEL:TERM_ADD', term_data)
                    else:
                        await redis_client.publish('CHANNEL:TERM_FRESH', term_data)
                return web.Response()
        except Exception as e:
            logger.error('update_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/terms/{term_id}')
    async def del_term(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_id = request.match_info['term_id']
                term_info = await redis_client.hgetall('HS:TERM:{}'.format(term_id))
                if not term_info:
                    return web.Response(status=404, text='term_id not found!')
                device_id = term_info['device_id']
                await redis_client.publish('CHANNEL:TERM_DEL', json.dumps({'device_id': device_id, 'term_id': term_id}))
                await redis_client.delete('HS:TERM:{}'.format(term_id))
                await redis_client.srem('SET:TERM', term_id)
                await redis_client.srem('SET:DEVICE_TERM:{}'.format(term_info['device_id']), term_id)
                await redis_client.delete('SET:TERM_ITEM:{}'.format(term_id))
                # delete all values
                keys = await self._find_keys(redis_client, 'LST:DATA_TIME:*:{}:*'.format(term_id))
                if keys:
                    self.redis_client.delete(*keys)
                keys = await self._find_keys(redis_client, 'HS:DATA:*:{}:*'.format(term_id))
                if keys:
                    self.redis_client.delete(*keys)
                # delete from protocols mapping
                all_keys = set()
                keys = await self._find_keys(redis_client, 'HS:MAPPING:*')
                for key in keys:
                    map_key = await redis_client.hgetall(key)
                    if str(map_key['term_id']) == term_id:
                        all_keys.add(key)
                if all_keys:
                    self.redis_client.delete(*all_keys)
                return web.Response()
        except Exception as e:
            logger.error('del_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/items')
    async def create_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                item_data = await self._read_data(request)
                item_dict = json.loads(item_data)
                logger.debug('new item arg=%s', item_dict)
                found = await redis_client.exists('HS:ITEM:{}'.format(item_dict['id']))
                if found:
                    return web.Response(status=409, text='item already exists!')
                self.redis_client.hmset('HS:ITEM:{}'.format(item_dict['id']), item_dict)
                await redis_client.sadd('SET:ITEM', item_dict['id'])
                return web.Response()
        except Exception as e:
            logger.error('create_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/items/{item_id}')
    async def update_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                item_id = request.match_info['item_id']
                old_item = await redis_client.hgetall('HS:ITEM:{}'.format(item_id))
                if not old_item:
                    return web.Response(status=404, text='item_id not found!')
                item_data = await self._read_data(request)
                item_dict = json.loads(item_data)
                if str(item_dict['id']) != item_id:
                    await self.del_item(request)
                    await self.create_item(request)
                else:
                    self.redis_client.hmset('HS:ITEM:{}'.format(item_id), item_dict)
                return web.Response()
        except Exception as e:
            logger.error('update_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/items/{item_id}')
    async def del_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                item_id = request.match_info['item_id']
                found = await redis_client.exists('HS:ITEM:{}'.format(item_id))
                if not found:
                    return web.Response(status=404, text='item_id not found!')
                await redis_client.delete('HS:ITEM:{}'.format(item_id))
                await redis_client.srem('SET:ITEM', item_id)
                # delete from term->item set
                keys = await self._find_keys(redis_client, 'SET:TERM_ITEM:*')
                for key in keys:
                    await redis_client.srem(key, item_id)
                # delete from term->item hash, TODO: publish msg to CHANNEL:TERM_ITEM_DEL
                keys = await self._find_keys(redis_client, 'HS:TERM_ITEM:*:{}'.format(item_id))
                if keys:
                    self.redis_client.delete(*keys)
                # delete from protocols mapping
                all_keys = set()
                keys = await self._find_keys(redis_client, 'HS:MAPPING:*')
                for key in keys:
                    map_key = await redis_client.hgetall(key)
                    if map_key and str(map_key['item_id']) == item_id:
                        all_keys.add(key)
                if all_keys:
                    await redis_client.delete(*all_keys)
                # delete all values
                keys = await self._find_keys(redis_client, 'LST:DATA_TIME:*:*:{}'.format(item_id))
                if keys:
                    self.redis_client.delete(*keys)
                keys = await self._find_keys(redis_client, 'HS:DATA:*:*:{}'.format(item_id))
                if keys:
                    self.redis_client.delete(*keys)
                return web.Response()
        except Exception as e:
            logger.error('del_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/terms/{term_id}/items')
    async def create_term_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_item_data = await self._read_data(request)
                term_item_dict = json.loads(term_item_data)
                logger.debug('new term_item arg=%s', term_item_dict)
                term_id = request.match_info['term_id']
                if term_id != str(term_item_dict['term_id']):
                    return web.Response(status=400, text='term_id mismatch in url and body!')
                item_id = term_item_dict['item_id']
                found = await redis_client.exists('HS:TERM:{}'.format(term_id))
                if not found:
                    return web.Response(status=404, text='term_id not found!')
                found = await redis_client.exists('HS:ITEM:{}'.format(item_id))
                if not found:
                    return web.Response(status=404, text='item_id not found!')
                found = await redis_client.exists('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                if found:
                    return web.Response(status=409, text='term_item already exists!')
                term_info = await redis_client.hgetall('HS:TERM:{}'.format(term_id))
                device_id = term_info['device_id']
                term_item_dict.update({'device_id': device_id})
                device_info = await redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                self.redis_client.hmset('HS:TERM_ITEM:{}:{}'.format(term_id, item_id), term_item_dict)
                await redis_client.sadd('SET:TERM_ITEM:{}'.format(term_id), item_id)
                # delete old mapping
                all_keys = set()
                keys = await self._find_keys(redis_client, 'HS:MAPPING:{}:*:*'.format(device_info['protocol'].upper()))
                for key in keys:
                    map_key = await redis_client.hgetall(key)
                    if str(map_key['term_id']) == term_id and str(map_key['item_id']) == item_id:
                        all_keys.add(key)
                if all_keys:
                    self.redis_client.delete(*all_keys)
                self.redis_client.hmset('HS:MAPPING:{}:{}:{}'.format(device_info['protocol'].upper(), device_id,
                                                                     term_item_dict['protocol_code']), term_item_dict)
                await redis_client.publish('CHANNEL:TERM_ITEM_ADD', json.dumps(term_item_dict))
                return web.Response()
        except Exception as e:
            logger.error('create_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/terms/{term_id}/items/{item_id}')
    async def update_term_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_item_data = await self._read_data(request)
                term_item_dict = json.loads(term_item_data)
                term_id = request.match_info['term_id']
                item_id = request.match_info['item_id']
                old_term_item = await redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                if not old_term_item:
                    return web.Response(status=404, text='term_item not found!')
                if str(term_item_dict['term_id']) == term_id and str(term_item_dict['item_id']) == item_id:
                    await self.del_term_item(request)
                    await self.create_term_item(request)
                return web.Response()
        except Exception as e:
            logger.error('update_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/terms/{term_id}/items/{item_id}')
    async def del_term_item(self, request):
        try:
            with (await self.redis_pool) as redis_client:
                term_id = request.match_info['term_id']
                item_id = request.match_info['item_id']
                term_item_dict = await redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                if not term_item_dict:
                    return web.Response(status=404, text='term_item not found!')
                term_info = await redis_client.hgetall('HS:TERM:{}'.format(term_id))
                device_id = term_info['device_id']
                device_info = await redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                await redis_client.publish('CHANNEL:TERM_ITEM_DEL',
                                           json.dumps({'device_id': device_id, 'term_id': term_id, 'item_id': item_id}))
                await redis_client.delete('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                await redis_client.srem('SET:TERM_ITEM:{}'.format(term_id), item_id)
                await redis_client.delete('HS:MAPPING:{}:{}:{}'.format(
                        device_info['protocol'].upper(), device_id, term_item_dict['protocol_code']))
                # delete all values
                keys = await self._find_keys(redis_client, 'LST:DATA_TIME:*:{}:{}'.format(term_id, item_id))
                if keys:
                    self.redis_client.delete(*keys)
                keys = await self._find_keys(redis_client, 'HS:DATA:*:{}:{}'.format(term_id, item_id))
                if keys:
                    self.redis_client.delete(*keys)
                return web.Response()
        except Exception as e:
            logger.error('del_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/device_call')
    async def device_call(self, request):
        redis_client = None
        channel_name = None
        try:
            with (await self.redis_pool) as redis_client:
                call_data = await self._read_data(request)
                call_data_dict = json.loads(call_data)
                logger.debug('new call_data arg=%s', call_data_dict)
                found = await redis_client.exists('HS:DEVICE:{}'.format(call_data_dict['device_id']))
                if not found:
                    return web.Response(status=404, text='device_id not found!')
                found = await redis_client.exists('HS:TERM:{}'.format(call_data_dict['term_id']))
                if not found:
                    return web.Response(status=404, text='term_id not found!')
                found = await redis_client.exists('HS:ITEM:{}'.format(call_data_dict['item_id']))
                if not found:
                    return web.Response(status=404, text='item_id not found!')
                found = await redis_client.exists('HS:TERM_ITEM:{}:{}'.format(
                        call_data_dict['term_id'], call_data_dict['item_id']))
                if not found:
                    return web.Response(status=404, text='term_item not found!')
                await redis_client.publish('CHANNEL:DEVICE_CALL', call_data)
                channel_name = 'CHANNEL:DEVICE_CALL:{}:{}:{}'.format(
                        call_data_dict['device_id'], call_data_dict['term_id'], call_data_dict['item_id'])
                res = await redis_client.subscribe(channel_name)
                cb = asyncio.futures.Future(loop=self.io_loop)

                async def reader(ch):
                    while await ch.wait_message():
                        msg = await ch.get_json()
                        logger.debug('device_call got msg: %s', msg)
                        if not cb.done():
                            cb.set_result(msg)

                tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
                rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
                await redis_client.unsubscribe(channel_name)
                await tsk
                return JSON(rst)
        except Exception as e:
            logger.exception(e)
            logger.error('device_call failed: %s', repr(e), exc_info=True)
            if redis_client and redis_client.in_pubsub and channel_name:
                await redis_client.unsubscribe(channel_name)
                self.redis_pool.release(redis_client)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/device_ctrl')
    async def device_ctrl(self, request):
        redis_client = None
        channel_name = None
        try:
            with (await self.redis_pool) as redis_client:
                ctrl_data = await self._read_data(request)
                ctrl_data_dict = json.loads(ctrl_data)
                logger.debug('new ctrl_data arg=%s', ctrl_data_dict)
                found = await redis_client.exists('HS:DEVICE:{}'.format(ctrl_data_dict['device_id']))
                if not found:
                    return web.Response(status=404, text='device_id not found!')
                found = await redis_client.exists('HS:TERM:{}'.format(ctrl_data_dict['term_id']))
                if not found:
                    return web.Response(status=404, text='term_id not found!')
                found = await redis_client.exists('HS:ITEM:{}'.format(ctrl_data_dict['item_id']))
                if not found:
                    return web.Response(status=404, text='item_id not found!')
                found = await redis_client.exists(
                        'HS:TERM_ITEM:{}:{}'.format(ctrl_data_dict['term_id'], ctrl_data_dict['item_id']))
                if not found:
                    return web.Response(status=404, text='term_item not found!')
                await redis_client.publish('CHANNEL:DEVICE_CTRL', ctrl_data)
                channel_name = 'CHANNEL:DEVICE_CTRL:{}:{}:{}'.format(
                        ctrl_data_dict['device_id'], ctrl_data_dict['term_id'], ctrl_data_dict['item_id'])
                res = await redis_client.subscribe(channel_name)
                cb = asyncio.futures.Future(loop=self.io_loop)

                async def reader(ch):
                    while await ch.wait_message():
                        msg = await ch.get_json()
                        logger.debug('device_ctrl got msg: %s', msg)
                        if not cb.done():
                            cb.set_result(msg)

                tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
                rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
                await redis_client.unsubscribe(channel_name)
                await tsk
                return JSON(rst)
        except Exception as e:
            logger.exception(e)
            logger.error('device_ctrl failed: %s', repr(e), exc_info=True)
            if redis_client and redis_client.in_pubsub and channel_name:
                await redis_client.unsubscribe(channel_name)
                self.redis_pool.release(redis_client)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/formula_check')
    async def formula_check(self, request):
        redis_client = None
        channel_name = None
        try:
            with (await self.redis_pool) as redis_client:
                formula_data = await self._read_data(request)
                formula_dict = json.loads(formula_data)
                logger.debug('formula_check arg=%s', formula_dict)
                channel_name = 'CHANNEL:FORMULA_CHECK_RESULT:{}'.format(len(formula_dict['formula']))
                res = await redis_client.subscribe(channel_name)
                cb = asyncio.futures.Future(loop=self.io_loop)

                async def reader(ch):
                    while await ch.wait_message():
                        msg = await ch.get(encoding='utf-8')
                        if not cb.done():
                            cb.set_result(msg)

                tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
                with (await self.redis_pool) as pub_client:
                    await pub_client.publish('CHANNEL:FORMULA_CHECK', formula_data)
                rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
                await redis_client.unsubscribe(channel_name)
                await tsk
                return web.Response(status=200, text=rst)
        except Exception as e:
            logger.exception(e)
            logger.error('formula_check failed: %s', repr(e), exc_info=True)
            if redis_client and redis_client.in_pubsub and channel_name:
                await redis_client.unsubscribe(channel_name)
                self.redis_pool.release(redis_client)
            return web.Response(status=400, text=repr(e))


if __name__ == '__main__':
    api_server = None
    try:
        parser = argparse.ArgumentParser(description='PyDataColl RESTful Server')
        parser.add_argument('--port', type=int, default=8080, help='http listening port, default: 8080')
        parser.add_argument('--production', action='store_true', help='run in production environment')
        args = parser.parse_args()
        api_server = APIServer(port=args.port, production=args.production)
        logger.info('serving on %s', api_server.web_server.sockets[0].getsockname())
        print('server is running.')
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        pass
    except Exception as ee:
        logger.info('got error: %s', repr(ee), exc_info=True)
    finally:
        api_server and api_server.stop_server()
    print('server is stopped.')
