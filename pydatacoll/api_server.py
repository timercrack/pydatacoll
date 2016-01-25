#!/usr/bin/env python
#
# Copyright 2016 timercrack
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
from collections import defaultdict
import importlib
from multiprocessing import Process
try:
    import ujson as json
except ImportError:
    import json
import asyncio
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
    def __init__(self, port: int = None, production: bool = None, io_loop: asyncio.AbstractEventLoop = None):
        super().__init__()
        self.port = port or config.getint('SERVER', 'web_port', fallback=8080)
        self.io_loop = io_loop
        if self.io_loop is None:
            self.io_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.io_loop)
        self.redis_client = redis.StrictRedis(db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.web_app = web.Application()
        self._add_router()
        self.web_handler = self.web_app.make_handler()
        self.web_server = self.io_loop.run_until_complete(
                self.io_loop.create_server(self.web_handler, '127.0.0.1', self.port))
        self.plugin_dict = dict()
        self.single_process = production or config.getboolean('SERVER', 'single_process', fallback=True)
        self._install_plugins()
        logger.info('ApiServer started, listening on port %s', self.port)

    def _add_router(self):
        for fun_name, fun_args in self.module_arg_dict.items():
            self.web_app.router.add_route(fun_args['method'], fun_args['url'], getattr(self, fun_name), name=fun_name)

    def _install_plugins(self):
        try:
            for item in config.get('SERVER', 'plugins').split(','):
                module_name = item.strip()
                if module_name in plugins.available_plugins:
                    plugin_module = importlib.import_module('pydatacoll.plugins.{}'.format(module_name))
                    class_name = plugins.available_plugins.get(module_name)
                    plugin_class = getattr(plugin_module, class_name)
                    if hasattr(plugin_class, 'not_implemented') or class_name in self.plugin_dict:
                        continue
                    if self.single_process:
                        plugin = plugin_class(self.io_loop)
                        self.io_loop.run_until_complete(plugin.install())
                    else:
                        plugin = Process(target=plugin_class.run)
                        plugin.start()
                    self.plugin_dict[class_name] = plugin
                else:
                    logger.error("can't found plugin: %s", module_name)
            logger.info("%s plugins founded: %s", len(self.plugin_dict), self.plugin_dict.keys())
        except Exception as e:
            logger.error("_install_plugins failed: %s", repr(e), exc_info=True)

    def _uninstall_plugins(self):
        for plugin in self.plugin_dict.values():
            if self.single_process:
                self.io_loop.run_until_complete(plugin.uninstall())

    def stop_server(self):
        self._uninstall_plugins()
        self.web_server.close()
        self.io_loop.run_until_complete(self.web_server.wait_closed())
        self.io_loop.run_until_complete(self.web_handler.finish_connections(1.0))
        self.io_loop.run_until_complete(self.web_app.finish())
        logger.info('ApiServer stopped')

    def found_and_delete(self, match: str):
        keys = list(self.redis_client.scan_iter(match))
        if keys:
            self.redis_client.delete(*keys)

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
            formula_list = self.redis_client.smembers('SET:FORMULA')
            return JSON(formula_list)
        except Exception as e:
            logger.error('get_formula_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/formulas/{formula_id}')
    async def get_formula(self, request):
        try:
            formula = self.redis_client.hgetall('HS:FORMULA:{}'.format(request.match_info['formula_id']))
            if not formula:
                return web.Response(status=404, text='formula_id not found!')
            return JSON(formula)
        except Exception as e:
            logger.error('get_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices')
    async def get_device_list(self, _):
        try:
            device_list = self.redis_client.smembers('SET:DEVICE')
            return JSON(device_list)
        except Exception as e:
            logger.error('get_device_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}')
    async def get_device(self, request):
        try:
            device = self.redis_client.hgetall('HS:DEVICE:{}'.format(request.match_info['device_id']))
            if not device:
                return web.Response(status=404, text='device_id not found!')
            return JSON(device)
        except Exception as e:
            logger.error('get_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms')
    async def get_term_list(self, _):
        try:
            term_list = self.redis_client.smembers('SET:TERM')
            return JSON(term_list)
        except Exception as e:
            logger.error('get_term_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms/{term_id}')
    async def get_term(self, request):
        try:
            term_id = request.match_info['term_id']
            term = self.redis_client.hgetall('HS:TERM:{}'.format(term_id))
            if not term:
                return web.Response(status=404, text='term_id not found!')
            return JSON(term)
        except Exception as e:
            logger.error('get_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/items')
    async def get_item_list(self, _):
        try:
            item_list = self.redis_client.smembers('SET:ITEM')
            return JSON(item_list)
        except Exception as e:
            logger.error('get_item_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/items/{item_id}')
    async def get_item(self, request):
        try:
            item_id = request.match_info['item_id']
            item = self.redis_client.hgetall('HS:ITEM:{}'.format(item_id))
            if not item:
                return web.Response(status=404, text='item_id not found!')
            return JSON(item)
        except Exception as e:
            logger.error('get_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}/terms')
    async def get_device_term_list(self, request):
        try:
            device_id = request.match_info['device_id']
            found = self.redis_client.exists('SET:DEVICE_TERM:{}'.format(device_id))
            if not found:
                return web.Response(status=404, text='device_id not found!')
            term_list = self.redis_client.smembers('SET:DEVICE_TERM:{}'.format(device_id))
            return JSON(term_list)
        except Exception as e:
            logger.error('get_device_term_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms/{term_id}/items')
    async def get_term_item_list(self, request):
        try:
            term_id = request.match_info['term_id']
            found = self.redis_client.exists('SET:TERM_ITEM:{}'.format(term_id))
            if not found:
                return web.Response(status=404, text='term_id not found!')
            item_list = self.redis_client.smembers('SET:TERM_ITEM:{}'.format(term_id))
            return JSON(item_list)
        except Exception as e:
            logger.error('get_term_item_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/terms/{term_id}/items/{item_id}')
    async def get_term_item(self, request):
        try:
            term_id = request.match_info['term_id']
            item_id = request.match_info['item_id']
            found = self.redis_client.exists('HS:TERM:{}'.format(term_id))
            if not found:
                return web.Response(status=404, text='term_id not found!')
            found = self.redis_client.exists('HS:ITEM:{}'.format(item_id))
            if not found:
                return web.Response(status=404, text='item_id not found!')
            term_item = self.redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
            if not term_item:
                return web.Response(status=404, text='term_item not found!')
            return JSON(term_item)
        except Exception as e:
            logger.error('get_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}/terms/{term_id}/items/{item_id}/datas')
    async def get_data_list(self, request):
        try:
            device_id = request.match_info['device_id']
            term_id = request.match_info['term_id']
            item_id = request.match_info['item_id']
            data_list = self.redis_client.hgetall('HS:DATA:{}:{}:{}'.format(device_id, term_id, item_id))
            return JSON(data_list)
        except Exception as e:
            logger.error('get_data_list failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='GET', url=r'/api/v1/devices/{device_id}/terms/{term_id}/items/{item_id}/datas/{index}')
    async def get_data(self, request):
        try:
            device_id = request.match_info['device_id']
            term_id = request.match_info['term_id']
            item_id = request.match_info['item_id']
            index = int(request.match_info['index'])
            idx_key = self.redis_client.lindex('LST:DATA_TIME:{}:{}:{}'.format(device_id, term_id, item_id), index)
            data_val = self.redis_client.hget('HS:DATA:{}:{}:{}'.format(device_id, term_id, item_id), idx_key)
            return JSON({idx_key: data_val})
        except Exception as e:
            logger.error('get_data failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/formulas')
    async def create_formula(self, request):
        try:
            formula_data = await self._read_data(request)
            formula_dict = json.loads(formula_data)
            logger.debug('new formula arg=%s', formula_dict)
            found = self.redis_client.exists('HS:FORMULA:{}'.format(formula_dict['id']))
            if found:
                return web.Response(status=409, text='formula already exists!')
            self.redis_client.hmset('HS:FORMULA:{}'.format(formula_dict['id']), formula_dict)
            self.redis_client.sadd('SET:FORMULA', formula_dict['id'])
            for param, param_value in formula_dict.items():
                if param.startswith('p'):
                    self.redis_client.sadd('SET:FORMULA_PARAM:{}'.format(param_value), formula_dict['id'])
            self.redis_client.publish('CHANNEL:FORMULA_ADD', json.dumps(formula_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/formulas')
    async def create_formula_batch(self, request):
        try:
            formula_data = await self._read_data(request)
            formula_list = json.loads(formula_data)
            if type(formula_list) != list:
                formula_list = [formula_list]
            for formula_dict in formula_list:
                logger.debug('new formula arg=%s', formula_dict)
                self.redis_client.hmset('HS:FORMULA:{}'.format(formula_dict['id']), formula_dict)
                self.redis_client.sadd('SET:FORMULA', formula_dict['id'])
                for param, param_value in formula_dict.items():
                    if param.startswith('p'):
                        self.redis_client.sadd('SET:FORMULA_PARAM:{}'.format(param_value), formula_dict['id'])
                self.redis_client.publish('CHANNEL:FORMULA_ADD', json.dumps(formula_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_formula_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/formulas/{formula_id}')
    async def update_formula(self, request):
        try:
            formula_id = request.match_info['formula_id']
            old_formula = self.redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
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
            formula_id = request.match_info['formula_id']
            formula_dict = self.redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
            if not formula_dict:
                return web.Response(status=404, text='formula_id not found!')
            self.redis_client.publish('CHANNEL:FORMULA_DEL', json.dumps(formula_id))
            for param, param_value in formula_dict.items():
                if param.startswith('p'):
                    self.redis_client.srem('SET:FORMULA_PARAM:{}'.format(param_value), formula_id)
            self.redis_client.delete('HS:FORMULA:{}'.format(formula_id))
            self.redis_client.srem('SET:FORMULA', formula_id)
            return web.Response()
        except Exception as e:
            logger.error('del_formula failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/formulas/del')
    async def del_formula_batch(self, request):
        try:
            formula_data = await self._read_data(request)
            formula_list = json.loads(formula_data)
            if type(formula_list) != list:
                formula_list = [formula_list]
            for formula_id in formula_list:
                formula_dict = self.redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
                if not formula_dict:
                    return web.Response(status=404, text='formula_id not found!')
                self.redis_client.publish('CHANNEL:FORMULA_DEL', json.dumps(formula_id))
                for param, param_value in formula_dict.items():
                    if param.startswith('p'):
                        self.redis_client.srem('SET:FORMULA_PARAM:{}'.format(param_value), formula_id)
                self.redis_client.delete('HS:FORMULA:{}'.format(formula_id))
                self.redis_client.srem('SET:FORMULA', formula_id)
            return web.Response()
        except Exception as e:
            logger.error('del_formula_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/devices')
    async def create_device(self, request):
        try:
            device_data = await self._read_data(request)
            device_dict = json.loads(device_data)
            logger.debug('new device arg=%s', device_dict)
            found = self.redis_client.exists('HS:DEVICE:{}'.format(device_dict['id']))
            if found:
                return web.Response(status=409, text='device already exists!')
            self.redis_client.hmset('HS:DEVICE:{}'.format(device_dict['id']), device_dict)
            self.redis_client.sadd('SET:DEVICE', device_dict['id'])
            self.redis_client.publish('CHANNEL:DEVICE_ADD', json.dumps(device_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/devices')
    async def create_device_batch(self, request):
        try:
            device_data = await self._read_data(request)
            device_list = json.loads(device_data)
            if type(device_list) != list:
                device_list = [device_list]
            for device_dict in device_list:
                logger.debug('new device arg=%s', device_dict)
                self.redis_client.hmset('HS:DEVICE:{}'.format(device_dict['id']), device_dict)
                self.redis_client.sadd('SET:DEVICE', device_dict['id'])
                self.redis_client.publish('CHANNEL:DEVICE_ADD', json.dumps(device_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/devices/{device_id}')
    async def update_device(self, request):
        try:
            device_id = request.match_info['device_id']
            old_device = self.redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
            if not old_device:
                return web.Response(status=404, text='device_id not found!')
            device_data = await self._read_data(request)
            device_dict = json.loads(device_data)
            if str(device_dict['id']) != device_id:
                await self.del_device(request)
                await self.create_device(request)
            else:
                self.redis_client.hmset('HS:DEVICE:{}'.format(device_id), device_dict)
                self.redis_client.publish('CHANNEL:DEVICE_FRESH', device_data)
            return web.Response()
        except Exception as e:
            logger.error('update_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/devices/{device_id}')
    async def del_device(self, request):
        try:
            device_id = request.match_info['device_id']
            device_dict = self.redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
            if not device_dict:
                return web.Response(status=404, text='device_id not found!')
            self.redis_client.publish('CHANNEL:DEVICE_DEL', json.dumps(device_id))
            self.redis_client.delete('HS:DEVICE:{}'.format(device_id))
            self.redis_client.srem('SET:DEVICE', device_id)
            # delete all terms connected to that device
            term_list = self.redis_client.smembers('SET:DEVICE_TERM:{}'.format(device_id))
            for term_id in term_list:
                self.redis_client.delete('HS:TERM:{}'.format(term_id))
                self.redis_client.srem('SET:TERM', term_id)
                self.found_and_delete('HS:TERM_ITEM:{}:*'.format(term_id))
                self.redis_client.delete('SET:TERM_ITEM:{}'.format(term_id))
            self.redis_client.delete('SET:DEVICE_TERM:{}'.format(device_id))
            self.redis_client.delete('LST:FRAME:{}'.format(device_id))
            # delete values
            self.found_and_delete('LST:DATA_TIME:{}:*'.format(device_id))
            self.found_and_delete('HS:DATA:{}:*'.format(device_id))
            # delete mapping
            self.found_and_delete('HS:MAPPING:*:{}:*'.format(device_id))
            return web.Response()
        except Exception as e:
            logger.error('del_device failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/devices/del')
    async def del_device_batch(self, request):
        try:
            devices_data = await self._read_data(request)
            device_list = json.loads(devices_data)
            if type(device_list) != list:
                device_list = [device_list]
            for device_id in device_list:
                device_dict = self.redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
                if not device_dict:
                    return web.Response(status=404, text='device_id not found!')
                self.redis_client.publish('CHANNEL:DEVICE_DEL', json.dumps(device_id))
                self.redis_client.delete('HS:DEVICE:{}'.format(device_id))
                self.redis_client.srem('SET:DEVICE', device_id)
                # delete all terms connected to that device
                term_list = self.redis_client.smembers('SET:DEVICE_TERM:{}'.format(device_id))
                for term_id in term_list:
                    self.redis_client.delete('HS:TERM:{}'.format(term_id))
                    self.redis_client.srem('SET:TERM', term_id)
                    self.found_and_delete('HS:TERM_ITEM:{}:*'.format(term_id))
                    self.redis_client.delete('SET:TERM_ITEM:{}'.format(term_id))
                self.redis_client.delete('SET:DEVICE_TERM:{}'.format(device_id))
                self.redis_client.delete('LST:FRAME:{}'.format(device_id))
                # delete values
                self.found_and_delete('LST:DATA_TIME:{}:*'.format(device_id))
                self.found_and_delete('HS:DATA:{}:*'.format(device_id))
                # delete mapping
                self.found_and_delete('HS:MAPPING:*:{}:*'.format(device_id))
            return web.Response()
        except Exception as e:
            logger.error('del_device_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/terms')
    async def create_term(self, request):
        try:
            term_data = await self._read_data(request)
            term_dict = json.loads(term_data)
            logger.debug('new term arg=%s', term_dict)
            found = self.redis_client.exists('HS:TERM:{}'.format(term_dict['id']))
            if found:
                return web.Response(status=409, text='term already exists!')
            self.redis_client.hmset('HS:TERM:{}'.format(term_dict['id']), term_dict)
            self.redis_client.sadd('SET:TERM', term_dict['id'])
            self.redis_client.sadd('SET:DEVICE_TERM:{}'.format(term_dict['device_id']), term_dict['id'])
            self.redis_client.publish('CHANNEL:TERM_ADD"', json.dumps(term_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/terms')
    async def create_term_batch(self, request):
        try:
            term_data = await self._read_data(request)
            term_list = json.loads(term_data)
            if type(term_list) != list:
                term_list = [term_list]
            for term_dict in term_list:
                logger.debug('new term arg=%s', term_dict)
                self.redis_client.hmset('HS:TERM:{}'.format(term_dict['id']), term_dict)
                self.redis_client.sadd('SET:TERM', term_dict['id'])
                self.redis_client.sadd('SET:DEVICE_TERM:{}'.format(term_dict['device_id']), term_dict['id'])
                self.redis_client.publish('CHANNEL:TERM_ADD"', json.dumps(term_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_term_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/terms/{term_id}')
    async def update_term(self, request):
        try:
            term_id = request.match_info['term_id']
            old_term = self.redis_client.hgetall('HS:TERM:{}'.format(term_id))
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
                    self.redis_client.publish('CHANNEL:TERM_DEL', json.dumps(old_term))
                    self.redis_client.publish('CHANNEL:TERM_ADD', term_data)
                else:
                    self.redis_client.publish('CHANNEL:TERM_FRESH', term_data)
            return web.Response()
        except Exception as e:
            logger.error('update_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='DELETE', url=r'/api/v1/terms/{term_id}')
    async def del_term(self, request):
        try:
            term_id = request.match_info['term_id']
            term_info = self.redis_client.hgetall('HS:TERM:{}'.format(term_id))
            if not term_info:
                return web.Response(status=404, text='term_id not found!')
            device_id = term_info['device_id']
            self.redis_client.publish('CHANNEL:TERM_DEL', json.dumps({'device_id': device_id, 'term_id': term_id}))
            self.redis_client.delete('HS:TERM:{}'.format(term_id))
            self.redis_client.srem('SET:TERM', term_id)
            self.redis_client.srem('SET:DEVICE_TERM:{}'.format(term_info['device_id']), term_id)
            self.redis_client.delete('SET:TERM_ITEM:{}'.format(term_id))
            # delete all values
            self.found_and_delete('LST:DATA_TIME:*:{}:*'.format(term_id))
            self.found_and_delete('HS:DATA:*:{}:*'.format(term_id))
            # delete from protocols mapping
            all_keys = set()
            keys = self.redis_client.scan_iter('HS:MAPPING:*')
            for key in keys:
                map_key = self.redis_client.hgetall(key)
                if str(map_key['term_id']) == term_id:
                    all_keys.add(key)
            if all_keys:
                self.redis_client.delete(*all_keys)
            return web.Response()
        except Exception as e:
            logger.error('del_term failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/terms/del')
    async def del_term_batch(self, request):
        try:
            term_data = await self._read_data(request)
            term_list = json.loads(term_data)
            if type(term_list) != list:
                term_list = [term_list]
            for term_id in term_list:
                term_info = self.redis_client.hgetall('HS:TERM:{}'.format(term_id))
                if not term_info:
                    return web.Response(status=404, text='term_id not found!')
                device_id = term_info['device_id']
                self.redis_client.publish('CHANNEL:TERM_DEL', json.dumps({'device_id': device_id, 'term_id': term_id}))
                self.redis_client.delete('HS:TERM:{}'.format(term_id))
                self.redis_client.srem('SET:TERM', term_id)
                self.redis_client.srem('SET:DEVICE_TERM:{}'.format(term_info['device_id']), term_id)
                self.redis_client.delete('SET:TERM_ITEM:{}'.format(term_id))
                # delete all values
                self.found_and_delete('LST:DATA_TIME:*:{}:*'.format(term_id))
                self.found_and_delete('HS:DATA:*:{}:*'.format(term_id))
                # delete from protocols mapping
                all_keys = set()
                keys = self.redis_client.scan_iter('HS:MAPPING:*')
                for key in keys:
                    map_key = self.redis_client.hgetall(key)
                    if str(map_key['term_id']) == term_id:
                        all_keys.add(key)
                if all_keys:
                    self.redis_client.delete(*all_keys)
            return web.Response()
        except Exception as e:
            logger.error('del_term_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/items')
    async def create_item(self, request):
        try:
            item_data = await self._read_data(request)
            item_dict = json.loads(item_data)
            logger.debug('new item arg=%s', item_dict)
            found = self.redis_client.exists('HS:ITEM:{}'.format(item_dict['id']))
            if found:
                return web.Response(status=409, text='item already exists!')
            self.redis_client.hmset('HS:ITEM:{}'.format(item_dict['id']), item_dict)
            self.redis_client.sadd('SET:ITEM', item_dict['id'])
            return web.Response()
        except Exception as e:
            logger.error('create_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/items')
    async def create_item_batch(self, request):
        try:
            item_data = await self._read_data(request)
            item_list = json.loads(item_data)
            if type(item_list) != list:
                item_list = [item_list]
            for item_dict in item_list:
                logger.debug('new item arg=%s', item_dict)
                self.redis_client.hmset('HS:ITEM:{}'.format(item_dict['id']), item_dict)
                self.redis_client.sadd('SET:ITEM', item_dict['id'])
            return web.Response()
        except Exception as e:
            logger.error('create_item_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/items/{item_id}')
    async def update_item(self, request):
        try:
            item_id = request.match_info['item_id']
            old_item = self.redis_client.hgetall('HS:ITEM:{}'.format(item_id))
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
            item_id = request.match_info['item_id']
            found = self.redis_client.exists('HS:ITEM:{}'.format(item_id))
            if not found:
                return web.Response(status=404, text='item_id not found!')
            self.redis_client.delete('HS:ITEM:{}'.format(item_id))
            self.redis_client.srem('SET:ITEM', item_id)
            # delete from term->item set
            self.found_and_delete('SET:TERM_ITEM:*')
            # delete from term->item hash, TODO: publish msg to CHANNEL:TERM_ITEM_DEL
            self.found_and_delete('HS:TERM_ITEM:*:{}'.format(item_id))
            # delete from protocols mapping
            all_keys = set()
            keys = self.redis_client.scan_iter('HS:MAPPING:*')
            for key in keys:
                map_key = self.redis_client.hgetall(key)
                if map_key and str(map_key['item_id']) == item_id:
                    all_keys.add(key)
            if all_keys:
                self.redis_client.delete(*all_keys)
            # delete all values
            self.found_and_delete('LST:DATA_TIME:*:*:{}'.format(item_id))
            self.found_and_delete('HS:DATA:*:*:{}'.format(item_id))
            return web.Response()
        except Exception as e:
            logger.error('del_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/items/del')
    async def del_item_batch(self, request):
        try:
            item_data = await self._read_data(request)
            item_list = json.loads(item_data)
            if type(item_list) != list:
                item_list = [item_list]
            for item_id in item_list:
                found = self.redis_client.exists('HS:ITEM:{}'.format(item_id))
                if not found:
                    return web.Response(status=404, text='item_id not found!')
                self.redis_client.delete('HS:ITEM:{}'.format(item_id))
                self.redis_client.srem('SET:ITEM', item_id)
                # delete from term->item set
                self.found_and_delete('SET:TERM_ITEM:*')
                # delete from term->item hash, TODO: publish msg to CHANNEL:TERM_ITEM_DEL
                self.found_and_delete('HS:TERM_ITEM:*:{}'.format(item_id))
                # delete from protocols mapping
                all_keys = set()
                keys = self.redis_client.scan_iter('HS:MAPPING:*')
                for key in keys:
                    map_key = self.redis_client.hgetall(key)
                    if map_key and str(map_key['item_id']) == item_id:
                        all_keys.add(key)
                if all_keys:
                    self.redis_client.delete(*all_keys)
                # delete all values
                self.found_and_delete('LST:DATA_TIME:*:*:{}'.format(item_id))
                self.found_and_delete('HS:DATA:*:*:{}'.format(item_id))
            return web.Response()
        except Exception as e:
            logger.error('del_item_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/terms/{term_id}/items')
    async def create_term_item(self, request):
        try:
            term_item_data = await self._read_data(request)
            term_item_dict = json.loads(term_item_data)
            logger.debug('new term_item arg=%s', term_item_dict)
            term_id = request.match_info['term_id']
            if term_id != str(term_item_dict['term_id']):
                return web.Response(status=400, text='term_id mismatch in url and body!')
            item_id = term_item_dict['item_id']
            found = self.redis_client.exists('HS:TERM:{}'.format(term_id))
            if not found:
                return web.Response(status=404, text='term_id not found!')
            found = self.redis_client.exists('HS:ITEM:{}'.format(item_id))
            if not found:
                return web.Response(status=404, text='item_id not found!')
            found = self.redis_client.exists('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
            if found:
                return web.Response(status=409, text='term_item already exists!')
            term_info = self.redis_client.hgetall('HS:TERM:{}'.format(term_id))
            device_id = term_info['device_id']
            term_item_dict.update({'device_id': device_id})
            device_info = self.redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
            self.redis_client.hmset('HS:TERM_ITEM:{}:{}'.format(term_id, item_id), term_item_dict)
            self.redis_client.sadd('SET:TERM_ITEM:{}'.format(term_id), item_id)
            if 'protocol_code' in term_item_dict:
                # delete old mapping
                all_keys = set()
                keys = self.redis_client.scan_iter('HS:MAPPING:{}:*:*'.format(device_info['protocol'].upper()))
                for key in keys:
                    map_key = self.redis_client.hgetall(key)
                    if str(map_key['term_id']) == term_id and str(map_key['item_id']) == item_id:
                        all_keys.add(key)
                if all_keys:
                    self.redis_client.delete(*all_keys)
                self.redis_client.hmset('HS:MAPPING:{}:{}:{}'.format(device_info['protocol'].upper(), device_id,
                                                                     term_item_dict['protocol_code']), term_item_dict)
            self.redis_client.publish('CHANNEL:TERM_ITEM_ADD', json.dumps(term_item_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/term_items')
    async def create_term_item_batch(self, request):
        try:
            term_item_data = await self._read_data(request)
            term_item_list = json.loads(term_item_data)
            if type(term_item_list) != list:
                term_item_list = [term_item_list]
            for term_item_dict in term_item_list:
                logger.debug('new term_item arg=%s', term_item_dict)
                device_id = term_item_dict['device_id']
                term_id = term_item_dict['term_id']
                item_id = term_item_dict['item_id']
                self.redis_client.hmset('HS:TERM_ITEM:{}:{}'.format(term_id, item_id), term_item_dict)
                self.redis_client.sadd('SET:TERM_ITEM:{}'.format(term_id), item_id)
                if 'protocol' in term_item_dict and 'protocol_code' in term_item_dict:
                    self.redis_client.hmset('HS:MAPPING:{}:{}:{}'.format(
                            term_item_dict['protocol'].upper(), device_id, term_item_dict['protocol_code']),
                            term_item_dict)
                self.redis_client.publish('CHANNEL:TERM_ITEM_ADD', json.dumps(term_item_dict))
            return web.Response()
        except Exception as e:
            logger.error('create_term_item_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='PUT', url=r'/api/v1/terms/{term_id}/items/{item_id}')
    async def update_term_item(self, request):
        try:
            term_item_data = await self._read_data(request)
            term_item_dict = json.loads(term_item_data)
            term_id = request.match_info['term_id']
            item_id = request.match_info['item_id']
            old_term_item = self.redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
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
            term_id = request.match_info['term_id']
            item_id = request.match_info['item_id']
            term_item_dict = self.redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
            if not term_item_dict:
                return web.Response(status=404, text='term_item not found!')
            term_info = self.redis_client.hgetall('HS:TERM:{}'.format(term_id))
            device_id = term_info['device_id']
            device_info = self.redis_client.hgetall('HS:DEVICE:{}'.format(device_id))
            self.redis_client.publish('CHANNEL:TERM_ITEM_DEL',
                                      json.dumps({'device_id': device_id, 'term_id': term_id, 'item_id': item_id}))
            self.redis_client.delete('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
            self.redis_client.srem('SET:TERM_ITEM:{}'.format(term_id), item_id)
            if 'protocol_code' in term_item_dict:
                self.redis_client.delete('HS:MAPPING:{}:{}:{}'.format(
                        device_info['protocol'].upper(), device_id, term_item_dict['protocol_code']))
            # delete all values
            self.found_and_delete('LST:DATA_TIME:*:{}:{}'.format(term_id, item_id))
            self.found_and_delete('HS:DATA:*:{}:{}'.format(term_id, item_id))
            return web.Response()
        except Exception as e:
            logger.error('del_term_item failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v2/term_items/del')
    async def del_term_item_batch(self, request):
        try:
            term_item_data = await self._read_data(request)
            term_item_list = json.loads(term_item_data)
            if type(term_item_list) != list:
                term_item_list = [term_item_list]
            for term_item_dict in term_item_list:
                device_id = term_item_dict['device_id']
                term_id = term_item_dict['term_id']
                item_id = term_item_dict['item_id']
                term_item_dict = self.redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                if not term_item_dict:
                    return web.Response(status=404, text='term_item not found!')
                self.redis_client.publish('CHANNEL:TERM_ITEM_DEL',
                                          json.dumps({'device_id': device_id, 'term_id': term_id, 'item_id': item_id}))
                self.redis_client.delete('HS:TERM_ITEM:{}:{}'.format(term_id, item_id))
                self.redis_client.srem('SET:TERM_ITEM:{}'.format(term_id), item_id)
                if 'protocol_code' in term_item_dict:
                    self.redis_client.delete('HS:MAPPING:{}:{}:{}'.format(
                            term_item_dict['protocol'].upper(), device_id, term_item_dict['protocol_code']))
                # delete all values
                self.found_and_delete('LST:DATA_TIME:*:{}:{}'.format(term_id, item_id))
                self.found_and_delete('HS:DATA:*:{}:{}'.format(term_id, item_id))
            return web.Response()
        except Exception as e:
            logger.error('del_term_item_batch failed: %s', repr(e), exc_info=True)
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/device_call')
    async def device_call(self, request):
        sub_client = None
        channel_name = None
        try:
            sub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='localhost'),
                                                      config.getint('REDIS', 'port', fallback=6379)),
                                                     db=config.getint('REDIS', 'db', fallback=1))
            call_data = await self._read_data(request)
            call_data_dict = json.loads(call_data)
            logger.debug('new call_data arg=%s', call_data_dict)
            found = await sub_client.exists('HS:DEVICE:{}'.format(call_data_dict['device_id']))
            if not found:
                return web.Response(status=404, text='device_id not found!')
            found = await sub_client.exists('HS:TERM:{}'.format(call_data_dict['term_id']))
            if not found:
                return web.Response(status=404, text='term_id not found!')
            found = await sub_client.exists('HS:ITEM:{}'.format(call_data_dict['item_id']))
            if not found:
                return web.Response(status=404, text='item_id not found!')
            found = await sub_client.exists('HS:TERM_ITEM:{}:{}'.format(
                    call_data_dict['term_id'], call_data_dict['item_id']))
            if not found:
                return web.Response(status=404, text='term_item not found!')
            channel_name = 'CHANNEL:DEVICE_CALL:{}:{}:{}'.format(
                    call_data_dict['device_id'], call_data_dict['term_id'], call_data_dict['item_id'])
            res = await sub_client.subscribe(channel_name)
            cb = asyncio.futures.Future(loop=self.io_loop)

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get_json()
                    logger.debug('device_call got msg: %s', msg)
                    if not cb.done():
                        cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
            self.redis_client.publish('CHANNEL:DEVICE_CALL', call_data)
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.unsubscribe(channel_name)
            sub_client.close()
            await tsk
            return JSON(rst)
        except Exception as e:
            logger.exception(e)
            logger.error('device_call failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name:
                await sub_client.unsubscribe(channel_name)
                sub_client.close()
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/device_ctrl')
    async def device_ctrl(self, request):
        sub_client = None
        channel_name = None
        try:
            sub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='localhost'),
                                                      config.getint('REDIS', 'port', fallback=6379)),
                                                     db=config.getint('REDIS', 'db', fallback=1))
            ctrl_data = await self._read_data(request)
            ctrl_data_dict = json.loads(ctrl_data)
            logger.debug('new ctrl_data arg=%s', ctrl_data_dict)
            found = await sub_client.exists('HS:DEVICE:{}'.format(ctrl_data_dict['device_id']))
            if not found:
                return web.Response(status=404, text='device_id not found!')
            found = await sub_client.exists('HS:TERM:{}'.format(ctrl_data_dict['term_id']))
            if not found:
                return web.Response(status=404, text='term_id not found!')
            found = await sub_client.exists('HS:ITEM:{}'.format(ctrl_data_dict['item_id']))
            if not found:
                return web.Response(status=404, text='item_id not found!')
            found = await sub_client.exists(
                    'HS:TERM_ITEM:{}:{}'.format(ctrl_data_dict['term_id'], ctrl_data_dict['item_id']))
            if not found:
                return web.Response(status=404, text='term_item not found!')
            channel_name = 'CHANNEL:DEVICE_CTRL:{}:{}:{}'.format(
                    ctrl_data_dict['device_id'], ctrl_data_dict['term_id'], ctrl_data_dict['item_id'])
            res = await sub_client.subscribe(channel_name)
            cb = asyncio.futures.Future(loop=self.io_loop)

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get_json()
                    logger.debug('device_ctrl got msg: %s', msg)
                    if not cb.done():
                        cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
            self.redis_client.publish('CHANNEL:DEVICE_CTRL', ctrl_data)
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.unsubscribe(channel_name)
            sub_client.close()
            await tsk
            return JSON(rst)
        except Exception as e:
            logger.exception(e)
            logger.error('device_ctrl failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name:
                await sub_client.unsubscribe(channel_name)
                sub_client.close()
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/formula_check')
    async def formula_check(self, request):
        sub_client = None
        channel_name = None
        try:
            sub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='localhost'),
                                                      config.getint('REDIS', 'port', fallback=6379)),
                                                     db=config.getint('REDIS', 'db', fallback=1))
            formula_data = await self._read_data(request)
            formula_dict = json.loads(formula_data)
            logger.debug('formula_check arg=%s', formula_dict)
            channel_name = 'CHANNEL:FORMULA_CHECK_RESULT:{}'.format(len(repr(formula_dict)))
            res = await sub_client.subscribe(channel_name)
            cb = asyncio.futures.Future(loop=self.io_loop)

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get(encoding='utf-8')
                    if not cb.done():
                        cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
            self.redis_client.publish('CHANNEL:FORMULA_CHECK', formula_data)
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.unsubscribe(channel_name)
            sub_client.close()
            await tsk
            return web.Response(status=200, text=rst)
        except Exception as e:
            logger.exception(e)
            logger.error('formula_check failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name:
                await sub_client.unsubscribe(channel_name)
                sub_client.close()
            return web.Response(status=400, text=repr(e))

    @param_function(method='POST', url=r'/api/v1/sql_check')
    async def sql_check(self, request):
        sub_client = None
        channel_name = None
        try:
            sub_client = await aioredis.create_redis((config.get('REDIS', 'host', fallback='localhost'),
                                                      config.getint('REDIS', 'port', fallback=6379)),
                                                     db=config.getint('REDIS', 'db', fallback=1))
            term_item_data = await self._read_data(request)
            term_item_dict = json.loads(term_item_data)
            logger.debug('sql_check arg=%s', term_item_dict)
            channel_name = 'CHANNEL:SQL_CHECK_RESULT:{}'.format(len(repr(term_item_dict)))
            res = await sub_client.subscribe(channel_name)
            cb = asyncio.futures.Future(loop=self.io_loop)

            async def reader(ch):
                while await ch.wait_message():
                    msg = await ch.get(encoding='utf-8')
                    if not cb.done():
                        cb.set_result(msg)

            tsk = asyncio.ensure_future(reader(res[0]), loop=self.io_loop)
            self.redis_client.publish('CHANNEL:SQL_CHECK', term_item_data)
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.unsubscribe(channel_name)
            sub_client.close()
            await tsk
            return web.Response(status=200, text=rst)
        except Exception as e:
            logger.exception(e)
            logger.error('sql_check failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name:
                await sub_client.unsubscribe(channel_name)
                sub_client.close()
            return web.Response(status=400, text=repr(e))


def main():
    api_server = None
    parser = argparse.ArgumentParser(description='PyDataColl RESTful Server')
    parser.add_argument('--port', type=int, help='http listening port, default: 8080')
    parser.add_argument('--production', action='store_true', help='run in production environment')
    parser.add_argument('--mock-iec104', action='store_true', help='run iec104 mock server in sub-process')
    args = parser.parse_args()
    iec104_server = None
    try:
        if args.mock_iec104:
            from test.mock_device.iec104device import run_server
            from multiprocessing import Process
            iec104_server = Process(target=run_server)
            iec104_server.start()
        api_server = APIServer(port=args.port, production=args.production)
        logger.info('serving on %s', api_server.web_server.sockets[0].getsockname())
        print('server is running.')
        print('used config file:', config_file)
        print('log stored in:', app_dir.user_log_dir)
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        pass
    except Exception as ee:
        logger.info('got error: %s', repr(ee), exc_info=True)
    finally:
        api_server and api_server.stop_server()
        iec104_server and iec104_server.terminate()
    print('server is stopped.')

if __name__ == '__main__':
    main()
