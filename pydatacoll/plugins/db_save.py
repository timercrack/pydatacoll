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

from collections import namedtuple
import math

try:
    import ujson as json
except ImportError:
    import json
import pymysql
from pydatacoll.plugins import BaseModule
from asteval import Interpreter
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('DBSaver')

PLUGIN_PARAM = dict(
        host=config.get('MYSQL', 'host', fallback='127.0.0.1'),
        port=config.getint('MYSQL', 'port', fallback=3306),
        user=config.get('MYSQL', 'user', fallback='pydatacoll'),
        password=config.get('MYSQL', 'password', fallback='pydatacoll'),
        db=config.get('MYSQL', 'db', fallback='pydatacoll'),
)


class DBSaver(BaseModule):
    # not_implemented = True
    interp = Interpreter(use_numpy=False)
    conn = None
    cursor = None
    save_unchanged = config.getboolean('DBSaver', 'save_unchanged', fallback=False)

    async def start(self):
        self.conn = self.conn or pymysql.Connect(**PLUGIN_PARAM)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    async def stop(self):
        self.conn and self.conn.close()

    @param_function(channel='CHANNEL:SQL_CHECK')
    async def check_sql(self, channel, data_dict):
        check_rst = 'OK'
        try:
            logger.debug('check_sql: got msg, channel=%s, dat_dict=%s', channel, data_dict)
            term_item = self.redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(
                    data_dict['term_id'], data_dict['item_id']))
            data_dict.update(term_item)
            param = namedtuple('Param', data_dict.keys())(**data_dict)
            if 'db_save_sql' not in data_dict and 'db_warn_sql' not in data_dict:
                check_rst = 'not found sql to check'
            if 'db_save_sql' in data_dict:
                self.cursor.execute(param.db_save_sql.format(PARAM=param))
            if 'db_warn_sql' in data_dict:
                self.cursor.execute(param.db_warn_sql.format(PARAM=param))
        except Exception as ee:
            check_rst = ee.args[0]
        finally:
            pub_ch = "CHANNEL:SQL_CHECK_RESULT:{}".format(len(repr(data_dict)))
            logger.debug('check_sql: publish check result to %s', pub_ch)
            self.redis_client.publish(pub_ch, check_rst)

    @param_function(channel='CHANNEL:DEVICE_DATA:*')
    async def save_mysql(self, channel, data_dict):
        try:
            logger.debug('save_mysql: got msg, channel=%s, dat_dict=%s', channel, data_dict)
            term_item = self.redis_client.hgetall('HS:TERM_ITEM:{}:{}'.format(
                    data_dict['term_id'], data_dict['item_id']))
            data_dict.update(term_item)
            param = namedtuple('Param', data_dict.keys())(**data_dict)
            if term_item and 'db_save_sql' in term_item:
                last_value = None
                last_key = self.redis_client.lindex('LST:DATA_TIME:{}:{}:{}'.format(
                        param.device_id, param.term_id, param.item_id), -2)
                if last_key:
                    last_value = self.redis_client.hget('HS:DATA:{}:{}:{}'.format(
                            param.device_id, param.term_id, param.item_id), last_key)
                if not last_value or self.save_unchanged or \
                        not math.isclose(param.value, float(last_value), rel_tol=1e-04):
                    sql = term_item['db_save_sql'].format(PARAM=param)
                    logger.debug('save_mysql: save data, sql=%s', sql)
                    self.cursor.execute(sql)
            if term_item and 'do_verify' in term_item and 'db_warn_sql' in term_item:
                self.interp.symtable['param'] = param
                self.interp.symtable['value'] = str(param.value)
                check_rst = self.interp(param.do_verify)
                if not check_rst:
                    sql = term_item['db_warn_sql'].format(PARAM=param)
                    logger.debug('save_mysql: save alert, sql=%s', sql)
                    self.cursor.execute(sql)
        except Exception as ee:
            logger.error('save_mysql failed: %s', repr(ee), exc_info=True)


if __name__ == '__main__':
    DBSaver.run()
