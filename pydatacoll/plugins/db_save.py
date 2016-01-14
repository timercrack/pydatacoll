from collections import namedtuple
import math
try:
    import ujson as json
except ImportError:
    import json
import pymysql
from pydatacoll.plugins import BaseModule
from pydatacoll.utils.asteval import Interpreter
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

    async def start(self):
        self.conn = self.conn or pymysql.Connect(**PLUGIN_PARAM)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    async def stop(self):
        self.conn and self.conn.close()

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
                if not last_value or not math.isclose(param.value, float(last_value), rel_tol=1e-04):
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
