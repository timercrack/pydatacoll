from collections import namedtuple
import math

try:
    import ujson as json
except ImportError:
    import json
import aiomysql
from pydatacoll.plugins import BaseModule
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('DBSaver')

PLUGIN_PARAM = dict(
        host='127.0.0.1', port=3306,
        user='pydatacoll', password='pydatacoll',
        db='test', no_delay=None
)


class DBSaver(BaseModule):
    mysql_pool = None

    async def start(self):
        self.mysql_pool = await aiomysql.create_pool(**PLUGIN_PARAM)

    async def stop(self):
        if self.mysql_pool is not None:
            self.mysql_pool.terminate()
            await self.mysql_pool.wait_closed()
            self.mysql_pool.close()

    @param_function(channel='CHANNEL:DEVICE_DATA:*')
    async def save_mysql(self, channel, data_dict):
        try:
            logger.debug('save_mysql: got msg, channel=%s, dat_dict=%s', channel, data_dict)
            param = namedtuple('Param', data_dict.keys())(**data_dict)
            with (await self.redis_pool) as redis_client:
                term_item = await redis_client.hgetall('HS:TERM_ITEM:{param.term_id}:{param.item_id}'.format(param=param))
                if term_item and 'db_save_sql' in term_item:
                    last_value = None
                    last_key = await redis_client.lindex('LST:DATA_TIME:{}:{}:{}'.format(
                            param.device_id, param.term_id, param.item_id), -2)
                    if last_key:
                        last_value = await redis_client.hget('HS:DATA:{}:{}:{}'.format(
                            param.device_id, param.term_id, param.item_id), last_key)
                    if not last_value or not math.isclose(param.value, float(last_value), rel_tol=1e-04):
                        conn = await self.mysql_pool.acquire()
                        # print('conn=', conn)
                        # with (await self.mysql_pool) as conn:
                        cur = await conn.cursor()
                        save_sql = term_item['db_save_sql'].format(PARAM=param)
                        logger.debug('save_mysql: saving data, sql=%s', save_sql)
                        await cur.execute(save_sql)
                        await conn.commit()
                        await conn.ensure_closed()
                        self.mysql_pool.release(conn)
        except Exception as ee:
            logger.error('save_mysql failed: %s', repr(ee), exc_info=True)
