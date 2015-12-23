from io import StringIO
from numbers import Number
import functools
from collections import namedtuple
import math
import datetime
try:
    import ujson as json
except ImportError:
    import json
from pydatacoll.utils.asteval import Interpreter
import numpy as np
import pandas as pd
from pydatacoll.plugins import BaseModule
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('FormulaCalc')


class FormulaCalc(BaseModule):
    formula_dict = dict()  # HS:TERM_ITEM:{term_id}:{item_id} -> value of HS:FORMULA:{formula_id}
    pandas_dict = dict()  # HS:DATA:{formula_id}:{term_id}:{item_id} -> pandas.Series
    interp = Interpreter(use_numpy=False)

    async def start(self):
        try:
            with (await self.redis_pool) as redis_client:
                self.interp.symtable['np'] = np
                self.interp.symtable['pd'] = pd
                formula_dict = await redis_client.smembers('SET:FORMULA')
                for formula_id in formula_dict:
                    formula_dict = await redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
                    if formula_dict:
                        await self.add_formula(None, formula_dict)
        except Exception as ee:
            logger.error('start failed: %s', repr(ee), exc_info=True)

    async def stop(self):
        await self.del_formula(None)

    @param_function(channel='CHANNEL:FORMULA_ADD')
    async def add_formula(self, _, formula_dict: dict):
        await self.fresh_formula(_, formula_dict)

    @param_function(channel='CHANNEL:FORMULA_FRESH')
    async def fresh_formula(self, _, formula_dict: dict):
        try:
            formula_id = str(formula_dict['id'])
            with (await self.redis_pool) as redis_client:
                if formula_id in self.formula_dict:
                    await self.del_formula(_, formula_id)
                for idx in range(8):
                    param = formula_dict.get('p{}'.format(idx))
                    if param and param not in self.pandas_dict:
                        data_dict = await redis_client.hgetall(param)
                        self.pandas_dict[param] = pd.Series(data_dict, dtype=float)
                        self.pandas_dict[param].index = self.pandas_dict[param].index.to_datetime()
                self.formula_dict[formula_id] = formula_dict
                logger.debug("fresh_formula add new formula: %s", self.formula_dict)
                data_key = await redis_client.lindex("LST:DATA_TIME:{}:{}:{}".format(
                    formula_dict['device_id'], formula_dict['term_id'], formula_dict['item_id']), -1)
                if not data_key:
                    logger.debug('fresh_formula formula value not exist, calculate now')
                    await self.calculate(formula_id)
        except Exception as ee:
            logger.error('fresh_formula failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:FORMULA_DEL')
    async def del_formula(self, _, formula_id=None):
        if formula_id is None:
            self.formula_dict.clear()
        else:
            self.formula_dict.pop(formula_id)

    @param_function(channel='CHANNEL:DEVICE_DATA:*')
    async def param_update(self, channel: str, data_dict: dict):
        try:
            logger.debug('param_update: got msg, channel=%s, dat_dict=%s', channel, data_dict)
            param = namedtuple('Param', data_dict.keys())(**data_dict)
            data_dict_key = 'HS:DATA:{}:{}:{}'.format(param.device_id, param.term_id, param.item_id)
            with (await self.redis_pool) as redis_client:
                formula_param_key = 'SET:FORMULA_PARAM:{}:{}:{}'.format(param.device_id, param.term_id, param.item_id)
                formula_list = await redis_client.smembers(formula_param_key)
                if formula_list:
                    logger.debug("this arg has formula refer to, formula list=%s", formula_list)
                    last_value = None
                    data_time_key = 'LST:DATA_TIME:{}:{}:{}'.format(param.device_id, param.term_id, param.item_id)
                    last_key = await redis_client.lindex(data_time_key, -2)
                    if last_key:
                        last_value = await redis_client.hget(data_dict_key, last_key)
                    else:
                        logger.debug("not found data in %s", data_time_key)
                    if not last_value or not math.isclose(param.value, float(last_value), rel_tol=1e-04):
                        self.pandas_dict[data_dict_key][pd.to_datetime(param.time)] = float(param.value)
                        logger.debug('%s value=%s,last_value=%s changed, calculate formula',
                                     data_dict_key, param.value, last_value)
                        for formula_id in formula_list:
                            await self.calculate(formula_id)
                    else:
                        logger.debug("%s value=%s,last_value=%s not change, ignored",
                                     data_dict_key, param.value, last_value)
                else:
                    logger.debug("%s not exists, ignored", formula_param_key)
        except Exception as ee:
            logger.error('param_update failed: %s', repr(ee), exc_info=True)

    async def calculate(self, formula_id):
        try:
            formula = self.formula_dict.get(formula_id)
            for idx in range(8):
                param_key = 'p{}'.format(idx)
                if param_key in self.interp.symtable:
                    del self.interp.symtable[param_key]
                if param_key in formula:
                    self.interp.symtable[param_key] = self.pandas_dict[formula[param_key]]
            value = self.interp(self.formula_dict[formula_id]['formula'])
            logger.debug("calculate formula=%s, value=%s, type(value)=%s",
                         self.formula_dict[formula_id]['formula'], value, type(value))
            if isinstance(value, Number):
                time_str = datetime.datetime.now().isoformat()
                value = float(value)
            elif isinstance(value, pd.Series):
                time_str = value.index[0].isoformat()
                value = float(value[0])
            else:
                return
            with (await self.redis_pool) as redis_client:
                data_key = "{}:{}:{}".format(formula['device_id'], formula['term_id'], formula['item_id'])
                last_value = await redis_client.hget('HS:DATA:{}'.format(data_key), time_str)
                if last_value and math.isclose(value, float(last_value), rel_tol=1e-04):
                    logger.debug("calculate value=%s,last_value=%s not change, ignored", value, last_value)
                    return
                await redis_client.hset("HS:DATA:{}".format(data_key), time_str, value)
                await redis_client.rpush("LST:DATA_TIME:{}".format(data_key), time_str)
                await redis_client.publish("CHANNEL:DEVICE_DATA:{}".format(data_key), json.dumps({
                    'device_id': formula['device_id'], 'term_id': formula['term_id'],
                    'item_id': formula['item_id'], 'time': time_str, 'value': value}))
        except Exception as ee:
            logger.error('calc failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:FORMULA_CHECK')
    async def formula_check(self, _, check_dict: dict):
        try:
            with (await self.redis_pool) as redis_client:
                check_rst = self.do_check(**check_dict)
                pub_ch = "CHANNEL:FORMULA_CHECK_RESULT:{}".format(len(check_dict['formula']))
                await redis_client.publish(pub_ch, check_rst)
        except Exception as ee:
            logger.error('param_update failed: %s', repr(ee), exc_info=True)

    @functools.lru_cache(typed=True)
    def do_check(self, **check_dict):
        rst = 'OK'
        try:
            output = StringIO()
            interp = Interpreter(writer=output, err_writer=output, use_numpy=False)
            interp.symtable['np'] = np
            interp.symtable['pd'] = pd
            ts = pd.Series(np.random.randn(10), index=pd.date_range(start='1/1/2016', periods=10))
            for idx in range(8):
                param_key = 'p{}'.format(idx)
                if param_key in check_dict:
                    interp.symtable[param_key] = ts
            value = interp(check_dict['formula'])
            if len(interp.error) > 0:
                rst = output.getvalue()
            elif not isinstance(value, Number) and not isinstance(value, pd.Series):
                rst = "result type must be Number or Series!"

        except Exception as ee:
            logger.error('do_check failed: %s', repr(ee), exc_info=True)
            rst = repr(ee)
        finally:
            return rst
