from numbers import Number
import functools
from collections import namedtuple
import math
import datetime
from numpy.matlib import randn
try:
    import ujson as json
except ImportError:
    import json
from asteval import Interpreter
import numpy as np
import pandas as pd
from pydatacoll.plugins import BaseModule
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('FormulaCalc')


class FormulaCalc(BaseModule):
    not_implemented = True
    formula_dict = dict()  # HS:TERM_ITEM:{term_id}:{item_id} -> value of HS:FORMULA:{formula_id}
    pandas_dict = dict()  # HS:DATA:{formula_id}:{term_id}:{item_id} -> pandas.Series
    interp = Interpreter()

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
        await self.del_formula()

    @param_function(channel='CHANNEL:FORMULA_ADD')
    async def add_formula(self, _, formula_dict: dict):
        await self.fresh_formula(_, formula_dict)

    @param_function(channel='CHANNEL:FORMULA_FRESH')
    async def fresh_formula(self, _, formula_dict: dict):
        formula_id = self.formula_dict.get(formula_dict['id'])
        if formula_id:
            await self.del_formula(formula_id)
        self.formula_dict[formula_dict['id']] = formula_dict
        for idx in range(8):
            param = formula_dict.get('p{}'.format(idx))
            if param and param not in self.pandas_dict:
                with (await self.redis_pool) as redis_client:
                    data_dict = await redis_client.hgetall(param)
                    self.pandas_dict[param] = pd.Series(data_dict, dtype=float)
                    self.pandas_dict[param].index = self.pandas_dict[param].index.to_datetime()
        with (await self.redis_pool) as redis_client:
            data_key = redis_client.lindex("LST:DATA_IME:{}:{}:{}".format(
                formula_dict['device_id'], formula_dict['term_id'], formula_dict['item_id']
            ))
            if not data_key:
                await self.calculate(formula_id)

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
            with (await self.redis_pool) as redis_client:
                formula_list = await redis_client.smembers('SET:FORMULA_PARAM:{}:{}:{}'.format(
                        param.device_id, param.term_id, param.item_id))
                if formula_list:
                    last_value = None
                    last_key = await redis_client.lindex('LST:DATA_TIME:{}:{}:{}'.format(
                            param.device_id, param.term_id, param.item_id), -1)
                    if last_key:
                        last_value = await redis_client.hget('HS:DATA:{}:{}:{}'.format(
                            param.device_id, param.term_id, param.item_id), last_key)
                    if not last_value or not math.isclose(param.value, last_value[1], rel_tol=1e-04):
                        self.pandas_dict[pd.to_datetime(param.time)] = float(param.value)
                        for formula_id in formula_list:
                            await self.calculate(formula_id)
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
            if isinstance(value, Number):
                with (await self.redis_pool) as redis_client:
                    data_key = "{}:{}:{}".format(formula['device_id'], formula['term_id'], formula['item_id'])
                    time_str = datetime.datetime.now().isoformat()
                    await redis_client.hset("HS:DATA:{}".format(data_key), time_str, value)
                    await redis_client.rpush("LST:DATA_TIME:{}".format(data_key), time_str)
                    await redis_client.publish("CHANNEL:DEVICE_DATA:{}".format(data_key), json.dumps({
                        'device_id': formula['device_id'], 'term_id': formula['term_id'],
                        'item_id': formula['item_id'], 'time': time_str, 'value': value,
                    }))
        except Exception as ee:
            logger.error('calc failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:FORMULA_CHECK')
    async def formula_check(self, _, check_dict: dict):
        try:
            with (await self.redis_pool) as redis_client:
                check_rst = self.do_check(**check_dict)
                await redis_client.publish("CHANNEL:FORMULA_CHECK_RESULT:{}".format(
                        len(check_dict['formula'])), check_rst)
        except Exception as ee:
            logger.error('param_update failed: %s', repr(ee), exc_info=True)

    @functools.lru_cache(typed=True)
    def do_check(self, **check_dict):
        rst = 'OK'
        try:
            interp = Interpreter()
            interp.symtable['np'] = np
            interp.symtable['pd'] = pd
            ts = pd.Series(randn(10), index=pd.date_range(start='1/1/2016', periods=10))
            for idx in range(8):
                param_key = 'p{}'.format(idx)
                if param_key in check_dict:
                    self.interp.symtable[param_key] = ts
            value = interp(check_dict['formula'])
            if len(interp.error) > 0:
                rst = interp.error_msg
            elif not isinstance(value, Number):
                rst = "result type must be Number!"

        except Exception as ee:
            logger.error('do_check failed: %s', repr(ee), exc_info=True)
            rst = repr(ee)
        finally:
            return rst
