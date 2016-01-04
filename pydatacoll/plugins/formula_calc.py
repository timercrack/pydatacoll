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
    # not_implemented = True
    formula_dict = dict()  # HS:TERM_ITEM:{term_id}:{item_id} -> value of HS:FORMULA:{formula_id}
    pandas_dict = dict()  # HS:DATA:{formula_id}:{term_id}:{item_id} -> pandas.Series
    interp = Interpreter(use_numpy=False)

    async def start(self):
        try:
            with (await self.redis_pool) as redis_client:
                self.interp.symtable['np'] = np
                self.interp.symtable['pd'] = pd
                formula_list = await redis_client.smembers('SET:FORMULA')
                for formula_id in formula_list:
                    formula = await redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
                    if formula:
                        await self.add_formula(None, formula)
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
                for param, param_value in formula_dict.items():
                    if param.startswith('p') and param_value not in self.pandas_dict:
                        data_dict = await redis_client.hgetall('HS:DATA:{}'.format(param_value))
                        self.pandas_dict[param_value] = pd.Series(data_dict, dtype=float)
                        self.pandas_dict[param_value].index = self.pandas_dict[param_value].index.to_datetime()
                formula_dict['result'] = "{}:{}:{}".format(
                        formula_dict['device_id'], formula_dict['term_id'], formula_dict['item_id'])
                self.formula_dict[formula_id] = formula_dict
                logger.debug("fresh_formula add new formula: %s", self.formula_dict)
                data_key = await redis_client.lindex("LST:DATA_TIME:{}".format(formula_dict['result']), -1)
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
    async def param_update(self, channel: bytes, data_dict: dict):
        try:
            logger.debug('param_update: got msg, channel=%s, dat_dict=%s', channel, data_dict)
            param = namedtuple('Param', data_dict.keys())(**data_dict)
            partial_key = channel[20:].decode('utf8')
            data_dict_key = 'HS:DATA:{}'.format(partial_key)
            with (await self.redis_pool) as redis_client:
                formula_param_key = 'SET:FORMULA_PARAM:{}'.format(partial_key)
                formula_list = await redis_client.smembers(formula_param_key)
                if formula_list:
                    logger.debug("this arg has formula refer to, formula list=%s", formula_list)
                    last_value = None
                    data_time_key = 'LST:DATA_TIME:{}'.format(partial_key)
                    last_key = await redis_client.lindex(data_time_key, -2)
                    if last_key:
                        last_value = await redis_client.hget(data_dict_key, last_key)
                    else:
                        logger.debug("not found data in %s", data_time_key)
                    if not last_value or not math.isclose(param.value, float(last_value), rel_tol=1e-04):
                        self.pandas_dict[partial_key][pd.to_datetime(param.time)] = float(param.value)
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
            if formula is None:
                logger.warn('calculate can not found formula, formula_id=%', formula_id)
                return
            for param, param_value in formula.items():
                if param.startswith('p'):
                    if param in self.interp.symtable:
                        del self.interp.symtable[param]
                    self.interp.symtable[param] = self.pandas_dict[param_value]
            value = self.interp(formula['formula'])
            logger.debug("calculate formula=%s, value=%s, type(value)=%s", formula['formula'], value, type(value))
            if isinstance(value, Number):
                time_str = datetime.datetime.now().isoformat()
                value = float(value)
            elif isinstance(value, pd.Series):
                time_str = value.index[0].isoformat()
                value = float(value[0])
            else:
                logger.warn('calculate value type=%s, ignored.', type(value))
                return
            if math.isnan(value):
                logger.warn('calculate formula=%s, value=NaN, ignored.', formula['formula'])
                return
            with (await self.redis_pool) as redis_client:
                last_value = await redis_client.hget('HS:DATA:{}'.format(formula['result']), time_str)
                if last_value and math.isclose(value, float(last_value), rel_tol=1e-04):
                    logger.debug("calculate value=%s,last_value=%s not change, ignored", value, last_value)
                    return
                await redis_client.hset("HS:DATA:{}".format(formula['result']), time_str, value)
                await redis_client.rpush("LST:DATA_TIME:{}".format(formula['result']), time_str)
                await redis_client.publish("CHANNEL:DEVICE_DATA:{}".format(formula['result']), json.dumps({
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
            for param, param_value in check_dict.items():
                if param.startswith('p'):
                    interp.symtable[param] = ts
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
