from collections import namedtuple
try:
    import ujson as json
except ImportError:
    import json
from pydatacoll.plugins import BaseModule
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('FormulaCalc')


class FormulaCalc(BaseModule):
    not_implemented = True
    formula_dict = dict()  # HS:TERM_ITEM:{term_id}:{item_id} -> value of HS:TERM_ITEM
    param_dict = dict()  # HS:DATA:{formula_id}:{term_id}:{item_id} -> pandas.Series

    async def start(self):
        try:
            with (await self.redis_pool) as redis_client:
                formula_dict = await redis_client.smembers('SET:FORMULA')
                for formula_id in formula_dict:
                    formula_dict = await redis_client.hgetall('HS:FORMULA:{}'.format(formula_id))
                    if formula_dict:
                        await self.add_formula(None, formula_dict)
        except Exception as ee:
            logger.error('init_formulas failed: %s', repr(ee), exc_info=True)

    async def stop(self):
        await self.del_formula()

    @param_function(channel='CHANNEL:FORMULA_ADD')
    async def add_formula(self, _, formula_dict):
        await self.fresh_formula(_, formula_dict)

    @param_function(channel='CHANNEL:FORMULA_FRESH')
    async def fresh_formula(self, _, formula_dict):
        try:
            formula_id = str(formula_dict['id'])
            formula = self.formula_dict.get(formula_id)
            if formula is not None:
                if str(formula.info['id']) != str(formula_dict['id']) or \
                                formula.info['protocol'] != formula_dict['protocol'] or \
                                formula.info['ip'] != formula_dict['ip'] or \
                                str(formula.info['port']) != str(formula_dict['port']):
                    await self.formula_dict.pop(formula_id).disconnect()
                else:
                    return
            protocol = formula_dict['protocol']
            protocol_class = self.protocol_list.get(protocol)
            if protocol_class is None:
                importlib.invalidate_caches()
                module = importlib.import_module('pydatacoll.protocols.{}.formula'.format(protocol))
                protocol_class = self.protocol_list[protocol] = getattr(module, '{}Device'.format(protocol.upper()))
                logger.info('fresh_formula new protocol %s registered', protocol_class.__name__)
            self.formula_dict[formula_id] = protocol_class(formula_dict, self.io_loop, self.redis_pool)
        except Exception as ee:
            logger.error('fresh_formula failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:FORMULA_DEL')
    async def del_formula(self, _, formula_id=None):
        try:
            if formula_id is None:
                [formula.disconnect() for formula in self.formula_dict.values()]
                self.formula_dict.clear()
                return

            if formula_id in self.formula_dict:
                formula = self.formula_dict.pop(formula_id)
                formula.disconnect()
        except Exception as ee:
            logger.error('del_formula failed: %s', repr(ee), exc_info=True)

    @param_function(channel='CHANNEL:DEVICE_DATA:*')
    async def param_update(self, channel, data_dict):
        try:
            logger.debug('param_update: got msg, channel=%s, dat_dict=%s', channel, data_dict)
            param = namedtuple('Param', data_dict.keys())(**data_dict)
            with (await self.redis_pool) as redis_client:
                formula_list = await redis_client.smembers('SET:FORMULA_PARAM:{}:{}:{}'.format(
                        param.device_id, param.term_id, param.item_id))
                [self.calc(formula_id, data_dict) for formula_id in formula_list or ()]
        except Exception as ee:
            logger.error('param_update failed: %s', repr(ee), exc_info=True)

    def calc(self, formula_id, data_dict):
        pass
