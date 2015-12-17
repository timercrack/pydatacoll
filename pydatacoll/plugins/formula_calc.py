try:
    import ujson as json
except ImportError:
    import json
from pydatacoll.plugins import BaseModule
from pydatacoll.utils.func_container import param_function
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('FormulaCalc')


class FormulaCalc(BaseModule):
    mysql_pool = None

    async def start(self):
        pass

    async def stop(self):
        pass
