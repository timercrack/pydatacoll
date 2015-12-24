from pydatacoll.plugins import BaseModule
import pydatacoll.utils.logger as my_logger
from pydatacoll.utils.func_container import param_function
logger = my_logger.get_logger('DataChecker')


class DataChecker(BaseModule):
    not_implemented = True
    async def stop(self):
        pass

    async def start(self):
        pass

    @param_function(channel='CHANNEL:DEVICE_DATA:*')
    async def data_check(self, channel: str, data_dict: dict):
        pass
