import unittest
import pydatacoll.utils.logger as my_logger
from pydatacoll.utils.func_container import ParamFunctionContainer, param_function

logger = my_logger.get_logger('UtilTest')


class UtilTest(unittest.TestCase):
    def test_func_container(self):

        class MyAPI(ParamFunctionContainer):
            def __init__(self, a, b, c):
                super().__init__()
                self.a = a
                self.b = b
                self.c = c

            @param_function(method='GET', url='/devices')
            def api_device_list(self, request):
                logger.debug('api_device_list req=%s', request)

            @param_function(method='POST', url='/devices_new')
            def api_new_device(self, request):
                logger.debug('api_new_device req=%s', request)

        api = MyAPI(1, 2, 3)
        self.assertDictEqual(api.module_arg_dict, {'api_device_list': {'method': 'GET', 'url': '/devices'},
                                                   'api_new_device': {'method': 'POST', 'url': '/devices_new'}})
