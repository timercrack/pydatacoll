from abc import ABCMeta
from functools import wraps


def param_function(**out_kwargs):
    def _rest_handler(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, *kwargs)

        for key, value in out_kwargs.items():
            setattr(wrapper, 'arg_{}'.format(key), value)
        setattr(wrapper, 'is_module_function', True)
        return wrapper

    return _rest_handler


class ParamFunctionContainer(metaclass=ABCMeta):
    def __init__(self):
        self.module_arg_dict = dict()
        self._collect_all()

    def _collect_all(self):
        for fun_name in dir(self):
            fun = getattr(self, fun_name)
            if hasattr(fun, 'is_module_function'):
                params = dict()
                for arg in dir(fun):
                    if arg.startswith('arg_'):
                        params[arg[4:]] = getattr(fun, arg)
                self.module_arg_dict[fun_name] = params
