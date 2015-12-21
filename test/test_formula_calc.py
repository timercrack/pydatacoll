import asyncio
import datetime
try:
    import ujson as json
except ImportError:
    import json
import functools
import aioredis
import asynctest
import redis
import pydatacoll.utils.logger as my_logger
import pydatacoll.plugins.formula_calc as formula_calc

logger = my_logger.get_logger('FormulaCalcTest')


class FormulaCalcTest(asynctest.TestCase):
    def setUp(self):
        super(FormulaCalcTest, self).setUp()
        self.redis_pool = asyncio.get_event_loop().run_until_complete(
                functools.partial(aioredis.create_pool, ('localhost', 6379), db=1, minsize=5, maxsize=10,
                                  encoding='utf-8')())
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        self.redis_client.flushdb()
        self.formula_calc = formula_calc.FormulaCalc(self.loop, self.redis_pool)
        self.loop.run_until_complete(self.formula_calc.install())

    def tearDown(self):
        self.loop.run_until_complete(self.formula_calc.uninstall())

    async def test_formula(self):
        pass
