import gc
import asyncio
import asynctest
import aiohttp
import pymysql
import redis
from multiprocessing import Process

try:
    import ujson as json
except ImportError:
    import json
import pydatacoll.utils.logger as my_logger
from pydatacoll import api_server
from pydatacoll.plugins import db_save
from pydatacoll.plugins.device_manage import DeviceManager
from test.mock_device.iec104device import run_server

logger = my_logger.get_logger('LoadTest')


class LoadTest(asynctest.TestCase):
    loop = asyncio.get_event_loop()  # make pycharm happy

    def setUp(self):
        logger.info('prepare data..')
        self.redis_client = redis.StrictRedis(db=1, decode_responses=True)
        self.redis_client.flushdb()
        self.conn = pymysql.Connect(**db_save.PLUGIN_PARAM)
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS test_data_check")
        self.cursor.execute("""
CREATE TABLE test_data_check(
  id INT AUTO_INCREMENT PRIMARY KEY,
  device_id VARCHAR(32),
  term_id VARCHAR(32),
  item_id VARCHAR(32),
  time DATETIME,
  value FLOAT,
  warn_msg VARCHAR(1000)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
""")
        self.cursor.execute("DROP TABLE IF EXISTS test_db_save")
        self.cursor.execute("""
CREATE TABLE test_db_save(
  id INT AUTO_INCREMENT PRIMARY KEY,
  device_id VARCHAR(32),
  term_id VARCHAR(32),
  item_id VARCHAR(32),
  time DATETIME,
  value FLOAT
) ENGINE=MyISAM DEFAULT CHARSET=utf8
""")
        self.conn.commit()
        self.api_server = api_server.APIServer(io_loop=self.loop, port=8080)
        self.mock_server = Process(target=run_server)
        logger.info('begin load test..')

    def tearDown(self):
        self.mock_server.terminate()
        self.api_server.stop_server()
        self.conn.close()

    async def test_heavy_load(self):
        device_count = 1
        term_count = 100
        item_count = 100
        device_list = []
        term_list = []
        item_list = []
        term_item_list = []
        formula_list = []
        for item_id in range(item_count):
            item_list.append({'id': item_id, 'name': '测试指标{}'.format(item_id)})
        for device_id in range(device_count):
            device_list.append({'id': device_id, 'name': '测试集中器{}'.format(device_id), 'ip': '127.0.0.1',
                                'port': device_id + 2404, 'protocol': 'iec104'})
            device_list.append({'id': 'F{}'.format(device_id), 'name': '测试公式设备F{}'.format(device_id),
                                'protocol': 'formula'})
            for term_id in range(term_count):
                term_list.append({'id': term_id, 'name': '测试终端{}'.format(term_id), 'device_id': device_id})
                term_list.append({'id': 'F{}'.format(term_id), 'name': '测试公式终端{}'.format(term_id),
                                  'device_id': 'F{}'.format(device_id)})
                for item_id in range(item_count):
                    term_item_list.append({
                        'device_id': device_id, 'term_id': term_id, 'item_id': item_id,
                        'protocol_code': term_id * 100 + item_id, 'code_type': 36, 'base_val': 0, 'coefficient': 1,
                        'down_limit': 100, 'up_limit': 1000, 'protocol': 'iec104',
                        'db_save_sql':
                            "INSERT INTO test_db_save(device_id,term_id,item_id,time,value) VALUES"
                            "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value})",
                        'db_warn_sql':
                            "INSERT INTO test_data_check(device_id,term_id,item_id,time,value,warn_msg) VALUES"
                            "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value},"
                            "'{PARAM.warn_msg}')",
                        'warn_msg': 'value error!',
                        'do_verify': 'param.down_limit <= value <= str(900)',
                    })
                    term_item_list.append({
                        'device_id': 'F{}'.format(device_id), 'term_id': 'F{}'.format(term_id),
                        'item_id': item_id, 'down_limit': 100, 'up_limit': 1000,
                        'db_save_sql':
                            "INSERT INTO test_db_save(device_id,term_id,item_id,time,value) VALUES"
                            "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value})",
                        'db_warn_sql':
                            "INSERT INTO test_data_check(device_id,term_id,item_id,time,value,warn_msg) VALUES"
                            "('{PARAM.device_id}','{PARAM.term_id}','{PARAM.item_id}','{PARAM.time}',{PARAM.value},"
                            "'{PARAM.warn_msg}')",
                        'warn_msg': 'value error!',
                        'do_verify': 'param.down_limit <= value <= str(900)',
                    })
                    formula_list.append({'id': 'D{}T{}i{}'.format(device_id, term_id, item_id),
                                         'formula': 'p1.max()', 'p1': '{}:{}:{}'.format(device_id, term_id, item_id),
                                         'device_id': 'F{}'.format(device_id), 'term_id': 'F{}'.format(term_id),
                                         'item_id': '{}'.format(item_id)})
        logger.info('create device..')
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/devices', data=json.dumps(device_list)) as r:
            self.assertEqual(r.status, 200)
        logger.info('create term..')
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/terms', data=json.dumps(term_list)) as r:
            self.assertEqual(r.status, 200)
        logger.info('create item..')
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/items', data=json.dumps(item_list)) as r:
            self.assertEqual(r.status, 200)
        logger.info('create formula..')
        async with aiohttp.post('http://127.0.0.1:8080/api/v1/formulas', data=json.dumps(formula_list)) as r:
            self.assertEqual(r.status, 200)
        logger.info('create term_item..')
        async with aiohttp.post('http://127.0.0.1:8080/api/v2/term_items', data=json.dumps(term_item_list)) as r:
            self.assertEqual(r.status, 200)
        logger.info('done.')
        self.mock_server.start()
        device = DeviceManager.device_dict['0']
        await device.data_link_established
        await device.run_task()
        await asyncio.sleep(3, loop=self.loop)
        self.assertEqual(device.coll_count, 1)
        gc.collect()
        await device.run_task()
        await asyncio.sleep(3, loop=self.loop)
        self.assertEqual(device.coll_count, 2)
        gc.collect()
        self.assertEqual(len(gc.garbage), 0)
        self.cursor.execute("SELECT COUNT(*) FROM test_db_save")
        rst = self.cursor.fetchall()
        self.assertGreater(rst[0][0], device_count*term_count*item_count*2)
