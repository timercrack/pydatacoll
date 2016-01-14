from setuptools import setup
import sys
if sys.version_info < (3, 5):
    print("PyDataColl needs python version >= 3.5, please upgrade!")
    sys.exit(1)

setup(
        name='pydatacoll',
        version='0.1',
        packages=['test', 'test.mock_device', 'pydatacoll', 'pydatacoll.utils', 'pydatacoll.utils.asteval',
                  'pydatacoll.plugins', 'pydatacoll.protocols', 'pydatacoll.protocols.iec104',
                  'pydatacoll.protocols.formula', 'pydatacoll.resources'],
        install_requires=[
            'aiohttp>=0.20.1',
            'aioredis>=0.2.4',
            'asynctest>=0.5.0',
            'cchardet>=1.0.0',
            'construct>=2.5.2',
            'hiredis>=0.2.0',
            'numpy>=1.10.4',
            'pandas>=0.17.1',
            'PyMySQL>=0.6.7',
            'python-dateutil>=2.4.2',
            'pytz>=2015.7',
            'redis>=2.10.5',
            'six>=1.10.0',
            'ujson>=1.34',
        ],
        url='https://github.com/timercrack/pydatacoll',
        license='Apache License 2.0',
        author='JeffChen',
        author_email='timercrack@gmail.com',
        description='Universal Data Acquisition System'
)
