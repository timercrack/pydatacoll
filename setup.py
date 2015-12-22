from distutils.core import setup

setup(
        name='pydatacoll',
        version='0.1',
        packages=['test', 'test.mock_device', 'pydatacoll', 'pydatacoll.utils', 'pydatacoll.plugins',
                  'pydatacoll.protocols', 'pydatacoll.protocols.iec104', 'pydatacoll.resources'],
        url='https://github.com/timercrack/pydatacoll',
        license='Apache License V2.0',
        author='JeffChen',
        author_email='timercrack@gmail.com',
        description='Universal Data Acquisition System'
)
