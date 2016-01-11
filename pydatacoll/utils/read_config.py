from appdirs import AppDirs
import os
import shutil
import configparser
from pydatacoll import __version__ as app_ver
from pydatacoll import resources

app_dir = AppDirs('PyDataColl', False, version=app_ver)
config_file = os.path.join(app_dir.user_config_dir, 'config.ini')
if not os.path.exists(config_file):
    if not os.path.exists(app_dir.user_config_dir):
        os.makedirs(app_dir.user_config_dir)
    shutil.copyfile(os.path.join(resources.__path__[0], 'config_example.ini'), config_file)
    print('create config file:', config_file)

config = configparser.ConfigParser(interpolation=None)
config.read(config_file)
