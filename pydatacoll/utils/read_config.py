#!/usr/bin/env python
#
# Copyright 2016 timercrack
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from appdirs import AppDirs
import os
import shutil
import configparser
from pydatacoll import version as app_ver
from pydatacoll import resources

config_example = """# PyDataColl configuration file
[SERVER]
# http(s) listening port
web_port = 8080
# http(s) request timeout in seconds
web_timeout = 10
# installed plugins
# plugins = device_manage, db_save, formula_calc
plugins = device_manage, db_save, formula_calc
# whether launch each plugin in a separate sub-process, It's recommend to
# set to False in production environment to fully leverage multiple processors
single_process = True

[DBSaver]
# whether save unchanged data
save_unchanged = False

[FormulaCalc]
# whether calculate unchanged parameter
calc_unchanged = False

[IEC104]
# data collection interval in seconds, default is 900(15 min)
coll_interval = 900
# log send/recv frame to redis for debug
log_frame = True
# protocol parameters, don't modify it unless you know what it means
T0 = 30
T1 = 15
T2 = 10
T3 = 20
K = 4096
W = 4000

[REDIS]
host = 127.0.0.1
port = 6379
db = 1
encoding = utf-8

[MYSQL]
host = 127.0.0.1
port = 3306
db = test
user = pydatacoll
password = pydatacoll

[LOG]
level = INFO
format = %(asctime)s %(name)s [%(levelname)s] %(message)s
"""

app_dir = AppDirs('PyDataColl', False, version=app_ver)
config_file = os.path.join(app_dir.user_config_dir, 'config.ini')
if not os.path.exists(config_file):
    if not os.path.exists(app_dir.user_config_dir):
        os.makedirs(app_dir.user_config_dir)
    with open(config_file, 'wt') as f:
        f.write(config_example)
    print('create config file:', config_file)

config = configparser.ConfigParser(interpolation=None)
config.read(config_file)
