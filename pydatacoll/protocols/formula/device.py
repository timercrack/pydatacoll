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

import asyncio

from pydatacoll.protocols import BaseDevice
import pydatacoll.utils.logger as my_logger

logger = my_logger.get_logger('FORMULADevice')


class FORMULADevice(BaseDevice):
    def __init__(self, device_info: dict, io_loop: asyncio.AbstractEventLoop):
        super(FORMULADevice, self).__init__(device_info, io_loop)

    def disconnect(self, reconnect=False):
        pass

    def send_frame(self, frame, check=True):
        pass

    def prepare_ctrl_frame(self, term_item_dict, value):
        pass

    def prepare_call_frame(self, term_item_dict):
        pass

    def fresh_task(self, term_dict, term_item_dict, delete=False):
        pass
