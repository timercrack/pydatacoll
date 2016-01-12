import asyncio
from collections import deque
import aioredis

from pydatacoll.protocols import BaseDevice
import pydatacoll.utils.logger as my_logger
from .frame import *

logger = my_logger.get_logger('IEC104Device')


class IEC104Device(BaseDevice):
    def __init__(self, device_info: dict, io_loop: asyncio.AbstractEventLoop,
                 redis_pool: aioredis.RedisPool):
        super(IEC104Device, self).__init__(device_info, io_loop, redis_pool)
        self.coll_interval = datetime.timedelta(seconds=config.getint('IEC104', 'coll_interval', fallback=900))
        self.coll_count = 0
        self.ssn = 0
        self.rsn = 0
        self.k = 0
        self.w = 0
        self.send_list = deque()
        self.last_call_all_time_begin = datetime.datetime.now()
        self.last_call_all_time_end = None
        self.connect_retry_count = 0
        self.user_canceled = False
        self.reader = None
        self.writer = None
        self.reconnect_handler = self.io_loop.call_soon(lambda: self.io_loop.create_task(self.reconnect()))
        self.connecting_task = None
        self.task_handler = None
        self.receive_handler = None
        self.start_act_handler = None
        self.test_act_handler = None
        self.k_decreased = asyncio.futures.Future(loop=self.io_loop)
        self.time_synced = asyncio.futures.Future(loop=self.io_loop)
        self.all_data_called = asyncio.futures.Future(loop=self.io_loop)
        self.power_data_called = asyncio.futures.Future(loop=self.io_loop)
        self.log_frame = config.getboolean('IEC104', 'log_frame', fallback=True)
        print('log_frame=', self.log_frame)

    async def reconnect(self):
        try:
            if self.reconnect_handler:
                self.reconnect_handler = None
            self.user_canceled = False
            self.connecting_task = self.io_loop.create_task(asyncio.wait_for(
                    asyncio.open_connection(self.device_info['ip'], self.device_info['port'], loop=self.io_loop),
                    timeout=IECParam.T0))
            self.reader, self.writer = await self.connecting_task
            self.connecting_task = None
            self.change_device_status(on_line=True)
            self.receive_handler = self.io_loop.create_task(self.receive())
            await self.send_frame(iec_104.init_frame(UFrame.STARTDT_ACT))
        except asyncio.TimeoutError:
            logger.debug('device[%s] connect timeout, try reconnect..', self.device_id)
            self.disconnect(reconnect=True)
        except Exception as e:
            logger.debug("device[%s] connect to %s:%s failed: %s, connect_retry_count=%s",
                         self.device_id, self.device_info['ip'], self.device_info['port'], repr(e),
                         self.connect_retry_count)
            self.disconnect(reconnect=True)

    def disconnect(self, reconnect=False):
        if not reconnect:
            self.user_canceled = True
            if self.reconnect_handler:
                self.reconnect_handler.cancel()
        elif self.reconnect_handler is None:
            self.reconnect_handler = self.io_loop.call_later(3, lambda: self.io_loop.create_task(self.reconnect()))
            self.connect_retry_count += 1
        if self.connecting_task:
            self.connecting_task.cancel()
        self.stop_timer(IECParam.T1)
        self.stop_timer(IECParam.T2)
        self.stop_timer(IECParam.T3)
        self.writer and self.writer.close()
        self.receive_handler and self.receive_handler.cancel()
        if self.connected:
            self.change_device_status(on_line=False)
        self.ssn = 0
        self.rsn = 0
        self.k = 0
        self.w = 0
        self.send_list.clear()
        self.start_act_handler = None
        self.test_act_handler = None

    def inc_ssn(self):
        self.ssn = self.ssn + 1 if self.ssn < 32767 else 0
        return self.ssn

    def inc_rsn(self):
        self.rsn = self.rsn + 1 if self.rsn < 32767 else 0
        return self.rsn

    def start_timer(self, timer_id):
        self.stop_timer(timer_id)
        setattr(self, "{}".format(timer_id.name.lower()),
                self.io_loop.call_later(timer_id, getattr(self, "on_timer{}".format(timer_id.name[-1]))))

    def stop_timer(self, timer_id):
        if hasattr(self, "{}".format(timer_id.name.lower())):
            timeout_handler = getattr(self, "{}".format(timer_id.name.lower()))
            if timeout_handler:
                timeout_handler.cancel()
                setattr(self, "{}".format(timer_id.name.lower()), None)

    # def on_timer0(self):
    #     logger.debug('device[%s] T0 timeout', self.device_id)
    #     if self.reconnect_handler is None:
    #         self.reconnect_handler = self.io_loop.call_soon(lambda: self.io_loop.create_task(self.reconnect()))

    def on_timer1(self):
        logger.error('device[%s] T1 timeout, send_list=%s', self.device_id,
                     [frm.APCI1 if frm.APCI1 == 'S' or isinstance(frm.APCI1, UFrame) else
                      frm.ASDU.TYP for frm in self.send_list])
        # if self.reconnect_handler is None:
        #     self.reconnect_handler = self.io_loop.call_soon(lambda: self.io_loop.create_task(self.reconnect()))

    def on_timer2(self):
        logger.debug('device[%s] T2 timeout, send S_frame(rsn=%s)', self.device_id, self.rsn)
        self.io_loop.create_task(self.send_frame(iec_104.init_frame("S", self.rsn)))

    def on_timer3(self):
        logger.debug('device[%s] T3 timeout, send heartbeat', self.device_id)
        self.test_act_handler = self.io_loop.create_task(self.send_frame(iec_104.init_frame(UFrame.TESTFR_ACT)))

    async def receive(self):
        try:
            self.receive_handler = None
            data = await self.reader.readexactly(2)
            head = iec_head.parse(data)
            data += await self.reader.readexactly(head.length)
            recv_time = datetime.datetime.now()
            self.start_timer(IECParam.T3)
            self.receive_handler = self.io_loop.create_task(self.receive())
            logger.debug("device[%s] recv: %s", self.device_id, data.hex())
            frame = iec_104.parse(data)
            if self.log_frame:
                self.io_loop.create_task(self.save_frame(data, send=False, save_time=recv_time))
            if isinstance(frame.APCI1, UFrame):
                await self.handle_u(frame)
            else:
                logger.debug("device[%s] self.ssn,frame.rsn=%s, self.rsn, frame.ssn=%s, k,w=%s",
                             self.device_id, (self.ssn, frame.APCI2), (self.rsn, frame.APCI1), (self.k, self.w))
                # S or I, check rsn, ssn first
                bad_frame = False
                if frame.APCI1 == 'S':  # S Frame
                    if self.ssn < frame.APCI2 and frame.APCI2 - self.ssn < 20000:
                        bad_frame = True
                    else:
                        self.k = self.ssn - frame.APCI2 if self.ssn >= frame.APCI2 else 32768 + self.ssn - frame.APCI2
                        if not self.k_decreased.done():
                            self.k_decreased.set_result(None)
                else:  # I Frame
                    if self.ssn < frame.APCI2 and frame.APCI2 - self.ssn < 20000:
                        bad_frame = True
                    else:
                        self.k = self.ssn - frame.APCI2 if self.ssn >= frame.APCI2 else 32768 + self.ssn - frame.APCI2
                        if not self.k_decreased.done():
                            self.k_decreased.set_result(None)
                        if self.rsn != frame.APCI1:
                            bad_frame = True
                        else:
                            self.inc_rsn()
                            self.w += 1
                if bad_frame:
                    logger.error("device[%s] I_frame mismatch! self.ssn,frame.rsn=%s, self.rsn, "
                                 "frame.ssn=%s, k,w=%s", self.device_id, (self.ssn, frame.APCI2),
                                 (self.rsn, frame.APCI1), (self.k, self.w))
                    if self.reconnect_handler is None:
                        logger.info('device[%s] try reconnect..', self.device_id)
                        self.disconnect(reconnect=True)
                elif frame.APCI1 != 'S':
                    await self.handle_i(frame)
        except asyncio.IncompleteReadError:
            if self.user_canceled:
                logger.info("device[%s] closed manually.", self.device_id)
            elif self.reconnect_handler:
                logger.info("device[%s] closed manually, try reconnect..", self.device_id)
                # self.disconnect(reconnect=True)
            else:
                logger.warn("device[%s] closed by server, try reconnect..", self.device_id)
                self.disconnect(reconnect=True)
        except ConnectionResetError:
            logger.warn("device[%s] remote server shutdown, try reconnect..", self.device_id)
            self.disconnect(reconnect=True)
        except Exception as e:
            logger.error("device[%s] receive failed: %s, try reconnect..", self.device_id, repr(e), exc_info=True)
            self.disconnect(reconnect=True)

    async def handle_u(self, frame):
        try:
            logger.debug("device[%s] got U_FRAME: %s", self.device_id, frame.APCI1.name)
            if frame.APCI1 == UFrame.STARTDT_ACT:
                # 对方也发送了STARTDT, 删除之前自己发送的STARTDT
                if self.send_list and self.send_list[0].APCI1 == UFrame.STARTDT_ACT:
                    logger.info('device[%s] remote side send STARTDT_ACT too, ignored mine', self.device_id)
                    self.send_list.popleft()
                    self.stop_timer(IECParam.T1)
                elif self.start_act_handler:
                    self.stop_timer(IECParam.T1)
                    self.start_act_handler.cancel()
                    self.start_act_handler = None
                await self.send_frame(iec_104.init_frame(UFrame.STARTDT_CON))
                self.task_handler = self.io_loop.call_later(
                        self.coll_interval.total_seconds(), lambda: self.io_loop.create_task(self.run_task()))
                logger.info('device[%s] call task will begin at %s', self.device_id,
                            (datetime.datetime.now()+self.coll_interval).isoformat())
                self.io_loop.create_task(self.check_to_send(frame))
            elif frame.APCI1 == UFrame.STARTDT_CON:
                self.task_handler = self.io_loop.call_later(
                        self.coll_interval.total_seconds(), lambda: self.io_loop.create_task(self.run_task()))
                logger.info('device[%s] call task will begin at %s', self.device_id,
                            (datetime.datetime.now()+self.coll_interval).isoformat())
                self.io_loop.create_task(self.check_to_send(frame))
            elif frame.APCI1 == UFrame.TESTFR_ACT:
                # 对方也发送了TESTFR_ACT, 删除之前自己发送的TESTFR_ACT
                if self.send_list and self.send_list[0].APCI1 == UFrame.TESTFR_ACT:
                    logger.debug('device[%s] remote side send TESTFR_ACT too, ignored mine', self.device_id)
                    self.send_list.popleft()
                    self.stop_timer(IECParam.T1)
                elif self.test_act_handler:
                    self.stop_timer(IECParam.T1)
                    self.test_act_handler.cancel()
                    self.test_act_handler = None
                await self.send_frame(iec_104.init_frame(UFrame.TESTFR_CON))
            elif frame.APCI1 == UFrame.TESTFR_CON:
                self.io_loop.create_task(self.check_to_send(frame))
            elif frame.APCI1 == UFrame.STOPDT_ACT:
                await self.send_frame(iec_104.init_frame(UFrame.STOPDT_CON))
                logger.debug("device[%s] receive STOPDT_ACT.", self.device_id)
                self.disconnect()
            elif frame.APCI1 == UFrame.STOPDT_CON:
                self.stop_timer(IECParam.T1)
                logger.debug("device[%s] receive STOPDT_CON.", self.device_id)
                self.disconnect()
        except Exception as e:
            logger.error("device[%s] handle_u failed: %s", self.device_id, repr(e), exc_info=True)
            self.disconnect(reconnect=True)

    async def handle_i(self, frame):
        try:
            self.start_timer(IECParam.T2)
            logger.debug("device[%s] got I_Frame-->TYP,Cause=%s,sq_count=%s", self.device_id,
                         (frame.ASDU.TYP.name, frame.ASDU.Cause.name), frame.ASDU.sq_count)
            if self.w >= IECParam.W:
                logger.debug("self.w,Param_S=%s, send S_frame", (self.w, IECParam.W.value))
                await self.send_frame(iec_104.init_frame("S", self.rsn))
            if frame.ASDU.Cause in (Cause.actcon, Cause.req):
                self.stop_timer(IECParam.T1)
                self.io_loop.create_task(self.check_to_send(frame))
            if frame.ASDU.Cause in (Cause.spont, Cause.introgen, Cause.reqcogen) or \
                    (frame.ASDU.Cause == Cause.req and TYP.M_SP_NA_1 <= frame.ASDU.TYP <= TYP.M_EP_TD_1) or \
                    (frame.ASDU.Cause == Cause.actcon and TYP.C_SC_NA_1 <= frame.ASDU.TYP <= TYP.C_SE_TC_1 and
                     frame.ASDU.data[0].se == 0):
                idx = 0
                data_pairs = set()
                for data in frame.ASDU.data:
                    # 实现完整的品质描述词判断
                    if hasattr(data, "IV") and data.IV != 0:
                        continue
                    data_addr = data.address if frame.ASDU.sq == 0 else frame.ASDU.StartAddress + idx
                    idx += 1
                    data_time = data.cp56time2a if hasattr(data, "cp56time2a") else data.cp24time2a \
                        if hasattr(data, "cp24time2a") else datetime.datetime.now()
                    data_pairs.add((data_time, data_addr, data.value))
                method = 'call' if frame.ASDU.Cause == Cause.req else \
                    'ctrl' if frame.ASDU.Cause == Cause.actcon else 'data'
                logger.debug('device[%s] method=%s, data_pairs=%s', self.device_id, method, data_pairs)
                await self.process_data(data_pairs, method)
            elif frame.ASDU.Cause == Cause.actcon:
                if TYP.C_SC_NA_1 <= frame.ASDU.TYP <= TYP.C_SE_TC_1 and frame.ASDU.data[0].se == 1:
                    send_data = frame
                    send_data.ASDU.Cause = Cause.act
                    send_data.ASDU.data[0].se = 0  # 执行
                    await self.send_frame(send_data)
                elif frame.ASDU.TYP == TYP.C_CS_NA_1:
                    if not self.time_synced.done():
                        self.time_synced.set_result(None)
            elif frame.ASDU.Cause == Cause.actterm:
                if frame.ASDU.TYP == TYP.C_IC_NA_1:  # 总召唤命令
                    if not self.all_data_called.done():
                        self.all_data_called.set_result(None)
                if frame.ASDU.TYP == TYP.C_CI_NA_1:  # 电能脉冲召唤命令
                    if not self.power_data_called.done():
                        self.power_data_called.set_result(None)
            elif frame.ASDU.Cause == Cause.act:
                logger.warning('device[%s] handle_i: act frame not allowed!', self.device_id)
            # 完成尚未实现的I帧
            else:
                logger.error("device[%s] unknown I_frame: %s", self.device_id, frame)

        except Exception as e:
            logger.error("device[%s] handle_i failed: %s", self.device_id, repr(e), exc_info=True)
            self.disconnect(reconnect=True)

    # 优化发送逻辑
    async def send_frame(self, frame, check=True):
        if frame is None:
            return
        stream_write = False
        encode_frame = None
        try:
            # send S
            if frame.APCI1 == "S":
                self.stop_timer(IECParam.T2)
                frame.APCI2 = self.rsn
                encode_frame = iec_104.build_isu(frame)
                self.writer.write(encode_frame)
                await self.writer.drain()
                self.w = 0
                stream_write = True
            # send U
            elif isinstance(frame.APCI1, UFrame):
                if not check or not self.send_list:
                    encode_frame = iec_104.build_isu(frame)
                    self.writer.write(encode_frame)
                    await self.writer.drain()
                    stream_write = True
                    if frame.APCI1 == UFrame.STARTDT_ACT:
                        self.start_timer(IECParam.T1)
                        self.start_act_handler = None
                    elif frame.APCI1 == UFrame.TESTFR_ACT:
                        self.start_timer(IECParam.T1)
                        self.test_act_handler = None
                if check and frame.APCI1 in (
                        UFrame.STARTDT_ACT, UFrame.TESTFR_ACT):
                    self.send_list.append(frame)
            # send I
            else:
                if check is False or not self.send_list or frame.ASDU.Cause not in (Cause.act,):
                    self.stop_timer(IECParam.T2)
                    frame.APCI1 = self.ssn
                    frame.APCI2 = self.rsn
                    encode_frame = iec_104.build_isu(frame)
                    while self.k >= IECParam.K:
                        logger.debug('device[%s] self.k,ParamK=%s, wait S..', self.device_id, (self.k, IECParam.K))
                        if self.k_decreased.done():
                            self.k_decreased = asyncio.futures.Future(loop=self.io_loop)
                        await self.k_decreased
                    self.writer.write(encode_frame)
                    await self.writer.drain()
                    self.inc_ssn()
                    self.k += 1
                    self.w = 0
                    stream_write = True
                    if frame.ASDU.Cause in (Cause.act,):
                        self.start_timer(IECParam.T1)
                if check and frame.ASDU.Cause in (Cause.act,):
                    self.send_list.append(frame)
            if stream_write:
                logger.debug("device[%s] send_frame(%s): %s", self.device_id,
                             frame.APCI1 if frame.APCI1 == "S" or isinstance(frame.APCI1, UFrame) else
                             frame.ASDU.TYP, encode_frame.hex())
                if self.log_frame:
                    self.io_loop.create_task(
                            self.save_frame(encode_frame, send=True, save_time=datetime.datetime.now()))
            logger.debug("device[%s] after send_frame: send_list=%s", self.device_id,
                         [frm.APCI1 if frm.APCI1 == 'S' or isinstance(frm.APCI1, UFrame) else
                          frm.ASDU.TYP for frm in self.send_list])
        except Exception as e:
            logger.error("device[%s] send_frame failed: %s", self.device_id, repr(e), exc_info=True)
            self.disconnect(reconnect=True)

    async def check_to_send(self, frame):
        try:
            self.stop_timer(IECParam.T1)
            if not self.send_list or frame is None:
                return
            if isinstance(frame.APCI1, UFrame) and frame.APCI1 in \
                    (UFrame.STARTDT_CON, UFrame.TESTFR_CON, UFrame.STOPDT_CON):
                pop_frame = self.send_list.popleft()
                logger.debug('device[%s] check_to_send: remove U_Frame %s', self.device_id, pop_frame.APCI1.name)
                await self.send_frame(self.send_list[0] if self.send_list else None, check=False)
            elif frame.ASDU.TYP == self.send_list[0].ASDU.TYP \
                    or frame.ASDU.Cause == Cause.req and self.send_list[0].ASDU.TYP == TYP.C_RD_NA_1:
                pop_frame = self.send_list.popleft()
                logger.debug('device[%s] check_to_send: remove I_Frame %s', self.device_id, pop_frame.ASDU.TYP.name)
                await self.send_frame(self.send_list[0] if self.send_list else None, check=False)
        except Exception as e:
            logger.error("device[%s] check_to_send failed: %s", self.device_id, repr(e), exc_info=True)
            self.disconnect(reconnect=True)

    async def run_task(self):
        try:
            logger.info('device[%s] run_task begin..', self.device_id)
            self.last_call_all_time_begin = datetime.datetime.now()
            if self.time_synced.done():
                self.time_synced = asyncio.futures.Future(loop=self.io_loop)
            if self.all_data_called.done():
                self.all_data_called = asyncio.futures.Future(loop=self.io_loop)
            if self.power_data_called.done():
                self.power_data_called = asyncio.futures.Future(loop=self.io_loop)
            await self.send_frame(iec_104.init_frame(self.ssn, self.rsn, TYP.C_CS_NA_1, Cause.act))
            await self.time_synced
            await self.send_frame(iec_104.init_frame(self.ssn, self.rsn, TYP.C_IC_NA_1, Cause.act))
            await self.all_data_called
            await self.send_frame(iec_104.init_frame(self.ssn, self.rsn, TYP.C_CI_NA_1, Cause.act))
            await self.power_data_called
            self.last_call_all_time_end = datetime.datetime.now()
            spent = self.last_call_all_time_end - self.last_call_all_time_begin
            self.coll_count += 1
            logger.info('device[%s] last task costs: %s seconds, rsn=%s, task_count=%s',
                        self.device_id, spent.total_seconds(), self.rsn, self.coll_count)
            if self.task_handler:
                self.task_handler.cancel()
            self.task_handler = self.io_loop.call_later(
                    self.coll_interval.seconds, lambda: self.io_loop.create_task(self.run_task()))
            logger.info('device[%s] run next task at %s', self.device_id,
                         (self.last_call_all_time_end+self.coll_interval).isoformat())
        except Exception as e:
            logger.error("device[%s] run_task failed: %s", self.device_id, repr(e), exc_info=True)

    def fresh_task(self, term_dict, term_item_dict, delete=False):
        pass

    def prepare_call_frame(self, term_item_dict):
        frame = iec_104.init_frame(self.ssn, self.rsn, TYP.C_RD_NA_1, Cause.act)  # 102 读命令
        frame.ASDU.data[0].address = int(term_item_dict['protocol_code'])
        return frame

    def prepare_ctrl_frame(self, term_item_dict, value):
        frame = iec_104.init_frame(self.ssn, self.rsn, TYP(int(term_item_dict['code_type'])), Cause.act)
        frame.ASDU.data[0].address = int(term_item_dict['protocol_code'])
        frame.ASDU.data[0].value = value
        frame.ASDU.data[0].se = 1
        return frame
