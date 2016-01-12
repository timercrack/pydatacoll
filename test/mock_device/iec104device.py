import random
import functools
import asyncio
from collections import defaultdict
from collections import deque
import redis

import pydatacoll.utils.logger as my_logger
from pydatacoll.protocols.iec104.frame import *
from pydatacoll.utils import str_to_number
from pydatacoll.utils.read_config import *

logger = my_logger.get_logger('MockIEC104')


# 模拟104从站
class IEC104Device:
    frame_list = defaultdict(list)

    def __init__(self, device, reader, writer):
        self.io_loop = asyncio.get_event_loop()
        self.device_id = device['id']
        self.redis = redis.StrictRedis(db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.coll_interval = datetime.timedelta(minutes=config.getint('IEC104', 'coll_interval', fallback=15))
        self.coll_count = 0
        self.ssn = 0
        self.rsn = 0
        self.k = 0
        self.w = 0
        self.send_list = deque()
        self.last_call_all_time_begin = None
        self.last_call_all_time_end = None
        self.connect_retry_count = 0
        self.user_canceled = False
        self.reader = reader
        self.writer = writer
        self.connecting_task = None
        self.task_handler = None
        self.receive_handler = None
        self.start_act_handler = None
        self.test_act_handler = None
        self.term_item_list = []
        self.log_frame = config.getboolean('IEC104', 'log_frame', fallback=True)
        print('log_frame=', self.log_frame)
        self.receive_handler = self.io_loop.create_task(self.receive())
        self.k_decreased = asyncio.futures.Future(loop=self.io_loop)
        logger.debug('mock device server start, device=%s', device)

    async def receive(self):
        try:
            self.receive_handler = None
            data = await self.reader.readexactly(2)
            head = iec_head.parse(data)
            data += await self.reader.readexactly(head.length)
            self.start_timer(IECParam.T3)
            self.receive_handler = self.io_loop.create_task(self.receive())
            logger.debug("device[%s] recv: %s", self.device_id, data.hex())
            frame = iec_104.parse(data)
            if self.log_frame:
                self.save_frame(frame, False)
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
                    logger.info('device[%s] try reconnect..', self.device_id)
                    self.connection_lost()
                elif frame.APCI1 != 'S':
                    await self.handle_i(frame)
        except asyncio.IncompleteReadError:
            if self.user_canceled:
                logger.info("device[%s] closed manually.", self.device_id)
            else:
                logger.warning("device[%s] closed by server", self.device_id)
                self.connection_lost()
        except ConnectionResetError:
            logger.warning("device[%s] remote server shutdown", self.device_id)
            self.connection_lost()
        except Exception as e:
            logger.error("device[%s] receive failed: %s", self.device_id, repr(e), exc_info=True)
            self.connection_lost()

    def connection_lost(self):
        self.receive_handler and self.receive_handler.cancel()
        self.stop_timer(IECParam.T0)
        self.stop_timer(IECParam.T1)
        self.stop_timer(IECParam.T2)
        self.stop_timer(IECParam.T3)
        self.frame_list[self.device_id].clear()
        self.term_item_list.clear()

    def inc_ssn(self):
        self.ssn = self.ssn + 1 if self.ssn < 32767 else 0
        return self.ssn

    def inc_rsn(self):
        self.rsn = self.rsn + 1 if self.rsn < 32767 else 0
        return self.rsn

    def start_timer(self, timer_id):
        self.stop_timer(timer_id)
        setattr(self, "{}".format(timer_id.name.lower()), self.io_loop.call_later(
            timer_id, getattr(self, "on_timer{}".format(timer_id.name[-1]))))

    def stop_timer(self, timer_id):
        if hasattr(self, "{}".format(timer_id.name.lower())):
            timeout_handler = getattr(self, "{}".format(timer_id.name.lower()))
            if timeout_handler:
                timeout_handler.cancel()
                setattr(self, "{}".format(timer_id.name.lower()), None)

    def on_timer0(self):
        logger.debug('device[%s] T0 timeout', self.device_id)

    def on_timer1(self):
        logger.debug('device[%s] T1 timeout', self.device_id)

    def on_timer2(self):
        logger.debug('device[%s] T2 timeout, send S_frame(rsn=%s)', self.device_id, self.rsn)
        self.io_loop.create_task(self.send_frame(iec_104.init_frame("S", self.rsn)))

    def on_timer3(self):
        logger.debug('device[%s] T3 timeout, send heartbeat', self.device_id)
        self.test_act_handler = self.io_loop.create_task(self.send_frame(iec_104.init_frame(UFrame.TESTFR_ACT)))

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
                self.io_loop.create_task(self.check_to_send(frame))
            elif frame.APCI1 == UFrame.STARTDT_CON:
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
            elif frame.APCI1 == UFrame.STOPDT_CON:
                self.stop_timer(IECParam.T1)
                logger.debug("device[%s] receive STOPDT_CON.", self.device_id)
        except Exception as e:
            logger.error("device[%s] handle_u failed: %s", self.device_id, repr(e), exc_info=True)

    async def handle_i(self, frame):
        try:
            self.start_timer(IECParam.T2)
            logger.debug("device[%s] got I_Frame-->TYP,Cause=%s,sq_count=%s", self.device_id,
                         (frame.ASDU.TYP.name, frame.ASDU.Cause.name), frame.ASDU.sq_count)
            # 控制方向的过程信息
            if TYP.C_SC_NA_1 <= frame.ASDU.TYP <= TYP.C_SE_TC_1 or TYP.C_CS_NA_1 == frame.ASDU.TYP:
                send_data = frame
                send_data.ASDU.Cause = Cause.actcon
                await self.send_frame(send_data)
            # 总召唤命令
            elif frame.ASDU.TYP == TYP.C_IC_NA_1:
                send_data = frame
                send_data.ASDU.Cause = Cause.actcon
                await self.send_frame(send_data)
                logger.debug('device[%s] send task data begin, ssn=%s', self.device_id, self.ssn)
                await self.generate_call_all_data()
                send_data.ASDU.Cause = Cause.actterm
                await self.send_frame(send_data)
            # 电能脉冲召唤命令
            elif frame.ASDU.TYP == TYP.C_CI_NA_1:
                send_data = frame
                send_data.ASDU.Cause = Cause.actcon
                await self.send_frame(send_data)
                self.generate_call_power_data()
                send_data.ASDU.Cause = Cause.actterm
                await self.send_frame(send_data)
                logger.debug('device[%s] send task data end, ssn=%s', self.device_id, self.ssn)
            # 读命令
            elif frame.ASDU.TYP == TYP.C_RD_NA_1:
                if frame.ASDU.Cause == Cause.act:
                    term_item_dict = self.redis.hgetall('HS:MAPPING:IEC104:{}:{}'.format(
                            self.device_id, frame.ASDU.data[0].address))
                    value = None
                    last_time = self.redis.lindex('LST:DATA_TIME:{}:{}:{}'.format(
                        self.device_id, term_item_dict['term_id'], term_item_dict['item_id']), -1)
                    if last_time:
                        value = self.redis.hget('HS:DATA:{}:{}:{}'.format(
                                self.device_id, term_item_dict['term_id'], term_item_dict['item_id']), last_time)
                    logger.debug('term_item_dict=%s', term_item_dict)
                    typ = TYP(int(term_item_dict['code_type']))
                    address = int(term_item_dict['protocol_code'])
                    send_frame = iec_104.init_frame(self.ssn, self.rsn, typ, Cause.req)
                    send_frame.ASDU.data[0].value = str_to_number(value) or random.uniform(100, 200)
                    send_frame.ASDU.data[0].address = address
                    logger.debug('C_RD_NA_1, send_frame=%s', send_frame)
                    await self.send_frame(send_frame)
            # 完成尚未实现的I帧
            else:
                logger.error("device[%s] unknown I_frame: %s", self.device_id, frame)

            if self.w == IECParam.W:
                logger.debug("self.w,Param_S=%s, send S_frame", (self.w, IECParam.W.value))
                await self.send_frame(iec_104.init_frame("S", self.rsn))
        except Exception as e:
            logger.error("device[%s] handle_i failed: %s", self.device_id, repr(e), exc_info=True)

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
                    self.save_frame(encode_frame, send=True)
            logger.debug("device[%s] after send_frame: send_list=%s", self.device_id,
                         [frm.APCI1 if frm.APCI1 == 'S' or isinstance(frm.APCI1, UFrame) else
                          frm.ASDU.TYP for frm in self.send_list])
        except Exception as e:
            logger.error("device[%s] send_frame failed: %s", self.device_id, repr(e), exc_info=True)

    async def check_to_send(self, frame):
        try:
            self.stop_timer(IECParam.T1)
            if not self.send_list or frame is None:
                return
            if isinstance(frame.APCI1, UFrame) \
                    and frame.APCI1 in (UFrame.STARTDT_CON, UFrame.TESTFR_CON, UFrame.STOPDT_CON):
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

    def save_frame(self, frame, send=True):
        IEC104Device.frame_list[self.device_id].append(('send' if send else 'recv', frame))

    async def generate_call_all_data(self):
        try:
            if not self.term_item_list:
                logger.info('device[%s] prepare term_item_list..', self.device_id)
                cursor = None
                all_keys = set()
                while cursor != 0:
                    res = self.redis.scan(cursor or b'0', match='HS:MAPPING:IEC104:{}:*'.format(self.device_id))
                    cursor, keys = res
                    all_keys.update(keys)
                self.term_item_list = [self.redis.hgetall(key) for key in all_keys]
                logger.info('device[%s] len(term_item_list) = %s', self.device_id, len(self.term_item_list))
            for term_item_dict in self.term_item_list:
                typ = int(term_item_dict['code_type'])
                if typ > TYP.M_EP_TD_1.value:
                    typ = TYP.M_ME_TC_1.value
                address = int(term_item_dict['protocol_code'])
                up_limit = term_item_dict.get('up_limit')
                down_limit = term_item_dict.get('down_limit')
                value = None
                if up_limit:
                    value = random.uniform(float(down_limit), float(up_limit))
                frame = iec_104.init_frame(self.ssn, self.rsn, TYP(typ), Cause.introgen)
                frame.ASDU.StartAddress = address
                frame.ASDU.data[0].address = address
                frame.ASDU.data[0].value = value or random.uniform(100, 200)
                if hasattr(frame.ASDU.data[0], 'cp56time2a'):
                    frame.ASDU.data[0].cp56time2a = datetime.datetime.now()
                if hasattr(frame.ASDU.data[0], 'cp24time2a'):
                    frame.ASDU.data[0].cp24time2a = datetime.datetime.now()
                await self.send_frame(frame)
        except Exception as e:
            logger.error("device[%s] generate_call_all_data failed: %s", self.device_id, repr(e), exc_info=True)

    def generate_call_power_data(self):
        pass


def create_servers(io_loop):
    server_list = []
    s = redis.StrictRedis(db=1, decode_responses=True)
    device_list = s.smembers('SET:DEVICE')
    for device_id in device_list:
        device = s.hgetall('HS:DEVICE:{}'.format(device_id))
        if 'port' not in device:
            continue
        server_list.append(
            io_loop.run_until_complete(
                    asyncio.start_server(functools.partial(IEC104Device, device), '127.0.0.1', device['port'])))
    return server_list


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    servers = create_servers(loop)
    try:
        print('mock device running.')
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    except Exception as ee:
        logger.error('run failed: %s', repr(ee), exc_info=True)
    finally:
        for server in servers:
            server.close()
            loop.run_until_complete(server.wait_closed())
        loop.close()
    print('mock device stopped.')
