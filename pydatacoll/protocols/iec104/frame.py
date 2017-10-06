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

from construct import *
from enum import IntEnum
import datetime
from pydatacoll.utils.read_config import config


class IECParam(IntEnum):
    # socket的connect / listen超时
    T0 = config.getint('IEC104', 'T0', fallback=30)
    # 发送I帧或U帧后，等待对方应答，等待超过T1则重启链路
    T1 = config.getint('IEC104', 'T1', fallback=15)
    # 接收到I帧后等待T2时间，然后发送对I帧的应答
    T2 = config.getint('IEC104', 'T2', fallback=10)
    # T3时间内未收到任何报文，发送TESTFR
    T3 = config.getint('IEC104', 'T3', fallback=20)
    # 发送方在有k个I格式报文未得到对方的确认时，将停止数据传送
    K = config.getint('IEC104', 'K', fallback=12)
    # 接收方最迟在接收了w个I格式报文后应发出认可
    W = config.getint('IEC104', 'W', fallback=8)


class UFrame(IntEnum):
    TESTFR_CON = 0x83,  # 心跳应答（确认）
    TESTFR_ACT = 0x43,  # 心跳请求（激活）

    STOPDT_CON = 0x23,  # 关闭链路应答（确认）
    STOPDT_ACT = 0x13,  # 关闭链路请求（激活）

    STARTDT_CON = 0x0b,  # 建立链路应答（确认）
    STARTDT_ACT = 0x07,  # 建立链路请求（激活）


class TYP(IntEnum):
    # 监视方向的过程信息
    M_SP_NA_1 = 1,  # 单点信息
    M_SP_TA_1 = 2,  # 带时标的单点信息
    M_DP_NA_1 = 3,  # 双点信息
    M_DP_TA_1 = 4,  # 带时标的双点信息
    M_ST_NA_1 = 5,  # 步位置信息
    M_ST_TA_1 = 6,  # 带时标的步位置信息
    M_BO_NA_1 = 7,  # 32比特串
    M_BO_TA_1 = 8,  # 带时标的32比特串
    M_ME_NA_1 = 9,  # 测量值，归一化值
    M_ME_TA_1 = 10,  # 测量值，带时标的归一化值
    M_ME_NB_1 = 11,  # 测量值，标度化值
    M_ME_TB_1 = 12,  # 测量值，带时标的标度化值
    M_ME_NC_1 = 13,  # 测量值，短浮点数
    M_ME_TC_1 = 14,  # 测量值，带时标的短浮点数
    M_IT_NA_1 = 15,  # 累计量
    M_IT_TA_1 = 16,  # 带时标的累计量
    # M_EP_TA_1 = 17,  # 带时标的继电保护设备事件
    # M_EP_TB_1 = 18,  # 带时标的继电保护设备成组启动事件
    # M_EP_TC_1 = 19,  # 带时标的继电保护设备成组输出电路信息
    M_PS_NA_1 = 20,  # 具有状态变位检出的成组单点信息
    M_ME_ND_1 = 21,  # 测量值，不带品质描述的归一化值
    M_SP_TB_1 = 30,  # 带时标CP56Time2a的单点信息
    M_DP_TB_1 = 31,  # 带时标CP56Time2a的双点信息
    M_ST_TB_1 = 32,  # 带时标CP56Time2a的步位置信息
    M_BO_TB_1 = 33,  # 带时标CP56Time2a的32位串
    M_ME_TD_1 = 34,  # 带时标CP56Time2a的归一化测量值
    M_ME_TE_1 = 35,  # 测量值，带时标CP56Time2a的标度化值
    M_ME_TF_1 = 36,  # 测量值，带时标CP56Time2a的短浮点数
    M_IT_TB_1 = 37,  # 带时标CP56Time2a的累计值
    M_EP_TD_1 = 38,  # 带时标CP56Time2a的继电保护装置事件
    # M_EP_TE_1 = 39,  # 带时标CP56Time2a的继电保护装置成组启动事件
    # M_EP_TF_1 = 40,  # 带时标CP56Time2a的继电保护装置成组输出电路信息
    # 控制方向的过程信息
    C_SC_NA_1 = 45,  # 单命令
    C_DC_NA_1 = 46,  # 双命令
    C_RC_NA_1 = 47,  # 步调节命令
    C_SE_NA_1 = 48,  # 设定值命令，归一化值
    C_SE_NB_1 = 49,  # 设定值命令，标度化值
    C_SE_NC_1 = 50,  # 设定值命令，短浮点数
    C_BO_NA_1 = 51,  # 设定值命令，32比特串
    C_SC_TA_1 = 58,  # 带时标CP56Time2a的单命令
    C_DC_TA_1 = 59,  # 带时标CP56Time2a的双命令
    C_RC_TA_1 = 60,  # 带时标CP56Time2a的步调节命令
    C_SE_TA_1 = 61,  # 带时标CP56Time2a的设定值命令，归一化值
    C_SE_TB_1 = 62,  # 带时标CP56Time2a的设定值命令，标度化值
    C_SE_TC_1 = 63,  # 带时标CP56Time2a的设定值命令，短浮点数
    C_BO_TA_1 = 64,  # 带时标CP56Time2a的32比特串
    # 监视方向的系统信息
    # M_EI_NA_1 = 70,  # 初始化结束
    # 控制方向的系统信息
    C_IC_NA_1 = 100,  # 总召唤命令
    C_CI_NA_1 = 101,  # 电能脉冲召唤命令
    C_RD_NA_1 = 102,  # 读命令
    C_CS_NA_1 = 103,  # 时钟同步命令
    # C_TS_NA_1 = 103,  # 测试命令
    # C_RP_NA_1 = 105,  # 复位进程命令
    # C_TS_NA_1 = 107,  # 带时标CP56Time2a的测试命令
    # 控制方向的参数命令
    # P_ME_NA_1 = 110,  # 测量值参数，归一化值
    # P_ME_NB_1 = 111,  # 测量值参数，标度化值
    # P_ME_NC_1 = 112,  # 测量值参数，短浮点数
    # P_AC_NA_1 = 113,  # 参数激活
    # 文件传输
    # F_FR_NA_1 = 120,  # 文件已准备好
    # F_SR_NA_1 = 121,  # 节已准备好
    # F_SC_NA_1 = 122,  # 召唤目录，选择文件，召唤文件，召唤节
    # F_LS_NA_1 = 123,  # 最后的节，最后的段
    # F_AF_NA_1 = 124,  # 确认文件，确认节
    # F_SG_NA_1 = 125,  # 段
    # F_DR_TA_1 = 126,  # 目录（监视方向有效）
    # F_SC_NB_1 = 127,  # 查询日志(QueryLog)


class Cause(IntEnum):
    unused = 0,  # 未用
    percyc = 1,  # 周期、循环
    back = 2,  # 背景扫描
    spont = 3,  # 突发（自发）
    init = 4,  # 初始化
    req = 5,  # 请求或者被请求
    act = 6,  # 激活
    actcon = 7,  # 激活确认
    deact = 8,  # 停止激活
    deactcon = 9,  # 停止激活确认
    actterm = 10,  # 激活终止
    retrem = 11,  # 远方命令引起的返送信息
    retloc = 12,  # 当地命令引起的返送信息
    file = 13,  # 文件传输
    introgen = 20,  # 响应站召唤
    inro1 = 21,  # 响应第1组召唤
    inro2 = 22,  # 响应第2组召唤
    inro3 = 23,  # 响应第3组召唤
    inro4 = 24,  # 响应第4组召唤
    inro5 = 25,  # 响应第5组召唤
    inro6 = 26,  # 响应第6组召唤
    inro7 = 27,  # 响应第7组召唤
    inro8 = 28,  # 响应第8组召唤
    inro9 = 29,  # 响应第9组召唤
    inro10 = 30,  # 响应第10组召唤
    inro11 = 31,  # 响应第11组召唤
    inro12 = 32,  # 响应第12组召唤
    inro13 = 33,  # 响应第13组召唤
    inro14 = 34,  # 响应第14组召唤
    inro15 = 35,  # 响应第15组召唤
    inro16 = 36,  # 响应第16组召唤
    reqcogen = 37,  # 响应计数量（累计量）站（总）召唤
    reqco1 = 38,  # 响应第1组计数量（累计量）召唤
    reqco2 = 39,  # 响应第2组计数量（累计量）召唤
    reqco3 = 40,  # 响应第3组计数量（累计量）召唤
    reqco4 = 41,  # 响应第4组计数量（累计量）召唤
    badtyp = 44,  # 未知的类型标识
    badre = 45,  # 未知的传送原因
    badad = 46,  # 未知的应用服务数据单元公共地址
    badad2 = 47,  # 未知的信息对象地址


# 带品质描述词的单点信息
SIQ = "SIQ" / BitStruct(
    "IV" / Default(Bit, 0),  # 0 有效 1 无效
    "NT" / Default(Bit, 0),  # 0 当前值 1 非当前值
    "SB" / Default(Bit, 0),  # 0 未被取代 1 被取代
    "BL" / Default(Bit, 0),  # 0 未被闭锁 1 被闭锁
    Padding(3),
    "value" / Default(Bit, 0),  # 单点信息 0 开 1 合
)

# 带品质描述词的双点信息
DIQ = "DIQ" / BitStruct(
    "IV" / Default(Bit, 0),  # 0 有效 1 无效
    "NT" / Default(Bit, 0),  # 0 当前值 1 非当前值
    "SB" / Default(Bit, 0),  # 0 未被取代 1 被取代
    "BL" / Default(Bit, 0),  # 0 未被闭锁 1 被闭锁
    Padding(2),
    "value" / Default(BitsInteger(2), 0),  # 双点信息 0 中间状态 1 确定开 2 确定合 3 不确定
)

# 品质描述词
QDS = "QDS" / BitStruct(
    "IV" / Default(Bit, 0),  # 0 有效 1 无效
    "NT" / Default(Bit, 0),  # 0 当前值 1 非当前值
    "SB" / Default(Bit, 0),  # 0 未被取代 1 被取代
    "BL" / Default(Bit, 0),  # 0 未被闭锁 1 被闭锁
    Padding(3),
    "OV" / Default(SymmetricMapping(Bit, {True: 0, False: 1}, default=True), 0),  # 0 未溢出 1 溢出
)

# 继电保护设备事件的品质描述词
QDP = "QDP" / BitStruct(
    "IV" / Default(Bit, 0),  # 0 有效 1 无效
    "NT" / Default(Bit, 0),  # 0 当前值 1 非当前值
    "SB" / Default(Bit, 0),  # 0 未被取代 1 被取代
    "BL" / Default(Bit, 0),  # 0 未被闭锁 1 被闭锁
    "EI" / Default(Bit, 0),  # 0 动作时间有效 1 动作时间无效
    Padding(3),
)

# 带瞬变状态指示的值
VTI = "VTI" / BitStruct(
    "VT" / Flag,  # 0 设备未在瞬变状态 1 设备处于瞬变状态
    "value" / Default(BitsInteger(7), 0),  # 值
)

# 二进制计数器读数
BCR = "BCR" / Struct(
    "value" / Default(Int32ul, 0),  # 读数
    EmbeddedBitStruct(
        "IV" / Default(Bit, 0),  # 0 有效 1 无效
        "CA" / Default(Bit, 0),  # 0 上次读数后计数器未被调整 1 被调整
        "CY" / Default(Bit, 0),  # 0 未溢出 1 溢出
        "sq" / Default(BitsInteger(5), 0),  # 0~31 顺序号
    ),
)

# 继电保护设备单个事件
SEP = "SEP" / BitStruct(
    "IV" / Default(Bit, 0),  # 0 有效 1 无效
    "NT" / Default(Bit, 0),  # 0 当前值 1 非当前值
    "SB" / Default(Bit, 0),  # 0 未被取代 1 被取代
    "BL" / Default(Bit, 0),  # 0 未被闭锁 1 被闭锁
    "EI" / Default(Bit, 0),  # 0 动作时间有效 1 动作时间无效
    Padding(1),
    "value" / Default(BitsInteger(2), 0),  # 事件状态 0 中间状态 1 确定开 2 确定合 3 不确定
)

# 测量值参数限定词
QPM = "QPM" / BitStruct(
    "POP" / Default(Bit, 0),  # 0 运行 1 未运行
    "LPC" / Default(Bit, 0),  # 0 未改变 1 改变
    "KPA" / Default(BitsInteger(6), 0),  # 参数类别 0 未用 1 门限值 2 平滑系数（滤波时间常数） 3 下限 4 上限
)

# 设定命令限定词
QOS = "QOS" / BitStruct(
    "se" / Default(Bit, 0),  # 0 执行 1 选择
    "QL" / Default(BitsInteger(7), 0),  # 0 缺省
)


def _encode_cp56time2a(obj, _):
    now = obj
    if obj is None:
        now = datetime.datetime.now()
    return Container(Year=now.year % 2000,
                     Month=now.month, Day=now.day,
                     Week=now.isoweekday(),
                     Hour=now.hour, SU=0,
                     Minute=now.minute, IV=0,
                     Millisecond=now.microsecond // 1000 + now.second * 1000)


# 二进制时间
cp56time2a = "cp56time2a" / ExprAdapter(Default(Struct(
    "Millisecond" / Int16ul,  # 0~59999
    EmbeddedBitStruct(
        "IV" / Default(Bit, 0),  # 0 有效 1 无效
        Padding(1),
        "Minute" / Default(BitsInteger(6), 0),  # 0~59
        "SU" / Default(Bit, 0),  # 0 标准时间 1 夏季时间
        Padding(2),
        "Hour" / Default(BitsInteger(5), 0),  # 0~23
        "Week" / Default(BitsInteger(3), 0),  # 1~7
        "Day" / Default(BitsInteger(5), 0),  # 1~31
        Padding(4),
        "Month" / Nibble,  # 1~12
        Padding(1),
        "Year" / Default(BitsInteger(7), 0),  # 0~99  取年份的后两位 例如: 2015 -> 15
    )
), None),
    encoder=_encode_cp56time2a,
    decoder=lambda obj, ctx: datetime.datetime(year=obj.Year + 2000,
                                               month=obj.Month, day=obj.Day,
                                               hour=obj.Hour,
                                               minute=obj.Minute,
                                               second=obj.Millisecond // 1000,
                                               microsecond=obj.Millisecond % 1000 * 1000)
)


def _encode_cp24time2a(obj, _):
    now = obj
    if obj is None:
        now = datetime.datetime.now()
    return Container(Minute=now.minute, IV=0, Millisecond=now.microsecond // 1000 + now.second * 1000)


def _decode_cp24time2a(obj, _):
    now = datetime.datetime.now()
    return datetime.datetime(now.year, now.month, now.day, now.hour,
                             minute=obj.Minute, second=obj.Millisecond // 1000,
                             microsecond=obj.Millisecond % 1000 * 1000)


# 二进制时间
cp24time2a = "cp24time2a" / ExprAdapter(Default(
    Struct("Millisecond" / Int16ul,  # 0~59999
           EmbeddedBitStruct(
               "IV" / Default(Bit, 0),  # 0 有效 1 无效
               Padding(1),
               "Minute" / Default(BitsInteger(6), 0),  # 0~59
           )), None),
    encoder=_encode_cp24time2a,
    decoder=_decode_cp24time2a
)

# 单命令
SCO = "SCO" / BitStruct(
    "se" / Default(Bit, 0),  # 0 执行 1 选择
    "QU" / Default(BitsInteger(5), 0),  # 0 无定义 1 短脉冲持续时间 2 长脉冲持续时间 3 持续输出
    Padding(1),
    "value" / Default(Bit, 0),  # 单命令状态 0 开 1 合
)

# 双命令
DCO = "DCO" / BitStruct(
    "se" / Default(Bit, 0),  # 0 执行 1 选择
    "QU" / Default(BitsInteger(5), 0),  # 0 无定义 1 短脉冲持续时间 2 长脉冲持续时间 3 持续输出
    "value" / Default(BitsInteger(2), 0),  # 双命令状态 0 不允许 1 开 2 合 3 不允许
)

# 步调节命令
RCO = "RCO" / BitStruct(
    "se" / Default(Bit, 0),  # 0 执行 1 选择
    "QU" / Default(BitsInteger(5), 0),  # 0 无定义 1 短脉冲持续时间 2 长脉冲持续时间 3 持续输出
    "value" / Default(BitsInteger(2), 0),  # 双命令状态 0 不允许 1 降一步 2 升一步 3 不允许
)

# 1 单点信息
ASDU_M_SP_NA_1 = "ASDU_M_SP_NA_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    Embedded(SIQ),
)

# 2 带时标的单点信息
ASDU_M_SP_TA_1 = "ASDU_M_SP_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(SIQ),
    cp24time2a,
)

# 3 双点信息
ASDU_M_DP_NA_1 = "ASDU_M_DP_NA_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    Embedded(DIQ),
)

# 4 带时标的双点信息
ASDU_M_DP_TA_1 = "ASDU_M_DP_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(DIQ),
    cp24time2a,
)

# 5 步位置信息
ASDU_M_ST_NA_1 = "ASDU_M_ST_NA_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    Embedded(VTI),
    Embedded(QDS),
)

# 6 带时标的步位置信息
ASDU_M_ST_TA_1 = "ASDU_M_ST_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(VTI),
    Embedded(QDS),
    cp24time2a,
)

# 7 32比特串
ASDU_M_BO_NA_1 = "ASDU_M_BO_NA_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    "value" / Default(Int32ul, 0),
    Embedded(QDS),
)

# 8 带时标的32比特串
ASDU_M_BO_TA_1 = "ASDU_M_BO_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int32ul, 0),
    Embedded(QDS),
    cp24time2a,
)

# 9 测量值，归一化值
ASDU_M_ME_NA_1 = "ASDU_M_ME_NA_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    "value" / Default(Int16ul, 0),
    Embedded(QDS),
)

# 10 测量值，带时标的归一化值
ASDU_M_ME_TA_1 = "ASDU_M_ME_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QDS),
    cp24time2a,
)

# 11 测量值，标度化值
ASDU_M_ME_NB_1 = "ASDU_M_SP_NB_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    "value" / Default(Int16ul, 0),
    Embedded(QDS),
)

# 12 测量值，带时标的标度化值
ASDU_M_ME_TB_1 = "ASDU_M_ME_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QDS),
    cp24time2a,
)

# 13 测量值，短浮点数
ASDU_M_ME_NC_1 = "ASDU_M_ME_NC_1" / Struct(
    EmbeddedBitStruct(If(this._._.sq == 0, "address" / Default(BitsInteger(24, swapped=True), 0))),
    "value" / Default(Float32l, 0),
    Embedded(QDS),
)

# 14 测量值，带时标短浮点数
ASDU_M_ME_TC_1 = "ASDU_M_ME_TC_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Float32l, 0),
    Embedded(QDS),
    cp24time2a,
)


# 15 累计量
ASDU_M_IT_NA_1 = "ASDU_M_IT_NA_1" / Struct(
    EmbeddedBitStruct("address" / If(this._._.sq == 0, Default(BitsInteger(24, swapped=True), 0))),
    Embedded(BCR),
)

# 16 带时标的累计量
ASDU_M_IT_TA_1 = "ASDU_M_IT_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(BCR),
    cp24time2a,
)

# 20 具有状态变位检出的成组单点信息
ASDU_M_PS_NA_1 = "ASDU_M_PS_NA_1" / Struct(
    EmbeddedBitStruct("address" / If(this._._.sq == 0, Default(BitsInteger(24, swapped=True), 0))),
    "value" / Default(Int16ul, 0),  # 每一位 0 开 1 合
    "CD" / Int16ul,  # 每一位 0 ST对应位未改变 1 ST对应位有改变
    Embedded(QDS),
)

# 21 测量值，不带品质描述的归一化值
ASDU_M_ME_ND_1 = "ASDU_M_ME_ND_1" / Struct(
    EmbeddedBitStruct("address" / If(this._._.sq == 0, Default(BitsInteger(24, swapped=True), 0))),
    "value" / Default(Int16ul, 0),
)

# 30 带时标CP56Time2a的单点信息
ASDU_M_SP_TB_1 = "ASDU_M_SP_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(SIQ),
    cp56time2a,
)

# 31 带时标CP56Time2a的双点信息
ASDU_M_DP_TB_1 = "ASDU_M_DP_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(DIQ),
    cp56time2a,
)

# 32 带时标CP56Time2a的步位置信息
ASDU_M_ST_TB_1 = "ASDU_M_ST_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(VTI),
    Embedded(QDS),
    cp56time2a,
)

# 33 带时标CP56Time2a的32位串
ASDU_M_BO_TB_1 = "ASDU_M_BO_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int32ul, 0),
    Embedded(QDS),
    cp56time2a,
)

# 34 带时标CP56Time2a的归一化测量值
ASDU_M_ME_TD_1 = "ASDU_M_ME_TD_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QDS),
    cp56time2a,
)

# 35 测量值，带时标CP56Time2a的标度化值
ASDU_M_ME_TE_1 = "ASDU_M_ME_TE_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QDS),
    cp56time2a,
)

# 36 测量值，带时标CP56Time2a的短浮点数
ASDU_M_ME_TF_1 = "ASDU_M_ME_TF_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Float32l, 0),
    Embedded(QDS),
    cp56time2a,
)

# 37 带时标CP56Time2a的累计值
ASDU_M_IT_TB_1 = "ASDU_M_IT_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(BCR),
    cp56time2a,
)

# 38 带时标CP56Time2a的继电保护装置事件
ASDU_M_EP_TD_1 = "ASDU_M_EP_TD_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(SEP),
    "CP16Time2a" / Int16ul,
    cp56time2a,
)

# 45 单命令
ASDU_C_SC_NA_1 = "ASDU_C_SC_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(SCO),
)

# 46 双命令
ASDU_C_DC_NA_1 = "ASDU_C_DC_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(DCO),
)

# 47 步调节命令
ASDU_C_RC_NA_1 = "ASDU_C_RC_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(RCO),
)

# 48 设定值命令，归一化值
ASDU_C_SE_NA_1 = "ASDU_C_SE_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QOS),
)

# 49 设定值命令，标度化值
ASDU_C_SE_NB_1 = "ASDU_C_SE_NB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QOS),
)

# 50 设定值命令，短浮点数
ASDU_C_SE_NC_1 = "ASDU_C_SE_NC_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Float32l, 0),
    Embedded(QOS),
)

# 51 设定值命令，32位比特串
ASDU_C_BO_NA_1 = "ASDU_C_BO_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int32ul, 0),
)

# 58 带时标CP56Time2a的单命令
ASDU_C_SC_TA_1 = "ASDU_C_SC_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(SCO),
    cp56time2a,
)

# 59 带时标CP56Time2a的双命令
ASDU_C_DC_TA_1 = "ASDU_C_DC_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(DCO),
    cp56time2a,
)

# 60 带时标CP56Time2a的步调节命令
ASDU_C_RC_TA_1 = "ASDU_C_RC_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    Embedded(RCO),
    cp56time2a,
)

# 61 带时标CP56Time2a的设定值命令，归一化值
ASDU_C_SE_TA_1 = "ASDU_C_SE_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QOS),
    cp56time2a,
)

# 62 带时标CP56Time2a的设定值命令，标度化值
ASDU_C_SE_TB_1 = "ASDU_C_SE_TB_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int16ul, 0),
    Embedded(QOS),
    cp56time2a,
)

# 63 带时标CP56Time2a的设定值命令，短浮点数
ASDU_C_SE_TC_1 = "ASDU_C_SE_TC_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Float32l, 0),
    Embedded(QOS),
    cp56time2a,
)

# 64 带时标CP56Time2a的32比特串
ASDU_C_BO_TA_1 = "ASDU_C_BO_TA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    "value" / Default(Int32ul, 0),
    cp56time2a,
)

# 100 总召唤命令
ASDU_C_IC_NA_1 = "ASDU_C_IC_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    # ULInt8("QOI"),  # 0 未用 20 站召唤（总招） 21~36 第1~16组召唤
    Const(b"\x14"),
)

# 101 电能脉冲召唤命令
ASDU_C_CI_NA_1 = "ASDU_C_CI_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
    # EmbeddedBitStruct(
    #     BitsInteger("FRZ", 2),
    #     # 0 读（无冻结复位） 1 计数量冻结不带复位（累加值） 2 冻结带复位（增量信息） 3 计数量复位
    #     BitsInteger("RQT", 6),  # 0 未用 1~4 请求1~4组计数量 5 请求总的计数量（总招）
    # ),
    Const(b"\x05"),
)

# 102 读命令
ASDU_C_RD_NA_1 = "ASDU_C_RD_NA_1" / Struct(
    EmbeddedBitStruct("address" / Default(BitsInteger(24, swapped=True), 0)),  # 信息对象地址
)

# 103 时钟同步命令
ASDU_C_CS_NA_1 = "ASDU_C_CS_NA_1" / Struct(
    Padding(3),
    cp56time2a,
)

typ_dict = {name: globals()["ASDU_%s" % name] for name in TYP.__members__}

ASDU_Part = "ASDU" / Struct(
    # 类型标识
    "TYP" / ExprAdapter(Byte, encoder=lambda obj, ctx: obj, decoder=lambda obj, ctx: TYP(obj)),
    # 可变结构限定词
    EmbeddedBitStruct(
        "sq" / Default(Bit, 0),  # 单个或者顺序寻址 0 信息对象地址不连续（包含地址） 1 信息对象地址连续（不包含地址）
        "sq_count" / Default(BitsInteger(7), 1),  # 数目 0 不含信息对象 1~127 信息元素的数目
    ),
    # # 传送原因 COT
    "Cause" / ExprAdapter(
        Default(Int16ul, Cause.unused), encoder=lambda obj, ctx: obj, decoder=lambda obj, ctx: Cause(obj)),
    # EmbeddedBitStruct(
    #     BitsInteger("SourceAddress", 8),  # 源发站地址 0 缺省值 1~255 源发站地址号
    #     "T" / Default(Bit, 0),  # 试验 0 未试验 1 试验
    #     "PN" / Default(Bit, 0),  # 0 肯定确认 1 否定确认
    #     ExprAdapter(BitsInteger("Cause", 6), encoder=lambda obj, ctx: obj, decoder=lambda obj, ctx: Cause(obj)),
    # ),
    # ASDU公共地址
    "GlobalAddress" / Default(Int16ul, 1),  # 0 未用 1~65534 站地址 65535 全局地址
    "StartAddress" / Default(If(this.sq == 1, BytesInteger(3, swapped=True)), 0),
    # 信息对象
    "data" / Default(Array(this.sq_count, Switch(lambda ctx: ctx.TYP.name, typ_dict, Pass)), [{}])
)

iec_head = "iec104_head" / Struct(
    Const(b"\x68"),
    "length" / Byte,  # 帧长度（不算帧头和长度，总帧长=length+2）
)


class CalcLength(Tunnel):
    def _decode(self, data, context):
        return data

    def _encode(self, data, context):
        return b"\x68%c%b" % (len(data) - 2, data[2:])


class If2(Construct):
    __slots__ = ["build_func", "parse_func", "true_part"]

    def __init__(self, build_func, parse_func, true_part):
        super(If2, self).__init__()
        self.build_func = build_func
        self.parse_func = parse_func
        self.true_part = true_part

    def _build(self, obj, stream, context, path):
        if self.build_func(context):
            return self.true_part._build(obj, stream, context, path)

    def _parse(self, stream, context, path):
        if self.parse_func(context):
            return self.true_part._parse(stream, context, path)


iec_104 = "iec104" / CalcLength(Struct(
    Const(b"\x68"),
    "length" / Default(Byte, 1),
    "APCI1" / ExprAdapter(
        Default(Int16ul, None),
        encoder=lambda obj, ctx: obj if isinstance(obj, UFrame) else 1 if obj == "S" else obj << 1,
        decoder=lambda obj, ctx: obj >> 1 if obj & 1 == 0 else "S" if obj & 3 == 1 else UFrame(obj),
    ),
    "APCI2" / ExprAdapter(
        Default(Int16ul, None),
        encoder=lambda obj, ctx: 0 if obj is None else obj << 1,
        decoder=lambda obj, ctx: obj >> 1,
    ),
    # Only I-frame has ASDU part, S-frame and U-frame doesn't
    "ASDU" / Default(If2(
        lambda ctx: not isinstance(ctx.APCI1, UFrame) and ctx.APCI1 != 1,
        lambda ctx: not isinstance(ctx.APCI1, UFrame) and ctx.APCI1 != "S",
        ASDU_Part), None)
))
