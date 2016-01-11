from construct import *
from enum import IntEnum
import datetime
from pydatacoll.utils.read_config import *


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
SIQ = BitStruct(
        "SIQ",
        Bit("IV"),  # 0 有效 1 无效
        Bit("NT"),  # 0 当前值 1 非当前值
        Bit("SB"),  # 0 未被取代 1 被取代
        Bit("BL"),  # 0 未被闭锁 1 被闭锁
        Padding(3),
        Bit("Value"),  # 单点信息 0 开 1 合
)

# 带品质描述词的双点信息
DIQ = BitStruct(
        "DIQ",
        Bit("IV"),  # 0 有效 1 无效
        Bit("NT"),  # 0 当前值 1 非当前值
        Bit("SB"),  # 0 未被取代 1 被取代
        Bit("BL"),  # 0 未被闭锁 1 被闭锁
        Padding(2),
        Bits("Value", 2),  # 双点信息 0 中间状态 1 确定开 2 确定合 3 不确定
)

# 品质描述词
QDS = BitStruct(
        "QDS",
        Bit("IV"),  # 0 有效 1 无效
        Bit("NT"),  # 0 当前值 1 非当前值
        Bit("SB"),  # 0 未被取代 1 被取代
        Bit("BL"),  # 0 未被闭锁 1 被闭锁
        Padding(3),
        Flag("OV", truth=0, falsehood=1, default=True),  # 0 未溢出 1 溢出
)

# 继电保护设备事件的品质描述词
QDP = BitStruct(
        "QDP",
        Bit("IV"),  # 0 有效 1 无效
        Bit("NT"),  # 0 当前值 1 非当前值
        Bit("SB"),  # 0 未被取代 1 被取代
        Bit("BL"),  # 0 未被闭锁 1 被闭锁
        Bit("EI"),  # 0 动作时间有效 1 动作时间无效
        Padding(3),
)

# 带瞬变状态指示的值
VTI = BitStruct(
        "VTI",
        Flag("VT"),  # 0 设备未在瞬变状态 1 设备处于瞬变状态
        Bits("Value", 7),  # 值
)

# 二进制计数器读数
BCR = Struct(
        "BCR",
        ULInt32("Value"),  # 读数
        EmbeddedBitStruct(
                Bit("IV"),  # 0 有效 1 无效
                Bit("CA"),  # 0 上次读数后计数器未被调整 1 被调整
                Bit("CY"),  # 0 未溢出 1 溢出
                Bits("SQ", 5),  # 0~31 顺序号
        ),
)

# 继电保护设备单个事件
SEP = BitStruct(
        "SEP",
        Bit("IV"),  # 0 有效 1 无效
        Bit("NT"),  # 0 当前值 1 非当前值
        Bit("SB"),  # 0 未被取代 1 被取代
        Bit("BL"),  # 0 未被闭锁 1 被闭锁
        Bit("EI"),  # 0 动作时间有效 1 动作时间无效
        Padding(1),
        Bits("Value", 2),  # 事件状态 0 中间状态 1 确定开 2 确定合 3 不确定
)

# 测量值参数限定词
QPM = BitStruct(
        "QPM",
        Bit("POP"),  # 0 运行 1 未运行
        Bit("LPC"),  # 0 未改变 1 改变
        Bits("KPA", 6),  # 参数类别 0 未用 1 门限值 2 平滑系数（滤波时间常数） 3 下限 4 上限
)

# 设定命令限定词
QOS = BitStruct(
        "QOS",
        Bit("SE"),  # 0 执行 1 选择
        Bits("QL", 7),  # 0 缺省
)

# 二进制时间
CP56Time2a = ExprAdapter(
        Struct(
                "CP56Time2a",
                ULInt16("Millisecond"),  # 0~59999
                EmbeddedBitStruct(
                        Bit("IV"),  # 0 有效 1 无效
                        Padding(1),
                        Bits("Minute", 6),  # 0~59
                        Bit("SU"),  # 0 标准时间 1 夏季时间
                        Padding(2),
                        Bits("Hour", 5),  # 0~23
                        Bits("Week", 3),  # 1~7
                        Bits("Day", 5),  # 1~31
                        Padding(4),
                        Nibble("Month"),  # 1~12
                        Padding(1),
                        Bits("Year", 7),  # 0~99  取年份的后两位 例如: 2015 -> 15
                )
        ),
        encoder=lambda time, ctx: Container(Year=time.year % 2000,
                                            Month=time.month, Day=time.day,
                                            Week=time.isoweekday(),
                                            Hour=time.hour, SU=0,
                                            Minute=time.minute, IV=0,
                                            Millisecond=time.microsecond // 1000 + time.second * 1000),
        decoder=lambda obj, ctx: datetime.datetime(year=obj.Year + 2000,
                                                   month=obj.Month, day=obj.Day,
                                                   hour=obj.Hour,
                                                   minute=obj.Minute,
                                                   second=obj.Millisecond // 1000,
                                                   microsecond=obj.Millisecond % 1000 * 1000)
)


def _decode_cp24time2a(obj, ctx):
    now = datetime.datetime.now()
    return datetime.datetime(now.year, now.month, now.day, now.hour,
                             minute=obj.Minute, second=obj.Millisecond // 1000,
                             microsecond=obj.Millisecond % 1000 * 1000)


# 二进制时间
CP24Time2a = ExprAdapter(
        Struct("CP24Time2a",
               ULInt16("Millisecond"),  # 0~59999
               EmbeddedBitStruct(
                       Bit("IV"),  # 0 有效 1 无效
                       Padding(1),
                       Bits("Minute", 6),  # 0~59
               )),
        encoder=lambda time, ctx: Container(Minute=time.minute, IV=0,
                                            Millisecond=time.microsecond // 1000 + time.second * 1000),
        decoder=_decode_cp24time2a
)

# 单命令
SCO = BitStruct(
        "SCO",
        Bit("SE"),  # 0 执行 1 选择
        Bits("QU", 5),  # 0 无定义 1 短脉冲持续时间 2 长脉冲持续时间 3 持续输出
        Padding(1),
        Bit("Value"),  # 单命令状态 0 开 1 合
)

# 双命令
DCO = BitStruct(
        "DCO",
        Bit("SE"),  # 0 执行 1 选择
        Bits("QU", 5),  # 0 无定义 1 短脉冲持续时间 2 长脉冲持续时间 3 持续输出
        Bits("Value", 2),  # 双命令状态 0 不允许 1 开 2 合 3 不允许
)

# 步调节命令
RCO = BitStruct(
        "RCO",
        Bit("SE"),  # 0 执行 1 选择
        Bits("QU", 5),  # 0 无定义 1 短脉冲持续时间 2 长脉冲持续时间 3 持续输出
        Bits("Value", 2),  # 双命令状态 0 不允许 1 降一步 2 升一步 3 不允许
)

# 1 单点信息
ASDU_M_SP_NA_1 = Struct(
        "ASDU_M_SP_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        Embed(SIQ),
)

# 2 带时标的单点信息
ASDU_M_SP_TA_1 = Struct(
        "ASDU_M_SP_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(SIQ),
        CP24Time2a,
)

# 3 双点信息
ASDU_M_DP_NA_1 = Struct(
        "ASDU_M_DP_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        Embed(DIQ),
)

# 4 带时标的双点信息
ASDU_M_DP_TA_1 = Struct(
        "ASDU_M_DP_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(DIQ),
        CP24Time2a,
)

# 5 步位置信息
ASDU_M_ST_NA_1 = Struct(
        "ASDU_M_ST_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        Embed(VTI),
        Embed(QDS),
)

# 6 带时标的步位置信息
ASDU_M_ST_TA_1 = Struct(
        "ASDU_M_ST_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(VTI),
        Embed(QDS),
        CP24Time2a,
)

# 7 32比特串
ASDU_M_BO_NA_1 = Struct(
        "ASDU_M_BO_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        ULInt32("Value"),
        Embed(QDS),
)

# 8 带时标的32比特串
ASDU_M_BO_TA_1 = Struct(
        "ASDU_M_BO_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt32("Value"),
        Embed(QDS),
        CP24Time2a,
)

# 9 测量值，归一化值
ASDU_M_ME_NA_1 = Struct(
        "ASDU_M_ME_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        ULInt16("Value"),
        Embed(QDS),
)

# 10 测量值，带时标的归一化值
ASDU_M_ME_TA_1 = Struct(
        "ASDU_M_ME_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QDS),
        CP24Time2a,
)

# 11 测量值，标度化值
ASDU_M_ME_NB_1 = Struct(
        "ASDU_M_SP_NB_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        ULInt16("Value"),
        Embed(QDS),
)

# 12 测量值，带时标的标度化值
ASDU_M_ME_TB_1 = Struct(
        "ASDU_M_ME_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QDS),
        CP24Time2a,
)

# 13 测量值，短浮点数
ASDU_M_ME_NC_1 = Struct(
        "ASDU_M_ME_NC_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        LFloat32("Value"),
        Embed(QDS),
)

# 14 测量值，带时标短浮点数
ASDU_M_ME_TC_1 = Struct(
        "ASDU_M_ME_TC_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        LFloat32("Value"),
        Embed(QDS),
        CP24Time2a,
)

# 15 累计量
ASDU_M_IT_NA_1 = Struct(
        "ASDU_M_IT_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        Embed(BCR),
)

# 16 带时标的累计量
ASDU_M_IT_TA_1 = Struct(
        "ASDU_M_IT_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(BCR),
        CP24Time2a,
)

# 20 具有状态变位检出的成组单点信息
ASDU_M_PS_NA_1 = Struct(
        "ASDU_M_PS_NA_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        ULInt16("Value"),  # 每一位 0 开 1 合
        ULInt16("CD"),  # 每一位 0 ST对应位未改变 1 ST对应位有改变
        Embed(QDS),
)

# 21 测量值，不带品质描述的归一化值
ASDU_M_ME_ND_1 = Struct(
        "ASDU_M_ME_ND_1",
        EmbeddedBitStruct(
                If(lambda ctx: ctx._.SQ == 0, BitField("Address", 24, swapped=True)),
        ),
        ULInt16("Value"),
)

# 30 带时标CP56Time2a的单点信息
ASDU_M_SP_TB_1 = Struct(
        "ASDU_M_SP_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(SIQ),
        CP56Time2a,
)

# 31 带时标CP56Time2a的双点信息
ASDU_M_DP_TB_1 = Struct(
        "ASDU_M_DP_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(DIQ),
        CP56Time2a,
)

# 32 带时标CP56Time2a的步位置信息
ASDU_M_ST_TB_1 = Struct(
        "ASDU_M_ST_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(VTI),
        Embed(QDS),
        CP56Time2a,
)

# 33 带时标CP56Time2a的32位串
ASDU_M_BO_TB_1 = Struct(
        "ASDU_M_BO_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt32("Value"),
        Embed(QDS),
        CP56Time2a,
)

# 34 带时标CP56Time2a的归一化测量值
ASDU_M_ME_TD_1 = Struct(
        "ASDU_M_ME_TD_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QDS),
        CP56Time2a,
)

# 35 测量值，带时标CP56Time2a的标度化值
ASDU_M_ME_TE_1 = Struct(
        "ASDU_M_ME_TE_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QDS),
        CP56Time2a,
)

# 36 测量值，带时标CP56Time2a的短浮点数
ASDU_M_ME_TF_1 = Struct(
        "ASDU_M_ME_TF_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        LFloat32("Value"),
        Embed(QDS),
        CP56Time2a,
)

# 37 带时标CP56Time2a的累计值
ASDU_M_IT_TB_1 = Struct(
        "ASDU_M_IT_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(BCR),
        CP56Time2a,
)

# 38 带时标CP56Time2a的继电保护装置事件
ASDU_M_EP_TD_1 = Struct(
        "ASDU_M_EP_TD_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(SEP),
        ULInt16("CP16Time2a"),
        CP56Time2a,
)

# 45 单命令
ASDU_C_SC_NA_1 = Struct(
        "ASDU_C_SC_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(SCO),
)

# 46 双命令
ASDU_C_DC_NA_1 = Struct(
        "ASDU_C_DC_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(DCO),
)

# 47 步调节命令
ASDU_C_RC_NA_1 = Struct(
        "ASDU_C_RC_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(RCO),
)

# 48 设定值命令，归一化值
ASDU_C_SE_NA_1 = Struct(
        "ASDU_C_SE_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QOS),
)

# 49 设定值命令，标度化值
ASDU_C_SE_NB_1 = Struct(
        "ASDU_C_SE_NB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QOS),
)

# 50 设定值命令，短浮点数
ASDU_C_SE_NC_1 = Struct(
        "ASDU_C_SE_NC_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        LFloat32("Value"),
        Embed(QOS),
)

# 51 设定值命令，32位比特串
ASDU_C_BO_NA_1 = Struct(
        "ASDU_C_BO_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt32("Value"),
)

# 58 带时标CP56Time2a的单命令
ASDU_C_SC_TA_1 = Struct(
        "ASDU_C_SC_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(SCO),
        CP56Time2a,
)

# 59 带时标CP56Time2a的双命令
ASDU_C_DC_TA_1 = Struct(
        "ASDU_C_DC_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(DCO),
        CP56Time2a,
)

# 60 带时标CP56Time2a的步调节命令
ASDU_C_RC_TA_1 = Struct(
        "ASDU_C_RC_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        Embed(RCO),
        CP56Time2a,
)

# 61 带时标CP56Time2a的设定值命令，归一化值
ASDU_C_SE_TA_1 = Struct(
        "ASDU_C_SE_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QOS),
        CP56Time2a,
)

# 62 带时标CP56Time2a的设定值命令，标度化值
ASDU_C_SE_TB_1 = Struct(
        "ASDU_C_SE_TB_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt16("Value"),
        Embed(QOS),
        CP56Time2a,
)

# 63 带时标CP56Time2a的设定值命令，短浮点数
ASDU_C_SE_TC_1 = Struct(
        "ASDU_C_SE_TC_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        LFloat32("Value"),
        Embed(QOS),
        CP56Time2a,
)

# 64 带时标CP56Time2a的32比特串
ASDU_C_BO_TA_1 = Struct(
        "ASDU_C_BO_TA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt32("Value"),
        CP56Time2a,
)

# 100 总召唤命令
ASDU_C_IC_NA_1 = Struct(
        "ASDU_C_IC_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        ULInt8("QOI"),  # 0 未用 20 站召唤（总招） 21~36 第1~16组召唤
)

# 101 电能脉冲召唤命令
ASDU_C_CI_NA_1 = Struct(
        "ASDU_C_CI_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
        EmbeddedBitStruct(
                Bits("FRZ", 2),
                # 0 读（无冻结复位） 1 计数量冻结不带复位（累加值） 2 冻结带复位（增量信息） 3 计数量复位
                Bits("RQT", 6),  # 0 未用 1~4 请求1~4组计数量 5 请求总的计数量（总招）
        ),
)

# 102 读命令
ASDU_C_RD_NA_1 = Struct(
        "ASDU_C_RD_NA_1",
        EmbeddedBitStruct(BitField("Address", 24, swapped=True)),  # 信息对象地址
)

# 103 时钟同步命令
ASDU_C_CS_NA_1 = Struct(
        "ASDU_C_CS_NA_1",
        Padding(24),
        CP56Time2a,
)

ASDU_Part = Struct(
        "ASDU",
        # 类型标识
        ExprAdapter(Byte("TYP"), encoder=lambda obj, ctx: obj, decoder=lambda obj, ctx: TYP(obj)),
        # 可变结构限定词
        EmbeddedBitStruct(
                Bit("SQ"),  # 单个或者顺序寻址 0 信息对象地址不连续（包含地址） 1 信息对象地址连续（不包含地址）
                Bits("SQ_COUNT", 7),  # 数目 0 不含信息对象 1~127 信息元素的数目
        ),
        # 传送原因 COT
        EmbeddedBitStruct(
                Bit("T"),  # 试验 0 未试验 1 试验
                Bit("PN"),  # 0 肯定确认 1 否定确认
                Bits("SourceAddress", 8),  # 源发站地址 0 缺省值 1~255 源发站地址号
                ExprAdapter(Bits("Cause", 6), encoder=lambda obj, ctx: obj, decoder=lambda obj, ctx: Cause(obj)),
        ),
        # ASDU公共地址
        ULInt16("GlobalAddress"),  # 0 未用 1~65534 站地址 65535 全局地址
        EmbeddedBitStruct(If(lambda ctx: ctx.SQ == 1, BitField("StartAddress", 24, swapped=True))),
        # 信息对象
        Array(
                lambda ctx: ctx.SQ_COUNT,
                Switch("data", keyfunc=lambda ctx: ctx.TYP.name, default=Pass,
                       cases={name: globals()["ASDU_%s" % name] for name in TYP.__members__}),
        ),
)


def exact_names(obj):
    # print("obj type=", type(obj))
    if obj.__class__.__name__ == "Struct":
        content = Container()
        # print("obj.subcons=", obj.subcons)
        for sub_con in obj.subcons:
            if sub_con is not None:
                # print("sub_con=", sub_con)
                content.update(exact_names(sub_con))
        return content
        # return content if obj.name is None else {obj.name: content}

    elif obj.__class__.__name__ in ("Restream", "Reconfig", "Buffered"):
        return exact_names(obj.subcon)

    return {} if obj.name is None else \
        {obj.name: datetime.datetime.now() if obj.name in ("CP56Time2a", "CP24Time2a") else 0}


def init_frame(cls, apci1=None, apci2=None, TYP=None, cause=Cause.unused,
               SQ_COUNT=1, SQ=0):
    cc = Container(APCI1=apci1, APCI2=apci2, length=0, ASDU=None)
    if TYP is not None:
        cc.ASDU = Container(TYP=TYP, SQ=SQ, SQ_COUNT=SQ_COUNT, T=0, PN=0, SourceAddress=0, StartAddress=0, Cause=cause,
                            GlobalAddress=1, data=list())
        for num in range(cc.ASDU.SQ_COUNT):
            cc.ASDU.data.append(exact_names(globals()["ASDU_" + TYP.name]))
    return cc


def build_isu(cls, obj):
    build_bin = iec_104.build(obj)
    return b"\x68%c%b" % (len(build_bin) - 2, build_bin[2:])


iec_head = Struct(
        "iec104_head",
        Magic(b"\x68"),
        Byte("length"),  # 帧长度（不算帧头和长度，总帧长=length+2）
)

iec_104 = Struct(
        "iec104",
        Magic(b"\x68"),
        Byte("length"),
        ExprAdapter(
                ULInt16("APCI1"),
                encoder=lambda obj, ctx: obj if isinstance(obj, UFrame) else 1 if obj == "S" else obj << 1,
                decoder=lambda obj, ctx: obj >> 1 if obj & 1 == 0 else "S" if obj & 3 == 1 else UFrame(obj),
        ),
        ExprAdapter(
                ULInt16("APCI2"),
                encoder=lambda obj, ctx: 0 if obj is None else obj << 1,
                decoder=lambda obj, ctx: obj >> 1,
        ),
        # 只有I帧包含ASDU部分，S和U帧没有
        If(lambda ctx: not isinstance(ctx.APCI1, UFrame) and ctx.APCI1 != 'S',
           ASDU_Part),
)

setattr(Struct, "init_frame", classmethod(init_frame))
setattr(Struct, "build_isu", classmethod(build_isu))
