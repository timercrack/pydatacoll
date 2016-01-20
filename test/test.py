from construct import *
from pydatacoll.protocols.iec104.frame import Cause

test = Struct(
    'ASDU',
    ExprAdapter(
        ULInt16('Cause'),
        encoder=lambda obj, ctx: (ctx.cot.SourceAddress << 8) + (ctx.cot.T << 7) + (ctx.cot.PN << 6) + ctx.cot.Cause,
        decoder=lambda obj, ctx: ctx.update(dict(
                SourceAddress=(obj & 0xff00) >> 8,  # 源发站地址 0 缺省值 1~255 源发站地址号
                T=(obj & 0x80) >> 7,  # 试验 0 未试验 1 试验
                PN=(obj & 0x40) >> 6,  # 0 肯定确认 1 否定确认
                Cause=Cause(obj & 0x1f))),  # 传送原因
    ),
)

if __name__ == '__main__':
    # c = Container(cot=1, SourceAddress=0xab, T=1, PN=1, Cause=5)
    # c.abc = 1
    # print(c)
    # build = test.build(c)
    build = b"\xab\xcd"
    print(build.hex())
    frame = test.parse(build)
    print(frame)
