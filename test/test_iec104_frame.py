import unittest

from pydatacoll.protocols.iec104.frame import *

soe_bin = b"\x68\x15\x1a\x00\x06\x00\x1e\x01\x03\x00\x01\x00\x08\x00\x00\x00\xad\x39\x1c\x10\xda\x0b\x05"
i_bin = b"\x68\x0e\xe8\x00\x06\x00\x65\x01\x0a\x00\x01\x00\x00\x00\x00\x05"
s_bin = b"\x68\x04\x01\x00\x94\x00"
u_bin = b"\x68\x04\x07\x00\x00\x00"
i_big = b"\x68\xfa\xd2\x00\x06\x00\x0f\x1e\x25\x00\x01\x00\xd3\x64\x00\x1e\x00\x00\x00\x00\xd4\x64\x00\xf0\x05\x00" \
        b"\x00\x01\xd5\x64\x00\x00\x00\x00\x00\x02\xd6\x64\x00\x00\x00\x00\x00\x03\xd7\x64\x00\x9a\xde\xa4\x00\x04" \
        b"\xd8\x64\x00\x98\x30\x00\x00\x05\xd9\x64\x00\x0a\x00\x00\x00\x06\xda\x64\x00\x00\x00\x00\x00\x07\xdb\x64" \
        b"\x00\x6e\x00\x00\x00\x08\xdc\x64\x00\x00\x00\x00\x00\x09\xdd\x64\x00\xbe\x0d\x34\x00\x0a\xde\x64\x00\xae" \
        b"\x01\x00\x00\x0b\xdf\x64\x00\xda\x1d\x03\x00\x0c\xe0\x64\x00\x00\x00\x00\x00\x0d\xe1\x64\x00\x94\x6e\x02" \
        b"\x00\x0e\xe2\x64\x00\x0c\x49\x00\x00\x0f\xe3\x64\x00\xdc\xf7\x21\x00\x10\xe4\x64\x00\x0a\x00\x00\x00\x11" \
        b"\xe5\x64\x00\x7c\x3d\x0a\x00\x12\xe6\x64\x00\x00\x00\x00\x00\x13\xe7\x64\x00\x94\x0c\x00\x00\x14\xe8\x64" \
        b"\x00\x00\x00\x00\x00\x15\xe9\x64\x00\x72\x15\x00\x00\x16\xea\x64\x00\x1e\x00\x00\x00\x17\xeb\x64\x00\x00" \
        b"\x00\x00\x00\x18\xec\x64\x00\x0a\x00\x00\x00\x19\xed\x64\x00\x00\x00\x00\x00\x1a\xee\x64\x00\x00\x00\x00" \
        b"\x00\x1b\xef\x64\x00\xb4\x1c\x4d\x00\x1c\xf0\x64\x00\xb6\xeb\x03\x00\x1d"


class IEC104Test(unittest.TestCase):
    def test_M_SP_NA_1(self):
        frame = iec_104.init_frame(1, 2, TYP.M_SP_NA_1, Cause.introgen)
        # print('after init=', frame)
        frame.ASDU.StartAddress = 10
        frame.ASDU.data[0].address = 5
        frame.ASDU.data[0].value = 1
        # print('frame=', frame)
        build = iec_104.build_isu(frame)
        # print('build=', build.hex())
        parse = iec_104.parse(build)
        # print('parse=', parse)
        rebuild = iec_104.build_isu(parse)
        # print('rebuild=', rebuild.hex())
        self.assertEqual(build, rebuild)
        self.assertEqual(frame.ASDU.data[0].value, parse.ASDU.data[0].value)

    def test_parse_u(self):
        frame = iec_104.parse(u_bin)
        # print "parsed Frame=%s" % frame
        self.assertEqual(frame.APCI1, UFrame.STARTDT_ACT)

    def test_parse_s(self):
        frame = iec_104.parse(s_bin)
        # print "parsed Frame=%s" % frame
        self.assertEqual(frame.APCI1, "S")

    def test_parse_i(self):
        frame = iec_104.parse(i_bin)
        # print("parsed Frame=%s" % frame)
        self.assertEqual(frame.ASDU.TYP, TYP.C_CI_NA_1)

    def test_build_u(self):
        c = iec_104.init_frame(UFrame.STARTDT_ACT)
        frame = iec_104.build_isu(c)
        # print "built U frame=%s" % frame.encode('hex')
        self.assertEqual(frame, u_bin)

    def test_build_s(self):
        c = iec_104.init_frame("S", 74)
        frame = iec_104.build_isu(c)
        # print "built S frame=%s" % frame.encode('hex')
        self.assertEqual(frame, s_bin)

    def test_build_C_CI_NA1(self):
        c = iec_104.init_frame(116, 3, TYP.C_CI_NA_1)
        c.ASDU.SourceAddress = 40
        c.ASDU.GlobalAddress = 1
        c.ASDU.data[0].RQT = 5
        frame = iec_104.build_isu(c)  # 使用 build_isu 组装I帧
        # print "built C_CI_NA1 frame=%s" % frame.encode('hex')
        self.assertEqual(frame, i_bin)

    def test_build_C_SE_TC_1(self):
        c = iec_104.init_frame(105, 3, TYP.C_SE_TC_1)
        frame = iec_104.build_isu(c)
        # print "built C_SE_TC_1 frame=%s" % frame.encode('hex')
        parse = iec_104.parse(frame)
        re_build = iec_104.build_isu(parse)
        # print "rebuilt C_SE_TC_1 frame=%s" % re_build.encode('hex')
        self.assertEqual(frame, re_build)

    def test_M_SP_TB_1(self):
        time = datetime.datetime(2005, 11, 26, 16, 28, 14, 765000)
        frame = iec_104.parse(soe_bin)
        # print("parse=", frame)
        self.assertEqual(frame.ASDU.data[0].cp56time2a, time)
        frame = iec_104.init_frame(13, 3, TYP.M_SP_TB_1)
        frame.ASDU.SourceAddress = 12
        frame.ASDU.GlobalAddress = 1
        frame.ASDU.data[0].address = 8
        frame.ASDU.data[0].cp56time2a = time
        build = iec_104.build_isu(frame)
        # print("build=", build.hex())
        # print("soe_bin=", soe_bin.hex())
        self.assertEqual(build, soe_bin)

    def test_parse_big(self):
        frame = iec_104.parse(i_big)
        # print "parsed Frame=%s" % frame
        self.assertEqual(frame.ASDU.TYP, TYP.M_IT_NA_1)

    def test_build_big(self):
        # print 'before=%s' % i_big.encode('hex')
        frame = iec_104.parse(i_big)
        # print('parse=', frame)
        c = iec_104.init_frame(frame.APCI1, frame.APCI2, frame.ASDU.TYP, sq_count=frame.ASDU.sq_count)
        c.ASDU.SourceAddress = frame.ASDU.SourceAddress
        c.ASDU.GlobalAddress = frame.ASDU.GlobalAddress
        for idx in range(frame.ASDU.sq_count):
            c.ASDU.data[idx].address = frame.ASDU.data[idx].address
            c.ASDU.data[idx].value = frame.ASDU.data[idx].value
            c.ASDU.data[idx].sq = frame.ASDU.data[idx].sq
        # print('parse2=', c)
        re_build = iec_104.build_isu(c)  # 使用 build_isu 组装I帧
        # print "built big frame=%s" % re_build.encode('hex')
        self.assertEqual(re_build, i_big)

    def test_build_asdu(self):
        cc = Container(TYP=TYP.C_CI_NA_1, sq=0, sq_count=1, T=0, PN=0, SourceAddress=0,
                       StartAddress=0, Cause=Cause.actcon, GlobalAddress=1)
        cc.data = [Container(FRZ=0, RQT=0, address=1)]
        frame = ASDU_Part.build(cc)
        # print "built ASDU frame=%s" % frame.encode('hex')
        parse = ASDU_Part.parse(frame)
        re_build = ASDU_Part.build(parse)
        # print "rebuilt ASDU frame=%s" % re_build.encode('hex')
        self.assertEqual(frame, re_build)

    def test_parse_bad(self):
        with self.assertRaises(Exception):
            iec_104.parse("\x11\x22\x33")

    def test_build_bad(self):
        with self.assertRaises(Exception):
            cc = Container(TYP=TYP.C_CI_NA_1)
            iec_104.build_isu(cc)

    def test_continuous(self):
        c = iec_104.init_frame(166, 7, TYP.M_IT_NA_1, sq_count=5, sq=1)
        c.ASDU.StartAddress = 50
        for idx in range(5):
            c.ASDU.data[idx].value = idx+100
            c.ASDU.data[idx].sq = idx
        # print("before build=", c)
        build = iec_104.build_isu(c)  # 使用 build_isu 组装I帧
        # print("build=", build.hex())
        parse = iec_104.parse(build)
        # print("parse=", parse)
        re_build = iec_104.build(parse)
        # print("re_build=", re_build.hex())
        self.assertEqual(re_build, build)
