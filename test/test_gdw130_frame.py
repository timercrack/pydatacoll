# coding=utf-8

import unittest
from coll_front.coll_protocol.GDW130.frame import *


head_str = b'\x68\x41\x00\x41\x00\x68'
ctrl_str = b'\xd0'
addr_str = b'\x32\x00\x00\x00\x03'
frame_str = b'\x68\x41\x00\x41\x00\x68' \
            b'\xc9\x82\x04\x01\x00\x00' \
            b'\x02\x70\x00\x00\x04\x00' \
            b'\x03\x01\x01\x00\xcb\x16'


class GDW130Test(unittest.TestCase):
    def test_parse_head(self):
        h = gdw_head.parse(head_str)
        print(h)
        h_str = gdw_head.build(h)
        self.assertEqual(h_str, head_str)

    def test_build_head(self):
        h = init_head(length=2)
        h_str = gdw_head.build(h)
        head = gdw_head.parse(h_str)
        h_str2 = gdw_head.build(head)
        self.assertEqual(h_str, h_str2)

    def test_parse_ctrl(self):
        ctl = gdw_ctrl.parse(ctrl_str)
        ctl_str = gdw_ctrl.build(ctl)
        self.assertEqual(ctl_str, ctrl_str)

    def test_build_ctrl(self):
        ctl = init_ctrl(Dir=1, Prm=0, Fcb=0, Acd=0, Fcv=1, Cfn=0, Qfn=0)
        ctl_str = gdw_ctrl.build(ctl)
        ctrl = gdw_ctrl.parse(ctl_str)
        ctl_str2 = gdw_ctrl.build(ctrl)
        self.assertEqual(ctl_str, ctl_str2)

    def test_parse_addr(self):
        address = gdw_addr.parse(addr_str)
        address_str = gdw_addr.build(address)
        self.assertEqual(address_str, addr_str)

    def test_build_addr(self):
        address = init_addr(a1=23, a2=0, MSA=1, grpAddrFlag=1)
        address_str = gdw_addr.build(address)
        addr = gdw_addr.parse(address_str)
        address_str2 = gdw_addr.build(addr)
        self.assertEqual(address_str, address_str2)

    def test_parse_frame(self):
        frm = gdw_130.parse_frame(frame_str)
        print(frm)
        frm_str = gdw_130.build_frame(frm)
        self.assertEqual(frame_str, frm_str)

    def test_build_frame(self):
        frame1 = gdw_130.init_frame()
        frm_str = gdw_130.build_frame(frame1)
        print(frm_str.hex())
        frame2 = gdw_130.parse_frame(frm_str)
        print(frame2)
        frm_str2 = gdw_130.build_frame(frame2)
        self.assertEqual(frm_str, frm_str2)
