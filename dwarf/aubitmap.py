#!/usr/bin/env python
#-*- coding: utf-8 -*-
#Bitmap
#Auth: Camel
#email: camelsky@gmail.com
import logging
from bitarray import bitdiff
from bitarray import bitarray
from datetime import date, datetime, timedelta


class Bitmap(bitarray):
    
    def __init__(self, arg=None, **kwargs):
        """
        baseBitarr([initial],[endian = 'big' or 'littel'])
        初始化对象
        """
        super(Bitmap, self).__init__(arg, **kwargs)
        self.base   = self
        # self.len    = self.base.length()
    

    def merge(self, *bitarrs):
        """
        合并运算，或运算
        返回合并后的对象
        merge(bitarray1,[bitarray2,bitarray3....])
        """
        for b in bitarrs:
            b = self._align_base(b)
            self.base |= b
        return self

    def filter(self, *bitarrs):
        """
        filter(bitarray,[bitarray1,...]) -> Bitmap
        过滤 base bitarr - 与运算
        返回过滤后的对象
        """
        for b in bitarrs:
            b = self._align_base(b)
            self.base &= b
        return self

    def anti(self, *bitarrs):
        """
        反向筛选 - self.base 和 bitarrs的取反 与运算 
        """
        for b in bitarrs:
            b = self._align_base(b)
            self.base &= ~b 
        return self

    def has_id(self, id):
        """
        bas_id(int) -> bool
        判断某个id在base bitarr中是否为真
        """
        return self.base[id]

    def retained_count(self, bitarrs):
        """
        retained_count(bitarray,[bitarray1,...]) -> list(int, int2,...)
        计算留存量
        返回在输入的各bitarrs中的留存量列表
        """
        return (self._and_count(bitarr) for bitarr in bitarrs)
    
    def _and(self, bitarr):
        """
        base bitarray 与输入的任意长度bitarray进行与运算
        """
        bitarr = self._align_base(bitarr)
        return self.base & bitarr

    def _and_count(self, bitarr):
        """
        计算base bitarray 按位与 bitarr 后的count 值
        """
        bitarr = self._align_base(bitarr)
        return (self.base & bitarr).count()
    
    def _align_base(self, b):
        """
        bitarray 长度对齐
        较短的bitarray对象在末位补0
        """
        base_len    = self.base.length()
        b_len       = b.length()
        diff        = base_len - b_len
        diffbarr    = bitarray(abs(diff))
        diffbarr.setall(False)
        if diff>0:
            b.extend(diffbarr)
        elif diff<0:
            self.base.extend(diffbarr)
        return b

    # def __repr__(self):
    #     string = self.base.__repr__()
    #     return string
