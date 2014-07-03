# coding:utf-8
# create by Camel
# at 2014-07-01

import logging
import time
from datetime import datetime, timedelta

import dau
import util
import dauconfig
config = dauconfig

class dauxBase(object):

    def _keyprefix_chain(self):
        '''
        生成区间数据前缀列表
        '''
        plus = config.MAX_BITMAP_LENGTH % config.BITMAP_BLK_LEN and 1 or 0
        nodes = range(config.MAX_BITMAP_LENGTH/config.BITMAP_BLK_LEN + plus)
        pres  = []
        for node in nodes:
            if node:
                pres.append(config.BITMAP_BLK_PREFIX % node)
            else:
                pres.append('{key}')
        logging.info(len(pres))
        return pres

    def _define_config(self, pref):
        '''
        根据统计区间返回区间配置
        '''
        dau_keys_conf = dict()
        filter_keys_conf = dict()
        for k,v in self.config.dau_keys_conf.items():
            dau_keys_conf[k] = pref.format(key=v)
        for k,v in self.config.filter_keys_conf.items():
            filter_keys_conf[k] = pref.format(key=v)
        return auconfig(dau_keys_conf, filter_keys_conf)

    def uid_pos(self, uid):
        pass

class AUstat(dauxBase):
    
    def __init__(self, baseday=None, cache_cli=None, filters=None):
        if not cache_cli:
            raise KeyError, 'have no cache server connection'
        self.baseDay = baseday
        self.cache_cli = cache_cli
        self.config  = dauconfig
        self.prefixs = self._keyprefix_chain()
        self.filters = filters

    def _sum_result(self, rows):
        '''
        将分区间返回结果累加得到汇总值
        _sum_result([[i1,i3],[i2,i4]]) -> [[i1+i2,i3+i4]]
        _sum_result([[(date1,i1),(date2,i3)],[(date1,i2),(date2,i4)]] -> 
            [[date1,i1+i2],[date2,i3+i4]]
        '''
        total  = 0
        totals = 0
        for item in rows:
            # logging.info(item)
            if isinstance(item, long):
                total += item
            else:
                if totals:
                    totals = map(lambda x,y:[x[0],x[1]+y[1]], totals, item)
                else:
                    totals = item
        return total or totals


    def _init_austats(self):
        '''
        实例化 dau.AUstat 并返回
        '''
        for prefix in self.prefixs:
            _config = self._define_config(prefix)
            filters = self.filters
            if self.filters:
                filters = self.filters.ins_filter(_config=_config)
                # logging.info('%s %s', prefix, filters.count())
            aus = dau.AUstat(self.baseDay, 
                redis_cli=self.cache_cli, filters=filters,
                config=_config)
            yield aus 

    def _call_austat(self, name, *args, **kwargs):
        for aus in self._init_austats():
            yield getattr(aus, name)(*args, **kwargs)
    

    def _func_bridge(self, name):
        def func(*args, **kwargs):
            rows = self._call_austat(name, *args, **kwargs)
            ret = self._sum_result(rows)
            return ret 
        return func 


    def __getattr__(self, name):
        try:
            return self._func_bridge(name)
        except Exception:
            raise KeyError, "'%s' is not defined" % name


class Filter(dauxBase):
    """
    dau.Filter 的区间管理
    """

    def __init__(self, cache_cli):
        if not cache_cli:
            raise KeyError, 'have no cache service connection'
        self.cache_cli = cache_cli
        self.expands = []
        self.overlaps = []
        self.operation_inner = []
        self.operation_ext   = []

    def expand(self, **kwargs):
        self.operation_inner.append(('expand',kwargs))
        return self

    def overlap(self, **kwargs):
        self.operation_inner.append(('overlap',kwargs))
        return self

    def _ins_filter(self, _config):
        ft = dau.Filter(config=_config)
        for item in self.operation_inner:
            getattr(ft, item[0])(self.cache_cli,**item[1])
        # logging.info(ft.count())
        return ft

    def ins_filter(self, _config):
        ft = self._ins_filter(_config)
        for item in self.operation_ext:
            getattr(ft, item[0])(item[1]._ins_filter(_config)) 
        # logging.info(ft.count())
        return ft

    def filter(self, ff):
        self.operation_ext.append(('filter', ff))
        return self

    def merge(self, ff):
        self.operation_ext.append(('merge', ff))
        return self

class auconfig():
    DATETIME_FORMAT     = dauconfig.DATETIME_FORMAT
    DATE_FORMAT         = dauconfig.DATE_FORMAT
    MONTH_FORMAT        = dauconfig.MONTH_FORMAT
    DATE_FORMAT_R       = dauconfig.DATE_FORMAT_R
    STD_OFFSET          = dauconfig.STD_OFFSET
    def __init__(self, dau_conf, filter_conf):
        self.dau_keys_conf    = dau_conf
        self.filter_keys_conf = filter_conf

            
