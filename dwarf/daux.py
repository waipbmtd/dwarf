#codding=utf-8
# create by Camel
# at 2014-07-01

import logging
import time
from datetime import datetime, timedelta

import dau
import util
import dauconfig
config = dauconfig

class AUstat(object):
    def __init__(self, baseday=None, cache_cli=None, filters=None):
        if not cache_cli:
            raise KeyError, 'have no cache server connection'
        self.baseDay = baseday
        self.cache_cli = cache_cli
        self.config  = dauconfig
        self.prefixs = self._keyprefix_chain()


    def _keyprefix_chain(self):
        plus = config.MAX_BITMAP_LENGTH % config.BITMAP_BLK_LEN and 1 or 0
        nodes = range(config.MAX_BITMAP_LENGTH/config.BITMAP_BLK_LEN + plus)
        pres  = []
        for node in nodes:
            if node:
                pres.append(config.BITMAP_BLK_PREFIX % node)
            else:
                pres.append('{key}')
        logging.debug(pres)
        return pres


    def _sum_result(self, rows):
        total  = 0
        totals = 0
        for item in rows:
            # print item
            if isinstance(item, long):
                total += item
            else:
                if totals:
                    totals = map(lambda x,y:[x[0],x[1]+y[1]], totals, item)
                else:
                    totals = item
        return total or totals

    def _define_config(self, pref):
        dau_keys_conf = dict()
        filter_keys_conf = dict()
        for k,v in self.config.dau_keys_conf.items():
            dau_keys_conf[k] = pref.format(key=v)
        for k,v in self.config.filter_keys_conf.items():
            filter_keys_conf[k] = pref.format(key=v)
        return auconfig(dau_keys_conf, filter_keys_conf)

    def _init_austats(self):
        for prefix in self.prefixs:
            _config = self._define_config(prefix)
            aus = dau.AUstat(self.baseDay, 
                redis_cli=self.cache_cli, config=_config)
            yield aus 

    def _call_austat(self, name, *args, **kwargs):
        for aus in self._init_austats():
            yield getattr(aus, name)(*args, **kwargs)
    

    def _func_bridge(self, name):
        fname =  name
        # try:
        #     getattr(dau.AUstat, name)
        # except Exception, e:
        #     raise e
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


class auconfig():
    DATETIME_FORMAT     = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT         = "%Y%m%d"
    MONTH_FORMAT        = "%Y%m"
    DATE_FORMAT_R       = '%Y-%m-%d'
    STD_OFFSET          = 0
    def __init__(self, dau_conf, filter_conf):
        self.dau_keys_conf    = dau_conf
        self.filter_keys_conf = filter_conf

    # def __getattr__(self, name):
    #     try:
    #         return getattr(dauconfig, name)
    #     except Exception, e:
    #         raise e
            
