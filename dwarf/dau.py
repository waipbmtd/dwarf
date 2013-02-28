#!/usr/bin/env python
#-*- coding:utf-8 -*-
# get dau

import sys, os
import redis
import time
import logging
from bitarray import bitarray
from datetime import datetime, date, timedelta
from tornado.options import define, options

try:
    import dauconfig
except ImportError:
    raise ImportError, "Configure file 'config.py' was not found"
from aubitmap import Bitmap

config = dauconfig

class AUstat():

    def __init__(self, baseday=None, redis_cli=None, filters=None):
        if not redis_cli:
            print('Need redis connection but not have')
            raise KeyError,'Redis connection not found'
        self.REDIS          = redis_cli
        self.baseDay        = baseday
        self.baseBitmap     = self._make_bitmap(baseday, filters) 
        self.newUserBitmap  = self.get_newuser_bitmap(baseday, filters)


    def _list_day(self, fday=None, tday=None):
        """
        return the date string list from fday to tday
        """
        if not tday:
            tday    = date.today().strftime(config.DATE_FORMAT)
        if not fday:
            fday    = date.today().strftime(config.DATE_FORMAT)
        
        date_fday   = date.today()
        date_tday   = date.today()
        try:
            date_fday   = fday
            date_tday   = tday
        except Exception, e:
            return False
        days        = (date_tday - date_fday).days+1
        dayList     = [date_fday+timedelta(v) for v in range(days)] 
        return dayList        

    def _make_bitmap(self, day=None, filters=None):
        """
        initial and return dwarf.Bitmap object
        """
        dauBitmap  = Bitmap()
        if day:
            DAU_KEY  = config.dau_keys_conf['dau']
            dauKey   = DAU_KEY.format(date=day.strftime(config.DATE_FORMAT))
            bitsDau  = self.REDIS.get(dauKey)
            if bitsDau:
                dauBitmap.frombytes(bitsDau)
        if filters:
            dauBitmap.filter(filters)
        return dauBitmap

    def get_newuser_bitmap(self, day=None, filters=None):
        """
        返回新用户当日登陆记录bitmap对象
        """
        dauBitmap = Bitmap()
        if day:
            offsets = self.REDIS.hget(config.dau_keys_conf['newuser'], 
                day.strftime(config.DATE_FORMAT))
            if not offsets:
                return dauBitmap
            offsets = int(offsets)
            dauBitmap = self._make_bitmap(day)
        else:
            dauBitmap = self.baseBitmap
        
        retBitmap = Bitmap(offsets*bitarray('0') + dauBitmap[offsets:])
        if filters:
            retBitmap.filter(filters)
        return retBitmap

    def get_dau(self, day=None):
        """
        return the dau number
        if day is provided then return the dau of the day
        """
        if not day:
            return self.baseBitmap.count()    
        return self._make_bitmap(day).count()

    def get_dnu(self, day=None):
        """
        return the daily count of new user number
        """
        if not day:
            return self.newUserBitmap.count()    
        return self.get_newuser_bitmap(day).count()

    def list_dau(self, fday=None, tday=None):
        """
        return a list of daily active user's number from fday to tday
        list_dau(string, string) -> [(date1, number),(date2, number)....]
        """
        dayList = self._list_day(fday, tday)
        return zip(dayList, map(self.get_dau,dayList))

    def list_dnu(self, fday=None, tday=None):
        """
        return a list of daily new user count from fday to tday
        list_dnu(string, string) -> [(date1, number),(date2, number)....]
        """
        dayList = self._list_day(fday, tday)
        return zip(dayList, map(self.get_dnu,dayList))

    def mau(self, fday=None, tday=None):
        """
        return the Merged Active User number
        """
        dayList = self._list_day(fday, tday)
        BaseBM  = self._make_bitmap()
        for day in dayList:
            BaseBM.merge(self._make_bitmap(day))
        return BaseBM.count()

    def get_30days_mau(self):
        """
        return the merged active user number in the last 30 days before
        self.baseDay
        """
        fday = self.baseDay-timedelta(days=30)
        return self.mau(fday=fday, tday=self.baseDay)

    def get_7days_mau(self):
        """
        return the last 7 days' active user number before baseDay
        """
        fday = self.baseDay-timedelta(days=7)
        return self.mau(fday=fday, tday=self.baseDay)

    def retained(self, day):
        """
        return the retained user number of baseDay in the givend day
        """
        bm  = self._make_bitmap(day)
        return self.baseBitmap.retained(bm)

    def get_retained(self, fday=None, tday=None):
        """
        return the list of daily retained number from fday to tday
        """
        dayList = self._list_day(fday, tday)
        return zip(dayList, 
            self.baseBitmap.retained_count(
                *[self._make_bitmap(day) for day in dayList]
                )
            )

    def get_retained_nu(self, fday=None, tday=None):
        """
        return the list of newuser retained number and the date string
        """
        dayList = self._list_day(fday, tday)
        return zip(dayList, 
            self.newUserBitmap.retained_count(
                *[self._make_bitmap(day) for day in dayList]
                )
            )

class Filter(Bitmap):
    """
    Generate AU filter object
    Need redis db to fetch the filter data
    """
    # def __ini__(self, *args, **kwargs):
    #     super(Filter, self).__init__(*args, **kwargs)
    #     return self

    def expand(self, redis_cli, **kwargs):
        for k,v in kwargs.items():
            fBm = self._get_filtet_bimap(redis_cli, k, v)
            self.merge(fBm)
        return self

    def overlap(self, redis_cli, **kwargs):
        for k,v in kwargs.items():
            fBm = self._get_filtet_bimap(redis_cli, k, v)
            self.filter(fBm)
        return self

    def _get_filtet_bimap(self, redis_cli, filtername, filterclass):
        if not isinstance(redis_cli, redis.client.Redis):
            raise TypeError, "Need redis connection but not found"
        fKey_format = config.filter_keys_conf.get(filtername)
        if not fKey_format:
            raise ValueError, "Can not find the key \'%s\' in config.filter_keys_conf" % k
        print fKey_format,filtername,filterclass
        fKey  = fKey_format.format(**{filtername:filterclass})
        fBits = redis_cli.get(fKey)
        fBm   = Bitmap()
        fBm.frombytes(fBits)
        print "fBm count:", fBm.count()
        return fBm


class AUrecord():
    """
    Do record active user's map in given redis db
    """

    def __init__(self, redis_cli):
        if not redis_cli:
            raise ValueError , "Need redis client but not found!"
        self.redis = redis_cli

    def mapActiveUserid(self, date, userid):
        """
        Record the active userid
        """
        sDate     = date.strftime(config.DATE_FORMAT)
        reKey     = config.dau_keys_conf['dau'].format(date=sDate)
        redis_cli = self.get_redis_cli()
        offset    = int(userid)
        if offset > 0 and offset <= config.MAX_BITMAP_LENGTH:
            redis_cli.setbit(reKey, offset, 1)
            logging.debug('Save auid in redis by setbit %s %d' % (reKey, offset)) 
            return 1

    def mapActiveUseridbyByte(self, date, bytes):
        """
        Save Active User map by byte data
        """
        sDate     = date.strftime(config.DATE_FORMAT)
        reKey     = config.dau_keys_conf['dau'].format(date=sDate)
        redis_cli = self.get_redis_cli()
        logging.debug('Save dau bytes: %s' % reKey)
        redis_cli.set(reKey, bytes)
        return


    def saveNewUserIndex(self, date, userid):
        """
        Save new user id to redis
        """
        sdate     = date.strftime(config.DATE_FORMAT)
        rKey      = config.dau_keys_conf['newuser']
        redis_cli = self.get_redis_cli()
        rVar      = int(userid)
        if rVar > 0 and rVar <=  config.MAX_BITMAP_LENGTH:
            redis_cli.hset(rKey, sdate, rVar)
            logging.debug('save newuser id>> hset %s %s %s' % (rKey, sdate, rVar))


    def mapFilter(self, filtername, filterclass, userid):
        """
        Save userid map in filter
        """
        redis_cli = self.get_redis_cli()
        f_conf    = config.filter_keys_conf
        rKey      = f_conf[filtername].format(**{filtername:filterclass})
        if not rKey:
            raise ValueError, "Haven't %s filter keys" % filtername
        offset    = int(userid)
        if offset>0 and offset <= config.MAX_BITMAP_LENGTH:
            redis_cli.setbit(rKey, offset, 1)
            logging.debug('Save auid in redis by setbit %s %d 1' % (rKey, offset)) 
            return 1


    def get_redis_cli(self):
        return self.redis

