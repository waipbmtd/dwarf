#coding:utf-8

import time
import logging
from datetime import date, datetime, timedelta

import tornado

from dwarf.daux import AUrecord
from tornado.options import options, define
import redis
import dauconfig

config = dauconfig

class redisPipeline:
    conn = None
    count = 0
    def __init__(self, conf):
        r = redis.Redis(host=conf['host'], port=conf['port'], db=conf['db'])
        self.conn = r.pipeline()
    def __del__(self):
        self._save()
        
    def _save(self):
        self.conn.execute()
        
    def setbit(self, reKey, offset, identfy=1):
        self.conn.setbit(reKey, offset, identfy)
        self.count += 1
        if self.count & 0xFF == 0:
            self._save()

def get_redis_client(pipe=False):
    conf = config.redis_conf
    try:
        if pipe:
            conn = redisPipeline(conf)
        else:
            conn = redis.Redis(**conf)
        return conn
    except Exception, e:
        raise e

def markActiveUserid(date, userid, redis_cli):
    # redis_cli = get_redis_client()
    auRecord = AUrecord(redis_cli)
    if auRecord.mapActiveUserid(date,userid):
        print date, userid

def markNewUserid(date, userid, redis_cli):
    ar = AUrecord(redis_cli)
    ar.mapNewUser(date, userid)
    ar.saveNewUserIndex(date, userid)

def markFilter(filtername, filterclass, userid, redis_cli):
    ar = AUrecord(redis_cli)
    ar.mapNewUser(date, userid)

def run():
    define("f", default=None)
    define("t", default=None)
    define("uid", default="")
    tornado.options.parse_command_line()

    #计算扫瞄日志的时间范围
    Today_date      = date.today()
    Yesterday_date  = date.today()-timedelta(days=1)
    sToday_date     = Today_date.strftime(config.DATE_FORMAT)
    sYesterday_date = Yesterday_date.strftime(config.DATE_FORMAT)
    if not options.f : 
        options.f = sYesterday_date
    if not options.t :
        options.t = sYesterday_date

    try:
        from_date   = datetime.strptime(options.f, config.DATE_FORMAT)
        to_date     = datetime.strptime(options.t, config.DATE_FORMAT)
    except ValueError, e:
        raise e

    uid = options.uid

    markActiveUserid(from_date, uid, get_redis_client())
    markNewUserid(from_date, uid, get_redis_client())

if __name__ == '__main__':
    run()