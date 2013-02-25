#!/usr/bin/env python
#-*- coding: utf-8 -*-
#handle the logs file
#auth: Camelluo

import os, sys
import redis
import tornado
import time
from datetime import date, datetime, timedelta
from tornado.options import options, define

os.sys.path.append('../')

from config import *
import storage.redis

def scan(from_date, to_date, port='9022'):
    """
    扫瞄from_date 到 to_date 之间的request日志
    将每日访问用户id映射入bitmap
    """
    print from_date, to_date
    days        = (to_date-from_date).days+1
    dateList    = [from_date+timedelta(v) for v in range(days)] 
    logFiles    = [LOGFILENAME_FORMAT.format(port=port, date=v.strftime(DATE_FORMAT)) for v in dateList]
    useridFiles = [USERFILENAME_FORMAT.format(port=port, date=v.strftime(DATE_FORMAT)) for v in dateList]
    map(makeUseridList, logFiles, useridFiles)
    map(saveDailyActiveUser, dateList, useridFiles)

def makeUseridList(logfile, targetfile):
    """
    从日志中提取唯一用户id
    """
    logfile     = os.path.join(LOGFILE_DIRECT, logfile)
    targetfile  = os.path.join(TAR_DIRECT, targetfile)
    cmd         = SHELL_FETCHUSERID.format(log_file=logfile, target_file=targetfile)
    print cmd
    os.system(cmd)
    return 1

def saveDailyActiveUser(date, useridfile):
    """"
    保存独立访问用户数据
    """
    filename = os.path.join(TAR_DIRECT, useridfile)
    print filename
    f = open(filename, 'r')
    for line in f.readlines():
        line = line.replace("\n","")
        line = line.replace("\r","")
        if line == "":
            continue
        # print line
        try:
            userid = int(line)
            if userid > 20000000:
                continue
            markActiveUserid(date.strftime(DATE_FORMAT), userid)
        except Exception, e:
            print e, line

def markActiveUserid(date, userid):
    redis_cli = get_redis_client()
    reKey     = DAU_KEY.format(date=date)
    redis_cli.setbit(reKey, int(userid), 1)
    print reKey, int(userid), 1

def get_redis_client():
    return storage.redis.get_redis_client(host=redis_host,port=redis_port,db=redis_db)
    # if not get_redis_client.__dict__.get('redis_cli'):
    #     get_redis_client.__dict__['redis_cli'] = redis.Redis(host=redis_host,port=redis_port,db=redis_db)
    # return get_redis_client.__dict__.get('redis_cli')

def run():
    define("f", default=None)
    define("t", default=None)
    define("port", default='9022')
    tornado.options.parse_command_line()

    #计算扫瞄日至的时间范围
    Today_date      = date.today()
    Yesterday_date  = date.today()-timedelta(days=1)
    sToday_date     = Today_date.strftime(DATE_FORMAT)
    sYesterday_date = Yesterday_date.strftime(DATE_FORMAT)
    if not options.f : 
        options.f = sYesterday_date
    if not options.t :
        options.t = sYesterday_date

    try:
        from_date   = datetime.strptime(options.f, DATE_FORMAT)
        to_date     = datetime.strptime(options.t, DATE_FORMAT)
    except ValueError, e:
        raise e

    scan(from_date, to_date, options.port)
    pass

if __name__ == '__main__':
    start   = time.time()
    run()
    end     = time.time()
    print end-start, "sec"
