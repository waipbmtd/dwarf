#!/usr/bin/env python
#coding: utf-8
#name: Mapping the newuserid and filter
#by Camelsky 2013-02-20

try:
    import MySQLdb
    import redis
except:
    print 'Need python modules \'redis\' and \'MySQLdb\' but no found! '
    exit(1)
import tornado
from tornado import database
from tornado.options import options, define
from datetime import date, datetime, timedelta
import time
import dauconfig
import dwarf.dau
import logging

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



def get_mysql():
    conf = config.mysql_conf
    conn = database.Connection(
        host="%s:%s" % (conf['host'], conf['port']), 
        database=conf['db'], 
        user=conf['user'], 
        password=conf['passwd'], 
    )
    return conn

def get_redis_client(pipe=False):
    conf = config.redis_conf
    try:
        if pipe:
            conn = redisPipeline(conf)
        else:
            conn = redis.Redis(**conf)
        return conn
    except Exception, e:
        print "redis connection Error!", e
        raise e


def getNewUserid(date, mysql_conn):
    sql  = "select id from user where create_time > %s order by id limit 1"
    conn = mysql_conn
    if not mysql_conn:
        conn = get_mysql()
    try:
        logging.debug(sql % date) 
        ret = conn.get(sql, date)
        if ret:
            uid = ret['id']
            return uid
        else:
            return 0
    except Exception, e:
        raise e
    finally:
        conn.close()

def getGenderUserid(from_date=None, mysql_conn=None):
    conn = mysql_conn
    if not mysql_conn:
        conn = get_mysql()
    sql     = "select id , gender from user %(where)s "
    where   = ""
    if from_date:
        if not where:
            where += "where "
        where += "create_time > %s"
        sql = sql % {'where' : where}
        print sql % {'date':from_date}
    ret = conn.iter(sql, from_date)
    return ret

def getRegUser(from_date=None, mysql_conn=None):
    conn = mysql_conn
    sql = "select user_id from user_statics where userinfo_status = 2"
    return conn.iter(sql)

def DateList(from_date, to_date):
    days        = (to_date-from_date).days+1
    dateList    = [from_date+timedelta(v) for v in range(days)] 
    return dateList    

def idList(dates, mysql_conn):
    for date in dates:
        yield getNewUserid(date, mysql_conn)


def mapNewUserid(dates, userids):
    auRecord = dwarf.dau.AUrecord(get_redis_client())
    map(auRecord.saveNewUserIndex, dates, userids)

# def mapFilterid(filtername, data):
#     auRecord = dwarf.dau.AUrecord(get_redis_client())
#     for cls, userid in data:
#         auRecord.mapFilter(filtername, cls, userid) 

def run():
    define("f", default=None)
    define("t", default=None)
    define("do", help="what need to do? \'nu\'(new user) or \'all\'",default='nu')
    tornado.options.parse_command_line()

    #计算时间范围
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
    
    mysql_conn = get_mysql()
    if options.do == 'all':
        auRecord = dwarf.dau.AUrecord(get_redis_client(pipe=True))
        genders = getGenderUserid(from_date, mysql_conn)
        filtername = 'gender'
        for v in genders:
            auRecord.mapFilter('gender', v.gender, v.id)
        count = 0 
        for v in getRegUser(from_date, mysql_conn):
            auRecord.mapFilter('regu', 1, v.user_id)
            count += 1
            if count & 0xFF == 0:
                print count 
    elif options.do == 'nu':
        auRecord = dwarf.dau.AUrecord(get_redis_client())
        dates = DateList(from_date, to_date)
        users = idList(dates, mysql_conn)
        # print users
        map(auRecord.saveNewUserIndex, dates, users)
        # print zip(dates, users)

if __name__ == '__main__':
    s = time.time()
    run()
    e = time.time()
    print "Elapes:",e-s,"sec"
