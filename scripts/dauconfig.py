#!/usr/bin/env python
#-*- coding: utf-8 -*-

DATETIME_FORMAT     = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT         = "%Y%m%d"
STD_OFFSET          = 10000
MAX_BITMAP_LENGTH   = 20000000


redis_conf = dict(
    host  = "127.0.0.1",
    port  = 6379,
    db    = 15,
    # max_connection = 1,
)

logfile_conf = dict(
    dir         = "/Users/Camel/snsLog/",
    name_format = "snsInfo-{date}.log.gz",
    stdout_sh   = "cat {filename}",
)

dau_keys_conf = dict(
    dau     = "sDau:{date}",
    newuser = "hNewUser",
)

filter_keys_conf = dict(
    gender      = "sFgender:{gender}",
    platform    = "sFplatform:{platform}",
    version     = "sFversion:{version}",
    channel     = "sFchannel:{channel}",
    regu        = "sFregu",
)

mysql_conf = dict(
    host    = '192.168.1.207',
    port    = 3306,
    db      = 'uplusmain',
    user    = 'moplus',
    passwd  = 'Wd36sRpt182jENTTGxVf',
    charset = 'utf8',
)