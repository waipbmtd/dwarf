#!/usr/bin/env python
#-*- coding: utf-8 -*-

DATETIME_FORMAT     = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT         = "%Y%m%d"
MONTH_FORMAT        = "%Y%m"
DATE_FORMAT_R       = '%Y-%m-%d'
STD_OFFSET          = 0
MAX_BITMAP_LENGTH   = 1000*1000*200
BITMAP_BLK_LEN      = 1000*1000*20
BITMAP_BLK_PREFIX   = "%d-{key}"


redis_conf = dict(
    host  = "127.0.0.1",
    port  = 6379,
    db    = 15,
    # max_connection = 1,
)

logfile_conf = dict(
    dir         = "/Users/Camel/snsLog/",
    name_format = "snsInfo-{date}.log.gz",
)

dau_keys_conf = dict(
    dau     = "sDau:{date}",
    newuser = "hNewUser",
    mau     = "sMau:{month}",
    dnu   = "sDnu:{date}",
    mnu   = "sMnu:{month}",
)

filter_keys_conf = dict(
    gender      = "sFgender:{gender}",
    platform    = "sFplatform:{platform}",
    version     = "sFversion:{version}",
    channel     = "sFchannel:{channel}",
    regu        = "sFregu:{regu}",
)

mysql_conf = dict(
    host    = '127.0.0.1',
    port    = 3306,
    db      = 'dbname',
    user    = 'username',
    passwd  = 'password',
    charset = 'utf8',
)