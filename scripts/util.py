#!/usr/bin/env python
#coding: utf-8
#Name: util
#Descript: util package
from datetime import datetime, date, timedelta
import calendar


def splitUa(ua="-"):
    "拆分UA"
    if ua == None or ua == '-' : return dict(channle = '',
        platform = '',
        model = '',
        )
    ua = ua.lower()
    uaDetail = {}
    
    if ua.find('#') >=0 :
        ua = ua[:ua.find('#')].replace("-","_") + ua[ua.find('#'):]
    
    # 处理特殊的android 机型ua
    def _addAndroid(ua):
        index = ua.find('-') 
        if index < 0 :
            index = len(ua)
        ua = ua[:index]+"-android"+ua[index:]
        return ua

    # 处理特殊的iphone机型ua
    def _addiPhone(ua):
        index = ua.find('-')
        if index < 0 :
            index = len(ua)
        ua = ua[:index]+"-iphone"+ua[index:]
        return ua

    if ua.find("mx_") == 0:
        ua = _addAndroid(ua)

    if ua.find("longcheer_#") == 0:
        ua = _addAndroid(ua)
    
    if ua.find("yiwap_#")== 0:
        ua = _addAndroid(ua)

    if ua in ('appstore#00bt','uplus#00bv','91zhushou#00el', 'cydia#00bu', 'tongbu#00bh'):
        ua = _addiPhone(ua)

    ua = ua.replace("symbian_","symbian-")
    ua = ua.replace("-#","#",1)
    
    lua = ua.split('-',2)
    
    xlens = 3 - len(lua)
    if xlens > 0:
        lua.extend(['' for i in range(xlens)])

    #如果第一个字段为平台名称，则添加默认渠道uplus至第一位
    if lua[0] in ('iphone', 'android' , 'symbian' , 'mtk' , 'win' , 'unknow' , '') :
        lua = ['uplus']+lua
    #如果第二个字段无法识别为已知平台则划归MTK
    if lua[1] not in ('iphone','android','symbian','mtk' , 'win' , 'unknow') :
        lua.insert(1,"mtk")

    #MTK平台将渠道名称和机型合并为渠道名称
    if lua[1] == "mtk":
        if lua[0].find("#") < 0:
            if lua[2] != '':
                lua[0] += "-%s" % lua[2]
    
    uaDetail['channel'] = lua[0]
    uaDetail['platform'] = lua[1]
    uaDetail['model'] = '-'.join([x for x in lua[2:] if x != '']) 

    return uaDetail
    #{'channel':...,'platform':...,'model':...}

def month1stdate(fday, tday):
    """
    fday 到 tday 之间每个月1号的datetime
    包括 fday 所属月
    """
    if tday < fday:
        raise ValueError, "fday can't larger then tday"
    fyear   = from_date.year
    fmonth  = from_date.month
    tyear   = to_date.year
    tmonth  = to_date.month
    month_num = (tyear-fyear)*12+(tmonth-tyear)+1

def monthdates(fday, tday):
    """
    拆分fday 到 tday 每日的datetime
    按月归类
    """
    if tday < fday:
        raise ValueError, "fday can't larger then tday"
    fyear  = fday.year
    fmonth = fday.month
    fmonth = (fyear, fmonth)
    ret    = []
    row    = []
    days        = (tday - fday).days+1
    for d in [fday+timedelta(v) for v in range(days)]:
        dm = (d.year, d.month)
        if dm == fmonth:
            row += [d]
        else:
            ret.append(row)
            fmonth = dm
            row = [d]
        if d.date() == tday.date():
            ret.append(row)
    return ret

