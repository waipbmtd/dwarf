#coding: utf-8 
from datetime import datetime, timedelta

def month1stdate(fday, tday):
    """
    fday 到 tday 之间每个月1号的datetime
    包括 fday 所属月
    """
    lMDates  = monthdates(fday, tday)
    lMD1 = [v[0] for v in lMDates]
    lMD1[0] = datetime(lMD1[0].year, lMD1[0].month, 1)
    return lMD1


def monthdates(fday, tday):
    """
    拆分fday 到 tday 每日的datetime
    按月归类
    """
    if tday < fday:
        raise ValueError, "fday can't larger then tday"
    ret    = []
    row    = []
    fmonth = (fday.year,fday.month)
    days   = (tday - fday).days+1
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

def list_day(fday, tday):
    """
    return the date string list from fday to tday
    """
    days        = (tday - fday).days+1
    dayList     = [fday+timedelta(v) for v in range(days)] 
    return dayList

def deltadays(dt1, dt2):
    """
    Difference between two datetime/date values
    :param dt1: datetime.datetime or datetime.date
    :param dt2: datetime.datetime or datetime.date
    :return: datetime.timedelta.days
    """
    if isinstance(dt1, datetime):
        dt1 = dt1.date()
    if isinstance(dt2, datetime):
        dt2 = dt2.date()
    return (dt1 - dt2).days