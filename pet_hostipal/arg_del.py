import datetime
import time
import pymysql
import pandas as pd
import argparse

data_base={
                'localhost':{'ip':'localhost','port':3306,'user':'root','password':'111111'},
                'company_up':{'ip':'','port':3306,'user':'work','password':''},
                'company_down':{'ip':'xxxx','port':3306,'user':'writer','password':'xxxx'},
}

def get_attr():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--start_time",help="start time of script")
    parser.add_argument("-e", "--end_time", help="end time of script")
    parser.add_argument("-fd", "--fdatabase", help="the from of database")
    parser.add_argument("-td", "--tdatabase", help="the store of database")
    args = parser.parse_args()
    try :
        start_time=args.start_time
    except Exception as e:
        start_time=None
    try:
        end_time = args.end_time
    except Exception as e:
        end_time=None
    try:
        fdatabase=args.fdatabase
    except Exception as e:
        fdatabase=None
    try:
        tdatabase=args.tdatabase
    except Exception as e:
        tdatabase=None
    return start_time,end_time,fdatabase,tdatabase

def set_attr_def_value(start_time,end_time,fdatabase,tdatabase):
    if end_time==None:
        end_time=datetime.datetime.now().date()
    else:
        end_time =datetime.datetime.fromtimestamp(time.mktime(time.strptime(end_time, "%Y-%m-%d"))).date()
    week_change=((2+end_time.isoweekday())%7)
    end_time=end_time-datetime.timedelta(week_change)
    if start_time==None:
        start_time=end_time-datetime.timedelta(7)
    else:
        start_time = datetime.datetime.fromtimestamp(time.mktime(time.strptime(start_time, "%Y-%m-%d"))).date()
    week_change=((2+start_time.isoweekday())%7)
    start_time=start_time-datetime.timedelta(week_change)
    if start_time>end_time:
        raise ValueError("the front start_time's firday >= the front end_time's firday")


    if fdatabase==None:
        fdatabase='company_up'
    fdatabase=data_base[fdatabase]

    if tdatabase==None:
        tdatabase='company_up'
    tdatabase=data_base[tdatabase]
    return start_time,end_time,fdatabase,tdatabase

def week_alter(start,end):
    while start<end:
        yield start
        start+=datetime.timedelta(7)