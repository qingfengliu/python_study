import time
import pandas as pd
from sqlalchemy import create_engine


fun = lambda year, month: list(range(1, 1+time.localtime(time.mktime((year,month+1,1,0,0,0,0,0,0)) - 86400).tm_mday))
data=pd.DataFrame()
engine_oracle = create_engine(
    'oracle+cx_oracle://XXXX:XXXX123@IP保密:1527/ORCL?charset=utf8')
# 计算星期蔡勒公式

def getWeekDate(*args):
    year,month,day = args
    year = int(year)
    year = year - int(year / 100) * 100
    century = int(year/100)
    month = int(month)
    if month == 1 or month == 2:
        month = month + 12
        if year == 0:
            year = 99
            century = century - 1
        else:
            year = year - 1
    day =int(day)
    week = year + int(year/4) + int(century/4) - 2 * century + int(26 * (month + 1)/10) + day - 1
    if week < 0:
        weekDay = (week % 7 + 7) % 7
    else:
        weekDay = week % 7
    return weekDay

for i in range(2010,2050):
    for j in range(1,13):
        for k in fun(i,j):
            data=data.append(
                pd.DataFrame({'year':i,'month':j,'day':k,'geshi1':'%s-%02d-%02d' %(i,j,k),
                              'weekday':getWeekDate(i,j,k)}
                             ,index=["0"]),ignore_index=True)
data['date_parsed'] = pd.to_datetime(data['geshi1'],format="%Y-%m-%d")
data.to_sql('BI_CALENDAR', engine_oracle, index=0, if_exists='append')

