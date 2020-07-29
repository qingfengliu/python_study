import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
hanshu='''
DECLARE
o_success VARCHAR2(32);
o_msg VARCHAR2(32);
BEGIN
	%(zunchuname)s('%(riqi)s',o_success,o_msg);
END;
'''
sdate  = datetime.datetime.strptime('2019-08-01','%Y-%m-%d')
engine_oracle = create_engine(
    'oracle+cx_oracle://XXXX:XXXX123@IP保密:1527/ORCL?charset=utf8')
for i in range(0,(datetime.datetime.now()-sdate).days):
    delta7 = datetime.timedelta(days=i)
    tmp=sdate + delta7
    tmp=tmp.strftime('%Y%m%d')
    print('***************%s************************' %tmp)
    conn = engine_oracle.connect()
    conn.execute(hanshu %{'zunchuname':'P_DM_JKFX_BIPORTAL','riqi':tmp})
    conn.close()