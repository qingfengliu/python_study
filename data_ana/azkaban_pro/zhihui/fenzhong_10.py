# encoding=utf8
import os
import decimal
os.environ["NLS_LANG"] = "GREEK_GREECE.AL32UTF8"
os.environ['LD_LIBRARY_PATH']='/usr/local/instantclient_19_3'

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
tables=[{'from':'table1','to':'table1'}]

for table in tables:
    print('*************from:%s***********to:%s***************' %(table['from'],table['to']))
    engine1 = create_engine("mssql+pymssql://account:password@ip:port/DeviceMonitor_0512",deprecate_large_types=True)
    db_conn1=engine1.connect()
    tmps1=db_conn1.execute('select * from '+table['from'])
    engine2 = create_engine('oracle+cx_oracle://account:password@ip:port/ORCL?charset=utf8')
    db_conn2=engine2.connect()
    for tmp1 in tmps1:
        tmp1=[None,'SYSDATE']+list(tmp1)
        for i,tmp in enumerate(tmp1):
            if isinstance(tmp,decimal.Decimal):
                tmp1[i]=float(tmp1[i])
        tmp1=tuple(tmp1)
        tmp1=str(tmp1).replace('None','NULL').replace("'SYSDATE'",'SYSDATE')
        db_conn2.execute('insert into '+table['to']+' values%s' %tmp1)
    db_conn2.close()
    db_conn1.close()
