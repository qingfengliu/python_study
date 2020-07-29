import pandas as pd
import argparse
import mod
import time
from sqlalchemy import create_engine
import os

def get_attr():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--path",help="存放存储过程的文件夹")
    parser.add_argument("-m", "--mudi", help="程序处理目的rp层还是dm层")
    args = parser.parse_args()
    try :
        path=args.path
        mudi=args.mudi.lower()
        if mudi=='rp':
            mudi="RP_保密"
        elif mudi=="dm":
            mudi="XXXX"
        else:
            raise Exception("参数错误")
    except Exception as e:
        path=None
    return path,mudi

path,mudi=get_attr()
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dt = time.strftime('%Y-%m-%d', time.localtime())
data=pd.read_csv('csvFile.csv',encoding='gbk')
temps=data.drop_duplicates(['转以后存储过程名'])['转以后存储过程名']

for temp in temps:
    rows=data[data['转以后存储过程名']==temp]
    with open(temp + '.sql', 'w') as f:
        changsql=''
        yewu=''
        dizhi=''
        for i,row in rows.iterrows():
            dizhi=row['地址']
            if mudi=='XXXX':
                row['表名']='XXXX.'+row['表名']
            changsql += mod.sql_mod %{'mubiao': row['表名'],'yuanbiao': row['表名'],'dizhi':dizhi}
            changsql+='\n'
            yewu=row['业务类型']
        sql_all=mod.arl_del% {'stoge_name': temp,
                'date': dt,'yewu': yewu,'zuozhe': 'liuqingfeng',
                              'chongfusql':changsql}
        f.write(sql_all)
        f.write('\n')
        engine = create_engine(
            'oracle+cx_oracle://RP_保密:RP_保密123@IP保密:1521/ORCL?charset=utf8')
        db_conn = engine.connect()
        db_conn.execute(sql_all)
        db_conn.close()
