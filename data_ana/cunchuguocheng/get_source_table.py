# -*- coding: utf-8 -*-
import argparse
import re
from sqlalchemy import create_engine
import pandas as pd
import csv
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'



def get_attr():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--proce", help="直接使用存储过程")
    parser.add_argument("-p","--path",help="存放存储过程的文件夹")
    parser.add_argument("-m", "--mudi", help="程序处理目的rp层还是dm层")
    args = parser.parse_args()
    try :
        path=args.path
        mudi=args.mudi.lower()
        proce = args.proce.split(',')
        if mudi=='rp':
            mudi="RP_保密"
        elif mudi=="dm":
            mudi="XXXX"
        else:
            raise Exception("参数错误")
    except Exception as e:
        path=None

    return path,mudi,proce


def get_data_file(path):  # 获取文件路径
    data=pd.read_excel(path)
    return data


path,mudi,proce=get_attr()

if not proce:
    data_frame=get_data_file(path)
else:
    data_frame=pd.DataFrame(data={'RP层存储过程':[proce[0]],
                             '主题':[proce[1]],'取数频率':[proce[2]],'对应存储':[proce[3]],
                             '地址':[proce[4]]})

temps=data_frame.drop_duplicates('RP层存储过程')['RP层存储过程']
csvFile2 = open('csvFile.csv','w', newline='') # 设置newline，否则两行之间会空一行
writer = csv.writer(csvFile2)
writer.writerow(['表名','存储过程','业务类型','更新时间','转以后存储过程名','地址'])


for temp in temps:
    igno = 0
    engine = create_engine(
        'oracle+cx_oracle://%s:%s123@IP保密:1527/ORCL?charset=utf8' %(mudi,mudi))
    db_conn = engine.connect()
    jiegou_infos=db_conn.execute("SELECT * FROM user_source where name='%s' ORDER BY line" %(temp))
    db_conn.close()
    tables = set()
    for line in jiegou_infos:
        line=line[-1]
        if re.search('/\*',line,re.IGNORECASE):
            igno=1
        if re.search('\*/', line, re.IGNORECASE):
            igno = 0
        table_name=re.search('insert into ((?!tmp))([a-zA-Z_\.]+)',line,re.IGNORECASE)
        try:
            table_name = table_name.group(2)
            table_name=table_name.split('.')[-1]
        except Exception as e:
            pass
        if table_name and not igno and table_name!='s_log':
            if mudi=='XXXX':
                table_name='XXXX.'+table_name
            tables.add(table_name)
    tables=list(tables)
    for table in tables:
        tmp=data_frame[data_frame['RP层存储过程']==temp]
        tmp1=[table,tmp['RP层存储过程'].tolist()[0],tmp['主题'].tolist()[0],
              tmp['取数频率'].tolist()[0],tmp['对应存储'].tolist()[0],tmp['地址'].tolist()[0]
              ]
        writer.writerow(tmp1)
csvFile2.close()