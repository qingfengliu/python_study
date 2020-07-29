import os
import pandas as pd
from sqlalchemy import create_engine
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
engine = create_engine(
    'oracle+cx_oracle://ZZ_XXXX:ZZ_XXXX123@IP保密:1521/ORCL')
files=[]
for file in os.listdir(r'D:\国家指南\2\pdf'):
    files.append(file.split('.')[0])
files=pd.DataFrame(files,columns=['国家'])

guojia=pd.read_sql("SELECT DISTINCT XIANGMDY_DM,XIANGMDY_MC FROM ZZ_DM_HWJKJB_XMHZB where shifhw='是'",engine)
tmps=pd.merge(guojia,files,how='outer',left_on='xiangmdy_mc',right_on='国家')
tmps.to_excel(r"D:\output.xlsx")
