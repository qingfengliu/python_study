import pypinyin
import os
import re
from sqlalchemy import create_engine
import pandas as pd

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
engine = create_engine(
    'oracle+cx_oracle://ZZ_XXXX:ZZ_XXXX123@IP保密:1527/ORCL')
data=pd.read_sql('select * from ZZ_DM_HWJKJB_GUOJDZ',engine)
data['pinyin']=data['shangybgj'].map(lambda x:x if x == None is
                                          x else re.sub('（|）','',''.join(pypinyin.lazy_pinyin(x))))

data.to_excel(r'E:\轉拼音.xlsx',sheet_name='sheet1')
