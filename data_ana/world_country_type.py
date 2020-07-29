import pandas as pd
from sqlalchemy import create_engine
engine_myql = create_engine('mysql+pymysql://qfliu:111111@localhost:3306/zhongjiao_yuce')
data=pd.read_excel(r'E:\数据源\下载数据\wordgpd.xls',sheet_name='metadata')
data.to_sql('word_country_type', engine_myql, index=0, if_exists='replace')