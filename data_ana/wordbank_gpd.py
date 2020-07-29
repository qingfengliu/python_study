import pandas as pd
from sqlalchemy import create_engine
engine_myql = create_engine('mysql+pymysql://qfliu:111111@localhost:3306/zhongjiao_yuce')
data=pd.read_excel(r'E:\数据源\下载数据\wordgpd.xls',sheet_name='countrygdp')
temp=data.drop(['Indicator Name','Indicator Code'], axis=1)
# temp=temp.stack()
# temp.index.map(lambda x:x['Country Name'])

data=temp.melt(id_vars=['Country Name','Country Code'],var_name='year',value_name='gdp')
data.to_sql('word_gpd', engine_myql, index=0, if_exists='replace')