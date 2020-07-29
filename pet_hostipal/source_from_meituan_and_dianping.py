#相同的`con_deal_id`,`con_shop_id`,`dt`求和并且一些文字信息使用的是

import sqlalchemy
import sql_name
import datetime
import pymysql
from sqlalchemy.dialects.mysql import DATE
import arg_del
import pandas as pd
import numpy as np
#每周六运行

start_time,end_time,fdatabase,tdatabase=arg_del.get_attr()
start_time,end_time,fdatabase,tdatabase=arg_del.set_attr_def_value(start_time,end_time,fdatabase,tdatabase)
print('start_time=%s***********************,end_time=%s*****************'%(start_time,end_time))
for now_time in arg_del.week_alter(start_time,end_time):
    day={}
    day['start_time']=now_time.strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(7)).strftime('%Y-%m-%d')
    print('************************day=%s*************************' % day['start_time'])
    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute(sql_name.sql_source_meituan_and_dianping %day)

    datas=localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    datas=pd.DataFrame(datas)
    datas.drop(['index','check_url'], axis=1, inplace=True)
    datas.rename(columns={'ccc.check_url':'check_url'},inplace=True)
    datas=datas.groupby(['deal_id','platform'],as_index=False).fillna(method='ffill')
    datas = datas.groupby(['deal_id','platform'],as_index=False).fillna(method='bfill')
    datas['sales']=datas['sales'].astype(float)
    datas['sales_meitun']=datas['sales_meitun'].astype(float)
    datas['sales']=datas['sales']+datas['sales_meitun']
    datas['pv']=datas['pv'].astype(float)
    datas['pv_meitun']=datas['pv_meitun'].astype(float)
    datas['pv'] = datas['pv'] + datas['pv_meitun']
    datas['today_hits']=datas['today_hits'].astype(float)
    datas['today_hits_meitun']=datas['today_hits_meitun'].astype(float)
    datas['today_hits'] = datas['today_hits'] + datas['today_hits_meitun']
    datas['hits']=datas['hits'].astype(float)
    datas['hits_meitun']=datas['hits_meitun'].astype(float)
    datas['hits'] = datas['hits'] + datas['hits_meitun']
    datas['click_count']=datas['click_count'].astype(float)
    datas['click_count_meitun']=datas['click_count_meitun'].astype(float)
    datas['hits'] = datas['hits'] + datas['hits_meitun']

    datas['start_time']=pd.to_datetime(datas['start_time'])
    datas['start_time_meitun'] = pd.to_datetime(datas['start_time_meitun'])

    #pandas 不能直接比较两列。

    datas['start_time'] = datas[['start_time', 'start_time_meitun']].max(axis=1)
    datas['shop_num']=datas['shop_num'].astype(float)
    datas['shop_num_meitun'] = datas['shop_num_meitun'].astype(float)
    datas['shop_num'] = datas[['shop_num','shop_num_meitun']].max(axis=1)
    # print(datas[datas['con_deal_id'] == '27253461'][['deal_id', 'shop_id', 'platform', 'dt', 'sales','pv']])
    conn = sqlalchemy.create_engine('mysql+pymysql://%(user)s:%(password)s@%(ip)s:%(port)s/o2o?charset=utf8' %tdatabase,
                                    connect_args={'charset': 'utf8'})
    datas = datas.reset_index()
    datas.drop(['index','click_count_meitun','start_time_meitun','hits_meitun','sales_meitun','shop_num_meitun','today_hits_meitun','pv_meitun'],
               axis=1, inplace=True)
    datas['platform']='美团+点评'
    datas.to_sql('tuangou_pet_source_data', conn, if_exists='append')