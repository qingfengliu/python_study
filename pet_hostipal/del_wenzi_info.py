# -*- coding: utf-8 -*-
#收集shop级别的团购的城市,店铺类型等信息。用于数据展示的过滤条件。
#数据放在tuangou_pet_world_filter中。以shop为单位的团购信息。这个表里还有其他信息,后续要加入dt用于合表不需要查整个表。
import pymysql
import pandas as pd
import sqlalchemy
import datetime
import arg_del

start_time,end_time,fdatabase,tdatabase=arg_del.get_attr()
start_time,end_time,fdatabase,tdatabase=arg_del.set_attr_def_value(start_time,end_time,fdatabase,tdatabase)

print('start_time=%s***********************,end_time=%s*****************'%(start_time,end_time))
#每周六运行
for now_time in arg_del.week_alter(start_time,end_time):
    day={}
    day['start_time']=now_time.strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(7)).strftime('%Y-%m-%d')
    print('*****************start_time:%s******************' % now_time)
    week_conn = pymysql.connect(host='', user='work', passwd='', db='o2o', charset='utf8',
                                     connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)

    week_cur = week_conn.cursor()
    week_cur.execute('select * from xiaonuan.dim_date;')
    week=week_cur.fetchall()
    week=pd.DataFrame(week)
    week=week[['date','dayweek54']]
    week_cur.close()
    week_conn.close()
    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute('select * from o2o.tuangou_pet_source_data where `dt`>"%(start_time)s" '
                          'and `dt`<="%(end_time)s";' %day)
    data=localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    data=pd.DataFrame(data)
    data_dianping=data
    week['date']=pd.to_datetime(week['date'])
    data_dianping['dt']=pd.to_datetime(data_dianping['dt'])-pd.Timedelta(days=1)

    data_dianping=data_dianping.merge(week,left_on='dt',right_on='date')
    data_dianping=data_dianping[['platform','deal_id','shop_id','city_name','title','shop_name','district_name','biz_name','category2_name','check_url','dayweek54']].copy()
    data_dianping=data_dianping.groupby(['platform','deal_id','dayweek54','shop_id'],as_index=False).apply(lambda x:x.tail(1))
    data_dianping=data_dianping.reset_index()
    data_dianping.drop(['level_0','level_1','shop_id'],axis=1,inplace=True)
    conn = sqlalchemy.create_engine(
        'mysql+pymysql://%(user)s:%(password)s@%(ip)s:%(port)s/o2o?charset=utf8' % tdatabase,
        connect_args={'charset': 'utf8'})
    data_dianping.to_sql('tuangou_pet_world_filter', conn, if_exists='append')