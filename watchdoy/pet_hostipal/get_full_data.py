# -*- coding: utf-8 -*-
#去掉shop量并且计算,zhouqi量和累加量等。是非常重要的步骤。几乎所有的计算都在这个文件中。
#生成的数据放在,tuangou_pet_full_and_summary_data,内容为细分到deal_id的每天的数据和周数据。
import pandas as pd
import numpy
import sqlalchemy

import datetime
import arg_del
import pymysql
import numpy as np
import traceback

# pd.options.mode.use_inf_as_na=True
start_time,end_time,fdatabase,tdatabase=arg_del.get_attr()
start_time,end_time,fdatabase,tdatabase=arg_del.set_attr_def_value(start_time,end_time,fdatabase,tdatabase)
print('start_time=%s***********************,end_time=%s*****************'%(start_time,end_time))

for now_time in arg_del.week_alter(start_time,end_time):
    day={}
    day['start_time']=now_time.strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(7)).strftime('%Y-%m-%d')
    print('*****************start_time:%s******************' %now_time)
    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute('select * from ( select * from o2o.tuangou_pet_source_data where `dt`>"%(start_time)s" and dt<="%(end_time)s") aaa,'
                          '(select max(`dt`) as check_time from o2o.tuangou_pet_source_data where `dt`>"%(start_time)s" and dt<="%(end_time)s") bbb,'
                          '(select count(1) as line) '
                          'as ccc;' %day)
    data=localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    data=pd.DataFrame(data)

    data.drop('index',axis=1,inplace=True)
    data.sort_values(['platform','deal_id','dt'],inplace=True)
    #时间类型的转化
    data['start_time']=data['start_time'].str.split(' ').str.get(0)
    data['start_time']=pd.to_datetime(data['start_time'])
    data['end_time']=data['end_time'].str.split(' ').str.get(0)
    data['end_time']=pd.to_datetime(data['end_time'])
    data['dt']=pd.to_datetime(data['dt'])
    #团购开始时间到最近更新时间
    data['tuangoudate']=(data['dt']-data['start_time']).dt.days+1
    data['dt']=(data['dt']-pd.Timedelta(days=1))
    data['sales']=data['sales'].astype(float)
    data['shop_num']=data['shop_num'].astype(float)
    data['sales_check']=data['sales_check'].astype(float)
    data['hits']=data['hits'].astype(float)
    data['today_hits']=data['today_hits'].astype(float)
    data['click_count']=data['click_count'].astype(float)
    data['pv']=data['pv'].astype(float)
    data['new_price']=data['new_price'].astype(float)
    data['old_price']=data['old_price'].astype(float)

    week_conn = pymysql.connect(host='', user='work', passwd='', db='o2o', charset='utf8',
                                     connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)

    week_cur = week_conn.cursor()
    week_cur.execute('select * from xiaonuan.dim_date;')
    week=week_cur.fetchall()
    week=pd.DataFrame(week)
    week=week[['date','dayweek54']]
    week_cur.close()
    week_conn.close()
    week['date']=pd.to_datetime(week['date'])
    data=data.merge(week,left_on='dt',right_on='date')
    #聚合将团购卷相同时间相同的聚合到一起,去掉shop维度
    temp=data.groupby(['deal_id','dt','platform'],as_index=False)[['today_hits','hits','click_count','pv','sales_check']].sum()
    data.drop_duplicates(['deal_id','dt','platform'],keep='last',inplace=True)     #inplace=True表示在原DataFrame上执行删除操作
    data.drop(['hits','click_count','pv','sales_check','shop_id','shop_name','today_hits'],axis=1,inplace=True)
    data=data.merge(temp,on=['deal_id','dt','platform'])
    #折扣率
    data['discount']=numpy.true_divide(data['new_price'],data['old_price'])
    data[data['old_price']==0]['discount']=0
    data[data['old_price']==7]['discount']=0
    # print(data.sort_values('dt')['dt'])
    # exit()
    #星期几
    data['week_num']=data['dt'].dt.week  #周数(假)
    data['week_day']=data['dt'].dt.dayofweek

    #宠tuangou_pri_table_data中取出历史数据。
    day_lask_week={}
    day_lask_week['start_time']=(now_time-datetime.timedelta(8)).strftime('%Y-%m-%d')
    day_lask_week['end_time']=(now_time-datetime.timedelta(1)).strftime('%Y-%m-%d')
    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute('''select * from (
        SELECT * FROM o2o.tuangou_pet_pri_table_data aaa where exists
        (select `deal_id` from (
                select `deal_id`,`platform`,max(`dt`) as `dt` from
                o2o.tuangou_pet_source_data where `dt`>'%(start_time)s' and `dt`<='%(end_time)s' group by
                `deal_id`,`platform`
                ) bbb
                where aaa.`deal_id`=bbb.deal_id  and aaa.platform=bbb.platform
                )
        ) aaa
    where exists
    (
        SELECT `deal_id` from
            (select `deal_id`,`platform`,max(`dt`) as `dt` FROM o2o.tuangou_pet_pri_table_data where
            `dt`>'%(start_time)s' and `dt`<='%(end_time)s' group by `deal_id`, `platform`
            )bbb
            where aaa.`deal_id`=bbb.deal_id  and
            aaa.platform=bbb.platform and `aaa`.dt=bbb.dt
    );

    ''' %day_lask_week)
    week_history=localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    if week_history:
        week_history=pd.DataFrame(week_history)
        week_history=week_history[['deal_id','dt','dayweek54','platform','sales','sales_check_leiji','sale_speed_leiji','shop_num_leiji','line_leiji',
                                   'pv_leiji','click_count_leiji','hits']]
        week_history.rename(columns={'sales_check_leiji':'sales_check','shop_num_leiji':'shop_num',
                                                  'line_leiji':'line','pv_leiji':'pv','click_count_leiji':'click_count'},inplace=True)
        data.sort_values(['platform', 'deal_id', 'dt'], inplace=True)
        data=pd.concat([data,week_history],ignore_index=True)
    max_week=data['dayweek54'].max()
    min_week=data['dayweek54'].min()
    if int(min_week)==1 and max_week>2:
        data['dayweek54'].loc[data[data['dayweek54'] == max_week].index] = 0
        max_week=1
    data.sort_values(['platform','deal_id','dt'],inplace=True)
    #日均参团店数
    data['shop_num_leiji']=data.groupby(['deal_id','platform'])['shop_num'].cumsum()

    data['line_leiji']=data.groupby(['deal_id','platform'])['line'].cumsum()

    data['team_buy_shop_day']= numpy.true_divide(data['shop_num_leiji'],data['line_leiji'])
    #累计用户浏览量

    data['pv']=data['pv'].astype(float)
    data['pv_leiji']=data.groupby(['deal_id','platform'])['pv'].cumsum()

    #hits是店铺浏览量
    #累计团购点击量
    data['click_count']=data['click_count'].astype(float)
    data['click_count_leiji']=data.groupby(['deal_id','platform'])['click_count'].cumsum()
    #当日团购销量
    data['sales_check']=data['sales_check'].astype(float)

    data['sales_day']=data.groupby(['deal_id','platform'])['sales'].diff()
    data['sales_day'].fillna(0,inplace=True)
    #累计核销量
    data['sales_check_leiji']=data.groupby(['deal_id','platform'])['sales_check'].cumsum()
    #累计销售速度:累计团购销售量/日均参团店数/天数


    data['sale_speed_leiji']=numpy.true_divide(numpy.true_divide(data['sales'],data['team_buy_shop_day']),data['tuangoudate'])
    # data[data['sale_speed_leiji']==np.inf]=np.nan
    #当日销售速度:当日销售速度=当日团购销售量/参团店数
    data['sales_speed_day']=numpy.true_divide(data['sales_day'],data['shop_num'])
    #累计单店销售量:累计团购销售量/日均参团店数
    data['shop_sales_leiji']=numpy.true_divide(data['sales'],data['team_buy_shop_day'])
    data_summary=data[['deal_id','dt','hits','sales','sale_speed_leiji',
                       'week_day','week_num','dayweek54','tuangoudate','platform']].copy()
    #求周期数
    data_week=data_summary.groupby(['deal_id','dayweek54','platform'],as_index=False).apply(lambda x:x.tail(1))
    data_week.reset_index(inplace=True)
    data_week.drop(['level_0','level_1'],axis=1,inplace=True)
    #取第一周的数
    tmp=data_week.groupby(['deal_id','platform'],as_index=False).apply(lambda x:x.head(1)).reset_index()
    tmp.index=tmp['level_1']
    #周期用户浏览量
    data_week['zhouqi_hits']=data_week.groupby(['deal_id','platform'])['hits'].diff()
    tmp['zhouqi_hits']=tmp['hits']
    data_week['zhouqi_hits'].loc[tmp[tmp['tuangoudate']<=7]['level_1']]=tmp[tmp['tuangoudate']<=7]['zhouqi_hits']

    #周期团购销售量
    data_week['zhouqi_sales']=data_week.groupby(['deal_id','platform'])['sales'].diff()
    tmp['zhouqi_sales']=tmp['sales']
    data_week['zhouqi_sales'].loc[tmp[tmp['tuangoudate']<=7]['level_1']]=tmp[tmp['tuangoudate']<=7]['zhouqi_sales']
    temp=data.groupby(['deal_id','dayweek54','platform'],as_index=False)['shop_num'].mean()
    temp.rename(columns={'shop_num':'zhouqi_shop_num'},inplace=True)
    data_week=data_week.merge(temp[['deal_id','zhouqi_shop_num','dayweek54','platform']],on=['deal_id','dayweek54','platform'], how='inner')
    #周期销售速度

    sale_speed_temp=data_week.groupby(['deal_id', 'platform'], as_index=False)['dt'].diff().dt.days
    sale_speed_temp.index=sale_speed_temp.index.droplevel()
    data_week['sale_speed_temp']=sale_speed_temp
    data_week['zhouqi_sale_speed']=numpy.true_divide(numpy.true_divide(data_week['zhouqi_sales'],
                                    data_week['zhouqi_shop_num']),data_week['sale_speed_temp'])
    tmp = tmp.sort_index()
    data_week = data_week.sort_index()
    data_week['zhouqi_sale_speed'].loc[tmp[tmp['tuangoudate']<=7]['level_1']]=numpy.true_divide(numpy.true_divide(tmp[tmp['tuangoudate']<=7]['zhouqi_sales'],data_week['zhouqi_shop_num'].loc[tmp[tmp['tuangoudate']<=7]['level_1']]),tmp[tmp['tuangoudate']<=7]['tuangoudate'])
    #-------------求周期数完毕---------------------
    data_week.rename(columns={'sales':'sales_leiji_zq','sale_speed_leiji':'sale_speed_leiji_zq'},inplace=True)
    data.drop(['city_name','district_name','biz_name','category2_name','date',
               'week_day','line','week_num','con_shop_id'],axis=1,inplace=True)
    # data['dayweek54'] =data['dt'].map(lambda x:x.year).astype(int) * 100 + data['dayweek54']
    # data_week['dayweek54'] =data_week['dt'].map(lambda x:x.year).astype(int) * 100 + data_week['dayweek54']
    data_week=data_week[data_week['dayweek54']==max_week]  #跨年 maxdata变了
    data_week.drop(['week_day', 'week_num', 'dt','hits', 'tuangoudate', 'sale_speed_temp'], axis=1, inplace=True)
    data=data.merge(data_week,on=['deal_id','dayweek54','platform'], how='inner')
    conn = sqlalchemy.create_engine('mysql+pymysql://%(user)s:%(password)s@%(ip)s:%(port)s/o2o?charset=utf8' %tdatabase,
                                    connect_args={'charset': 'utf8'})
    data.to_sql('tuangou_pet_full_and_summary_data', conn, if_exists='append')
    #---------gut_week_rank_data---------------
    day = {}
    day['start_time']=(now_time-datetime.timedelta(1)).strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(6)).strftime('%Y-%m-%d')

    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute('select * from tuangou_pet_full_and_summary_data where `dt`>"%(start_time)s" and `dt`<="%(end_time)s";' % day)
    data = localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    data = pd.DataFrame(data)
    data = data[['deal_id', 'dt', 'start_time', 'zhouqi_sales', 'zhouqi_shop_num', 'zhouqi_sale_speed', 'zhouqi_hits',
                 'sale_speed_leiji', 'sales', 'shop_num',
                 'new_price', 'discount', 'city_name_m', 'district_name_m', 'description', 'biz_name_m',
                 'category2_name_m', 'description_m', 'dayweek54',
                 'shop_num_leiji', 'line_leiji', 'sales_check_leiji', 'pv_leiji', 'click_count_leiji', 'hits',
                 'platform','con_deal_id']]
    data = data.sort_values(['platform', 'deal_id', 'dt'])
    data = data.groupby(['platform', 'deal_id', 'dayweek54'], as_index=False).apply(lambda x: x.tail(1))  # 取最后一条记录
    conn = sqlalchemy.create_engine('mysql+pymysql://%(user)s:%(password)s@%(ip)s:%(port)s/o2o?charset=utf8' %tdatabase,
                                    connect_args={'charset': 'utf8'})
    data = data.reset_index()
    data.drop(['level_0', 'level_1'], axis=1, inplace=True)
    data.to_sql('%s' % 'tuangou_pet_pri_table_data', conn, if_exists='append')