#将tuangou_pet_world_filter表中的shop信息(用于过滤)和tuangou_pet_pri_table_data合并起来。是数据过程的最后一步。
#数据放入tuangou_city_rank,以城市,团购类型为细分的,周团购信息。
import pymysql
import pandas as pd
import sqlalchemy
import datetime
import arg_del

start_time,end_time,fdatabase,tdatabase=arg_del.get_attr()
start_time,end_time,fdatabase,tdatabase=arg_del.set_attr_def_value(start_time,end_time,fdatabase,tdatabase)
print('start_time=%s***********************,end_time=%s*****************'%(start_time,end_time))
for now_time in arg_del.week_alter(start_time,end_time):

    #每周六运行
    day={}
    day['start_time']=(now_time-datetime.timedelta(1)).strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(6)).strftime('%Y-%m-%d')
    print('*****************start_time:%s******************' % now_time)
    week_conn = pymysql.connect(host='', user='work', passwd='', db='o2o', charset='utf8',
                                     connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)

    week_cur = week_conn.cursor()
    week_cur.execute('select `dayweek54` from xiaonuan.dim_date where `date_dt`="%s";' %(day['end_time']))
    week=week_cur.fetchall()
    week_cur.close()
    week_conn.close()
    week=week[0]['dayweek54']
    day['week']=week

    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute("select * from(select * from o2o.tuangou_pet_pri_table_data WHERE `dt`>'%(start_time)s' and "
                          "`dt`<='%(end_time)s') aaa " 
                          "left join(select `deal_id` as `deal_id2`,`city_name`,`category2_name` as `shop_type`,`platform`,`check_url` "
                          "from o2o.tuangou_pet_world_filter WHERE `dayweek54`='%(week)s' "
                          "group by `deal_id`,`city_name`,`category2_name`,`platform`) bbb"
                          " on aaa.deal_id=bbb.deal_id2 and aaa.platform=bbb.platform;" %day
                          )
    data=localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    data=pd.DataFrame(data)
    data=data[['deal_id','dt','click_count_leiji','dayweek54','description','hits','sale_speed_leiji',
               'pv_leiji','sales','sales_check_leiji','zhouqi_hits','zhouqi_sale_speed','city_name','shop_type',
               'zhouqi_sales','platform','check_url']]
    #周销量、累计销量、卖速week、卖速
    rank=data.groupby(['city_name','shop_type','dayweek54','platform'])[['zhouqi_sales','sales','zhouqi_sale_speed','sale_speed_leiji']].rank(method='min',ascending=False)
    rank_shop_type_all=data.groupby(['city_name','dayweek54','platform'])[['zhouqi_sales','sales','zhouqi_sale_speed','sale_speed_leiji']].rank(method='min',ascending=False)
    rank.rename(columns={'zhouqi_sale_speed':'zhouqi_sale_speed_rank','zhouqi_sales':'zhouqi_sales_rank','sale_speed_leiji':'sale_speed_leiji_rank',
                          'sales':'sales_rank'},inplace=True)
    rank_shop_type_all.rename(columns={'zhouqi_sale_speed':'zhouqi_sale_speed_rank','zhouqi_sales':'zhouqi_sales_rank','sale_speed_leiji':'sale_speed_leiji_rank',
                          'sales':'sales_rank'},inplace=True)
    # rank_shop_type_all['shop_type']='全部'
    temp=data.copy()
    data=pd.concat([data,rank],axis=1)
    temp=pd.concat([temp,rank_shop_type_all],axis=1)
    temp['shop_type']='全部'
    data = pd.concat([data,temp])
    conn = sqlalchemy.create_engine(
        'mysql+pymysql://%(user)s:%(password)s@%(ip)s:%(port)s/o2o?charset=utf8' % tdatabase,
        connect_args={'charset': 'utf8'})
    # conn=sqlalchemy.create_engine('mysql+pymysql://root:111111@localhost:3306/o2o?charset=utf8',connect_args={'charset':'utf8'})
    data.to_sql('tuangou_city_rank', conn, if_exists='append')