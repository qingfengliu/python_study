import sqlalchemy
import sql_name
import datetime
import pymysql
import time
from sqlalchemy.dialects.mysql import DATE
import arg_del

#从第二周周六(也就是实际的每周第二天)

start_time,end_time,fdatabase,tdatabase=arg_del.get_attr()
start_time,end_time,fdatabase,tdatabase=arg_del.set_attr_def_value(start_time,end_time,fdatabase,tdatabase)

for now_time in arg_del.week_alter(start_time,end_time):
    day={}
    day['start_time']=now_time.strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(7)).strftime('%Y-%m-%d')
    print('****************day=%s***************' % day['start_time'])
    localhost_conn = pymysql.connect(host=fdatabase['ip'], user=fdatabase['user'], passwd=fdatabase['password'],
                                     db='o2o', charset='utf8',
                                     connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
    localhost_cur = localhost_conn.cursor()
    localhost_cur.execute(sql_name.sql_source_meituan %day)
    datas=localhost_cur.fetchall()
    localhost_cur.close()
    localhost_conn.close()
    if not datas:
        continue
    conn = sqlalchemy.create_engine('mysql+pymysql://%(user)s:%(password)s@%(ip)s:%(port)s/o2o?charset=utf8' %tdatabase,
                                    connect_args={'charset': 'utf8'})
    metadata = sqlalchemy.MetaData(conn)
    users_table = sqlalchemy.Table('tuangou_pet_source_data', metadata,
                                   sqlalchemy.Column('deal_id', sqlalchemy.String(45)),
                                   sqlalchemy.Column('shop_id', sqlalchemy.String(45)),
                                   sqlalchemy.Column('dt',DATE),
                                   sqlalchemy.Column('title', sqlalchemy.String(45)),
                                   sqlalchemy.Column('start_time', sqlalchemy.String(45)),
                                   sqlalchemy.Column('end_time', sqlalchemy.String(45)),
                                   sqlalchemy.Column('category2_name', sqlalchemy.String(45)),
                                   sqlalchemy.Column('shop_name', sqlalchemy.String(45)),
                                   sqlalchemy.Column('description', sqlalchemy.String(45)),
                                   sqlalchemy.Column('city_name', sqlalchemy.String(45)),
                                   sqlalchemy.Column('district_name', sqlalchemy.String(45)),
                                   sqlalchemy.Column('biz_name', sqlalchemy.String(45)),
                                   sqlalchemy.Column('old_price', sqlalchemy.String(45)),
                                   sqlalchemy.Column('new_price', sqlalchemy.String(45)),
                                   sqlalchemy.Column('hits', sqlalchemy.String(45)),
                                   sqlalchemy.Column('today_hits', sqlalchemy.String(45)),
                                   sqlalchemy.Column('sales', sqlalchemy.String(45)),
                                   sqlalchemy.Column('click_count', sqlalchemy.String(45)),
                                   sqlalchemy.Column('pv', sqlalchemy.String(45)),
                                   sqlalchemy.Column('shop_num', sqlalchemy.String(45)),
                                   sqlalchemy.Column('sales_check', sqlalchemy.String(45)),
                                   sqlalchemy.Column('category2_name_m', sqlalchemy.String(45)),
                                   sqlalchemy.Column('city_name_m', sqlalchemy.String(45)),
                                   sqlalchemy.Column('district_name_m', sqlalchemy.String(45)),
                                   sqlalchemy.Column('biz_name_m', sqlalchemy.String(45)),
                                   sqlalchemy.Column('description_m', sqlalchemy.String(45)),
                                   sqlalchemy.Column('platform', sqlalchemy.String(45)),
                                   sqlalchemy.Column('con_deal_id', sqlalchemy.String(45)),
                                   sqlalchemy.Column('con_shop_id', sqlalchemy.String(45)),
                                   sqlalchemy.Column('check_url', sqlalchemy.String(100)),
                                   )
    insert_table = users_table.insert().prefix_with('IGNORE')
    len_data=len(datas)
    temp=[]
    for i,data in enumerate(datas):
        temp.append(data)
        if i%1000==0 and len(temp)>100:
            insert_table.execute(temp)
            temp.clear()
            continue
        if i==(len(datas)-1) and temp:
            insert_table.execute(temp)