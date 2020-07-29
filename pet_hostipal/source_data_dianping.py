#相同的`con_deal_id`,`con_shop_id`,`dt`求和并且一些文字信息使用的是

import sqlalchemy
import sql_name
import calendar
import datetime
import time
import traceback
import pymysql
import pandas as pd
import arg_del


start_time,end_time,fdatabase,tdatabase = arg_del.get_attr()
start_time,end_time,fdatabase,tdatabase = arg_del.set_attr_def_value(start_time,end_time,fdatabase,tdatabase)

print('start_time=%s***********************,end_time=%s*****************'%(start_time,end_time))
for now_time in arg_del.week_alter(start_time,end_time):
    day={}
    day['start_time']=now_time.strftime('%Y-%m-%d')
    day['end_time']=(now_time+datetime.timedelta(7)).strftime('%Y-%m-%d')
    print('************************day=%s*************************' % day['start_time'])
    liuliang_conn = pymysql.connect(host='', user='work', passwd='', db='o2o', charset='utf8',
                                    connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
    liuliang_cur = liuliang_conn.cursor()
    liuliang_cur.execute(sql_name.sql_full_liuliang % day)
    liuliang_data = liuliang_cur.fetchall()
    liuliang_data = pd.DataFrame(liuliang_data)
    liuliang_cur.close()
    liuliang_conn.close()
    print('get liuliang data successful!')
    tuangou_conn = pymysql.connect(host='xxxx', user='writer', passwd='xxxx', db='o2o', charset='utf8',
                                   connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
    tuangou_cur = tuangou_conn.cursor()
    tuangou_cur.execute('SET SESSION group_concat_max_len = 102400;')
    tuangou_cur.execute(sql_name.sql_full_tuangou % day)
    tuangou_conn.commit()
    tuangou_datas = tuangou_cur.fetchall()
    tuangou_cur.close()
    tuangou_conn.close()
    print('get full_tuangou data successful!')
    for x in range(0, len(tuangou_datas), 1000):
        if x + 1000 < len(tuangou_datas):
            temp = pd.DataFrame(tuangou_datas[x:x + 1000])
        else:
            temp = pd.DataFrame(tuangou_datas[x:])
        data = temp.merge(liuliang_data, how='left', on=['shop_id', 'dt'])
        localhost_conn = pymysql.connect(host=tdatabase['ip'], user=tdatabase['user'], passwd=tdatabase['password'], db='o2o', charset='utf8',
                                         connect_timeout=7200 * 3, cursorclass=pymysql.cursors.DictCursor)
        localhost_cur = localhost_conn.cursor()
        column_name = data.columns
        for m, row in enumerate(data.itertuples()):
            temp = {}
            for i in column_name:
                if i == 'dt':
                    temp[i] = datetime.date.strftime(getattr(row, i), '%Y-%m-%d')
                    continue
                temp[i] = getattr(row, i)
                if temp[i] == None:
                    temp[i] = 'nan'
            temp['title'] = localhost_conn.escape_string(temp['title'])
            temp['platform'] = '大众点评'
            temp['con_deal_id']=temp['deal_id']
            temp['con_shop_id'] = temp['shop_id']
            sql = 'insert into o2o.tuangou_pet_source_data (`deal_id`,`shop_id`,`dt`,' \
                  '`title`,`start_time`,`end_time`,`category2_name`,' \
                  '`shop_name`,`description`,`description_m`,`city_name`,`district_name`,' \
                  '`biz_name`,`old_price`,`new_price`,' \
                  '`hits`,`today_hits`,`sales`,`click_count`,`pv`,`shop_num`,' \
                  '`sales_check`,`category2_name_m`,`city_name_m`,`district_name_m`,' \
                  '`biz_name_m`,`platform`,`con_deal_id`,`con_shop_id`,`check_url`) values("%(deal_id)s","%(shop_id)s","%(dt)s",' \
                  '"%(title)s","%(start_time)s","%(end_time)s","%(category2_name)s",' \
                  '"%(shop_name)s","%(description)s","%(description_m)s","%(city_name)s","%(district_name)s",' \
                  '"%(biz_name)s","%(old_price)s","%(new_price)s",' \
                  '"%(hits)s","%(today_hits)s","%(sales)s","%(click_count)s",' \
                  '"%(pv)s","%(shop_num)s","%(sales_check)s","%(category2_name_m)s",' \
                  '"%(city_name_m)s","%(district_name_m)s","%(biz_name_m)s","%(platform)s",' \
                  '"%(deal_id)s","%(shop_id)s","%(check_url)s");' % temp
            try:
                localhost_cur.execute(sql)
            except Exception as e:
                print(traceback.print_exc())
                print(sql)
        localhost_conn.commit()
        localhost_cur.close()
        localhost_conn.close()