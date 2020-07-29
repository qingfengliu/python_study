import impala
import datetime
import pymysql
from impala.dbapi import connect

def time_parse(t):
    t = int(t)
    today = datetime.date.today()
    oneday = datetime.timedelta(days=t)
    yesterday = today - oneday
    return yesterday.strftime("%Y-%m-%d")

#1.清理掉数据库里的旧数据

conn = pymysql.connect(host='xxxx',port=3306, user='writer', passwd='xxxx', db='', charset='utf8',
                       connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
cur=conn.cursor()
cur.execute('SET SQL_SAFE_UPDATES=0; delete from t_spider_tmallshop_goods where TO_DAYS(NOW()) - TO_DAYS(`dt`) > 10;')
conn.commit()
cur.close()
conn.close()

#2.将新数据导入到mysql中

conn = connect(host='10.15.1.16', port=10000,timeout=3600,auth_mechanism="PLAIN",user='qfliu')
cur = conn.cursor()
cur.execute('select to_date(`dt`),`url`,`title`,`price`,`month_sale`,`kuanhao`,`huohao`,`shop_name`,'
            '`nid`,"",`tag_price`,`time_to_market` from ods.t_spider_tmallshop_goods where  to_date(`dt`)="%s"  and shop_name="nike"' %time_parse(0) )
temp=cur.fetchall()
cur.close()
conn.close()

conn = pymysql.connect(host='xxxx', port=3306,user='writer', passwd='xxxx', db='', charset='utf8',
                       connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
cur=conn.cursor()
for i,j in enumerate(temp):
    cur.execute('replace into `t_spider_tmallshop_goods` values %s;' %str(j))
    conn.commit()
cur.close()
conn.close()

