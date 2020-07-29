import pymysql
conn = pymysql.connect(host='xxxx', user='writer', passwd='xxxx', db='', charset='utf8',
					connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
cur=conn.cursor()
cur.execute("SET SQL_SAFE_UPDATES=0;") 
cur.execute("delete from ``.`t_hh_meituan_shop_comments_tmp` where TO_DAYS(NOW( ))-TO_DAYS(`dt`) >=3;")
conn.commit()
cur.close()
conn.close()