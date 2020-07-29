import smtplib
from email.mime.text import MIMEText
import pymysql
import pandas as pd
import time
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import datetime
def time_parse(t):
    t = int(t)
    today = datetime.date.today()
    oneday = datetime.timedelta(days=t)
    yesterday = today - oneday
    return yesterday.strftime("%Y-%m-%d")

to_list = ['xycui@.com','wguan@.com','ljwang@.com']

# 接收邮件，可设置为你的QQ邮箱或者其他邮箱
#
#对于大型的邮件服务器，有反垃圾邮件的功能，必须登录后才能发邮件，如126,163
mail_server="smtp.exmail.qq.com"         # 126的邮件服务器
mail_login_user="xycui@.com"   #必须是真实存在的用户，这里我测试的时候写了自己的126邮箱
mail_passwd="xxxx"               #必须是对应上面用户的正确密码，我126邮箱对应的密码
def time_parse(t):
    t = int(t)
    today = datetime.date.today()
    oneday = datetime.timedelta(days=t)
    yesterday = today - oneday
    return yesterday.strftime("%Y-%m-%d")

def send_mail(to_list,sub,content):
    '''
    to_list:发给谁
    sub:主题
    content:内容
    send_mail("aaa@126.com","sub","content")
    '''
    me=mail_login_user+"<"+mail_login_user+">"
    msg = MIMEMultipart()
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    msg.attach(content)
    s = smtplib.SMTP()
    s.connect(mail_server)
    s.login(mail_login_user,mail_passwd)
    s.sendmail(me, to_list, msg.as_string())
    s.close()
    return True

#recive pandas Dataframe
def get_table_content(data):
    # a = pd.DataFrame({'数列1': (1, 1, 1, 1), '数列2': (2, 2, 2, 2), '数列3': (3, 3, 3, 3), '数列4': (4, 4, 4, 4)})
    # a.index = {'行1', '行2', '行3', '行4'}  # 这里dataframe类型a就是要输出的表格
    sub = "test"
    d = ''  # 表格内容
    for i in range(len(data)):
        d = d + """
            <tr>
              <td>""" + str(data.index[i]) + """</td>
              <td>""" + str(data.iloc[i][0]) + """</td>
              <td width="60" align="center">""" + str(data.iloc[i][1]) + """</td>
              <td width="75">""" + str(data.iloc[i][2]) + """</td>
            </tr>"""
    html = """\
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />


    <body>
    <div id="container">
    <p><strong>测试程序邮件发送:</strong></p>
    <div id="content">
     <table width="30%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
    <tr>
      <td width="40"><strong>项目</strong></td>
      <td width="50"><strong>项目名称</strong></td>
      <td width="60" align="center"><strong>爬虫状态</strong></td>
      <td width="50"><strong>爬虫item数量</strong></td>
    </tr>""" + d + """
    </table>
    </div>
    </div>
    </div>
    </body>
    </html>
          """
    context = MIMEText(html, _subtype='html', _charset='utf-8')  # 解决乱码
    return context


conn = pymysql.connect(host='xxxx',port=3306 ,user='writer', passwd='xxxx', db='o2o', charset='utf8',
                                   connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()
#如果三家店铺中某日有记录则置spiderstatic为1，并且spideritemcount代表item数量如果没有则全为0。证明对于爬虫可能出现问题
cur.execute('''
select 
	'天猫数据' as `spidername`,
	case when `aaa`.`tm_item`>0  then 1 else 0 end as `spiderstatic`,
    case when `aaa`.`tm_item`  then `aaa`.`tm_item` else 0 end as `spideritemcount`
    from
	(select count(*) as `tm_item` from .t_spider_tmallshop_goods 
    where `dt`='%(time)s') 
    aaa
union 
select 
	'Nike官网数据' as `spidername`,
	case when `aaa`.`Nike_item`>0  then 1 else 0 end as `spiderstatic`,
    case when `aaa`.`Nike_item`  then `aaa`.`Nike_item` else 0 end as `spideritemcount`
    from
	(select count(*) as `Nike_item` from .t_spider_NikeStore_goods 
    where `dt`='%(time)s') 
    aaa
union 
select 
	'耐克东自营' as `spidername`,
	case when `aaa`.`Nike_item`>0  then 1 else 0 end as `spiderstatic`,
    case when `aaa`.`Nike_item`  then `aaa`.`Nike_item` else 0 end as `spideritemcount`
    from
	(select count(*) as `Nike_item` from .jd_staccato_data 
    where `dt`='%(time)s' and `dianpu_name`='耐克（滔搏）品牌授权店') 
    aaa
;
''' %{'time':time.strftime('%Y-%m-%d', time.localtime(time.time()))})


data=cur.fetchall()
cur.close()
conn.close()
data=pd.DataFrame(data)
data=data[['spidername','spiderstatic','spideritemcount']]
return_value=0
if (data['spiderstatic']==0).any():
    return_value = -1
if return_value==-1:
    static_str='爬虫任务检测失败'
else:
    static_str = '爬虫任务检测成功'
content=get_table_content(data)
if send_mail(to_list, "%s京东天猫对比爬虫数据:%s" % (time.strftime('%Y-%m-%d', time.localtime(time.time())),static_str),content):
    print("发送成功")
else:
    print('发送失败')
exit(return_value)