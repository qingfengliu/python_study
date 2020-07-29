#京东与淘宝的nike商品价格对比
import smtplib
from email.mime.text import MIMEText
import sqlalchemy
import time
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import io
conn = sqlalchemy.create_engine('mysql+pymysql://writer:xxxx@xxxx:3306/?charset=utf8',
								connect_args={'charset': 'utf8'})

to_list = ['xycui@.com',
           'wguan@.com','liu.qin.sz@belle.com.cn','zhan.yp.sz@belle.com.cn',
           'li.b.sz@belle.com.cn','liu.yiyang.sz@belle.com.cn','fang.c.sz@belle.com.cn','chen.dl@belle.com.cn',
           'luo.l.sz@belle.com.cn','yu.wt@topsports.com.cn','chen.lulu.sz@belle.com.cn','zhang.w@topsports.com.cn']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

# 接收邮件，可设置为你的QQ邮箱或者其他邮箱
#
#对于大型的邮件服务器，有反垃圾邮件的功能，必须登录后才能发邮件，如126,163
mail_server="smtp.exmail.qq.com"         # 126的邮件服务器
mail_login_user="xycui@.com"   #必须是真实存在的用户，这里我测试的时候写了自己的126邮箱
mail_passwd="xxxx"               #必须是对应上面用户的正确密码，我126邮箱对应的密码

def send_excel(to_list,sub,content,filename=None,file_data=None):
    me = mail_login_user + "<" + mail_login_user + ">"
    msg = MIMEMultipart()
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    puretext = MIMEText(content)
    msg.attach(puretext)
    if filename:
        xlsxpart = MIMEApplication(file_data)
        xlsxpart.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(xlsxpart)
    s = smtplib.SMTP()
    s.connect(mail_server)
    s.login(mail_login_user,mail_passwd)
    s.sendmail(me, to_list, msg.as_string())
    s.close()
    return True

def send_mail(to_list,sub,content):
    '''
    to_list:发给谁
    sub:主题
    content:内容
    send_mail("aaa@126.com","sub","content")
    '''
    me=mail_login_user+"<"+mail_login_user+">"
    msg = MIMEText(content)
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    s = smtplib.SMTP()
    s.connect(mail_server)
    s.login(mail_login_user,mail_passwd)
    s.sendmail(me, to_list, msg)
    s.close()
    return True

if __name__ == '__main__':
    sql = '''
        select `product_code1` as `product_code`,`dianpu_name`,`price`,`url` from
        (select `product_code1`,`dianpu_name`,`price`,`url`,`dt` from .`jd_nike_price_compare` aaa 
        where exists (
            select * from(
                select `dianpu_name`,`product_code1`,min(`price`) as `price`,`dt` 
                from .`jd_nike_price_compare` where TO_DAYS(NOW()) - TO_DAYS(`dt`) = 0
                group by `dianpu_name`,`product_code1` 
            ) bbb
            where aaa.product_code1=bbb.`product_code1` and `aaa`.price=bbb.`price` 
            and `aaa`.`dianpu_name`=bbb.`dianpu_name` and aaa.dt=bbb.dt
        group by `product_code1`,`dianpu_name`) group by `product_code1`,`dianpu_name`) ccc
        union
        select `product_code1`,`dianpu_name`,`price`,`url` from 
        (select `product_code1`,`dianpu_name`,`price`,`url`,`dt`
        from .`jd_staccato_data` aaa 
        where exists (
            select * from(
                select `dianpu_name`,`product_code1`,min(`price`) as `price`,dt 
                from .`jd_staccato_data` where TO_DAYS(NOW()) - TO_DAYS(`dt`) = 0
                group by `dianpu_name`,`product_code1` 
            ) bbb
            where aaa.`product_code1`=bbb.`product_code1` and `aaa`.price=bbb.`price` 
            and `aaa`.`dianpu_name`=bbb.`dianpu_name` and aaa.dt=bbb.dt
        group by `product_code1`,`dianpu_name`) group by `product_code1`,`dianpu_name`) ccc
    '''
    data=pd.read_sql(sql, conn)
    # data['product_code']=data['product_code'].map(lambda x:x.split())
    data=data[data['product_code']!='']
    data=data[data['dianpu_name'].isin(['耐克（NIKE）京东自营专区','滔搏运动官方旗舰店','耐克（滔搏）品牌授权店'])]
    data.index = data['product_code']
    temp=[]
    # print(data[data['product_code']=='846469-063'])
    for name, group in data.groupby(by='dianpu_name'):
        temp.append(group)
    data=pd.concat(temp,axis=1,join='inner')
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    data.to_excel(writer, sheet_name='sheet1')
    writer.save()
    xlsx_data = output.getvalue()
    if send_excel(to_list,"%s京东横向对比"%time.strftime('%Y-%m-%d', time.localtime(time.time())),
                  "请下载附件！！！",'%s京东横向对比.xlsx' %time.strftime('%Y-%m-%d', time.localtime(time.time())),xlsx_data):
        print("发送成功")
    else:
        print('发送失败')