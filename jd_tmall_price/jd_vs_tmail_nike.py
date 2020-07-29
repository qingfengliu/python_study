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
    print(content)
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

    temps=[]
    plaform=['天猫旗舰店','官网旗舰店']
    sql = '''
        select `shop_id` as `item_id`,`product_code`,`shop_name` as `title`,`price`,`tmallprice`,`url`,`dt` from(
        (select * from jd_staccato_data where TO_DAYS(NOW()) - TO_DAYS(`dt`) = 0 and `product_code`!='') aaa left join
        (
            select distinct concat(`kuanhao`,`huohao`) as `kuanhao`,`price` as `tmallprice`
            from t_spider_tmallshop_goods where TO_DAYS(NOW()) - TO_DAYS(`dt`) = 0
        ) bbb
        on aaa.`product_code`=bbb.`kuanhao`
        ) where CONVERT(`price`,DECIMAL)>CONVERT(`tmallprice`,DECIMAL);
    '''
    data=pd.read_sql(sql, conn)
    data.rename(columns={'item_id': '京东货号', 'product_code': 'NIKE货号', 'title': '商品名',
                         'price':'京东商品价格','tmallprice':'淘宝商品价格','url':'商品URL','dt':'商品抓取时间'},
                inplace=True)

    temps.append(data)

    sql = '''
      select `shop_name` as `title`,`shop_id` as `item_id`,`product_code`,`price`,`Nikeprice`,`url`,`Nikeurl`,`dt` from(
        (select distinct `shop_name`,`price`,`dt`,`shop_id`,`url`,`product_code1` 
        as `product_code` from jd_staccato_data where TO_DAYS(NOW()) - TO_DAYS(`dt`) = 0
        ) aaa left join
        (
            select distinct `kuanhao`,`price` as `Nikeprice`,`url` as `Nikeurl`
            from t_spider_NikeStore_goods where TO_DAYS(NOW()) - TO_DAYS(`dt`) = 0 group by SUBSTRING_INDEX(`kuanhao`, '-', 1),`price`
        ) bbb
        on aaa.`product_code`=bbb.`kuanhao`
        ) where CONVERT(`price`,DECIMAL)>CONVERT(`Nikeprice`,DECIMAL);
    '''
    data=pd.read_sql(sql, conn)
    data.rename(columns={'item_id': '京东货号', 'product_code': 'NIKE货号', 'title': '商品名',
                         'price':'京东商品价格','Nikeprice':'NIKE官网价格','url':'商品URL','dt':'商品抓取时间'},
                inplace=True)
    temps.append(data)
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    for i,temp in enumerate(temps):
        sheet=plaform[i]
        if temp.empty:
            pass
        else:
            #有数据
            temp.to_excel(writer, sheet_name=sheet)
    writer.save()
    xlsx_data = output.getvalue()
    if send_excel(to_list,"%s京东商品价格对比(天级数据)"%time.strftime('%Y-%m-%d', time.localtime(time.time())),
                  "请下载附件！！！",'京东价格对比.xlsx',xlsx_data):
        print("发送成功")
    else:
        print('发送失败')

