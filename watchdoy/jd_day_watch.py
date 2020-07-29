# -*- coding: utf-8 -*-

import smtplib
from email.mime.text import MIMEText
import web
import time

db = web.database(
    dbn='mysql',
    db='',
    user='writer',
    pw='xxxx',
    port=3306,
    host='xxxx'
)

to_list = [
    'chen.zzbj@belle.com.cn',
    # 'fxwang@.com',
    # 'wguan@.com'
]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

# 对于大型的邮件服务器，有反垃圾邮件的功能，必须登录后才能发邮件，如126,163
mail_server = "smtp.126.com"  # 126的邮件服务器
mail_login_user = "spider_man_warn@126.com"  # 必须是真实存在的用户，这里我测试的时候写了自己的126邮箱
mail_passwd = ""  # 必须是对应上面用户的正确密码，我126邮箱对应的密码


def send_mail(to_list, sub, content):
    """
        to_list:发给谁
        sub:主题
        content:内容
        send_mail("aaa@126.com","sub","content")
    """
    me = mail_login_user + "<" + mail_login_user + ">"
    msg = MIMEText(content)
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    s = smtplib.SMTP()
    s.connect(mail_server)
    s.login(mail_login_user, mail_passwd)
    s.sendmail(me, to_list, msg.as_string())
    s.close()
    return True


if __name__ == '__main__':
    sql = """
    SELECT 
    `description`,'未调用' as `statics`
FROM
    t_platform_task
WHERE
    `project_id` = 144 AND `ttype` = 1
        AND `spider` LIKE 'shangzhi%'
        and `description` NOT IN ('t_spider_jdweizhi_orders_client') # orders_client是两天前的数据
        AND `description` NOT IN (SELECT DISTINCT
            `spider_name`
        FROM
            .t_spider_jdweizhi_spider
        WHERE
            TO_DAYS(NOW()) - TO_DAYS(`dt`) <= 1) 
# 以上部分是爬虫平台没有运行的task
UNION SELECT 
    `spider_name`,'执行失败' as `statics`
FROM
    .t_spider_jdweizhi_spider
WHERE
    TO_DAYS(NOW()) - TO_DAYS(`dt`) <= 1
        AND `flag` = 0
# 以上部分是执行错误的
UNION select * from ( 

SELECT 
    `description`,'未调用' as `statics`
FROM
    t_platform_task
WHERE
    `project_id` = 144 AND `ttype` = 1
        AND `spider` LIKE 'shangzhi%'
        and `description` IN ('t_spider_jdweizhi_orders_client')
        AND `description` NOT IN (SELECT DISTINCT
            `spider_name`
        FROM
            .t_spider_jdweizhi_spider
        WHERE
            TO_DAYS(NOW()) - TO_DAYS(`dt`) = 2)
UNION SELECT 
    `spider_name`,'执行失败' as `statics`
FROM
    .t_spider_jdweizhi_spider
WHERE
    TO_DAYS(NOW()) - TO_DAYS(`dt`) <= 2
        AND `flag` = 0
        AND `spider_name`='t_spider_jdweizhi_orders_client') aaa;
# 对于两前的天级别数据进行判断
    """
    data_str = '爬虫执行状态(%s):' % time.strftime('%Y-%m-%d', time.localtime(time.time())) + '\n'
    temps = [i for i in db.query(sql)]
    if not temps:
        data_str += '今日京东天级别数据爬取完毕！！！无异常'
    else:
        for i in temps:
            data_str += i.get('description') + ',' + i.get('statics') + '\n'
    if send_mail(to_list, "%s京东爬虫抓取情况(天级数据)" % time.strftime('%Y-%m-%d', time.localtime(time.time())), data_str):
        print("发送成功")
    else:
        print('发送失败')
