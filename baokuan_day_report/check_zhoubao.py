import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
from io import BytesIO
import pymysql
import pandas as pd
import matplotlib.dates as dates
from matplotlib.dates import AutoDateLocator, DateFormatter
to_list = ['qfliu@.com','wguan@.com','wen.xp.sz@belle.com.cn','fxwang@.com','zyliu@.com']
# 接收邮件，可设置为你的QQ邮箱或者其他邮箱
#
#对于大型的邮件服务器，有反垃圾邮件的功能，必须登录后才能发邮件，如126,163
mail_server="smtp.exmail.qq.com"         # 126的邮件服务器
mail_login_user="qfliu@.com"   #必须是真实存在的用户，这里我测试的时候写了自己的126邮箱
mail_passwd="xxxx"               #必须是对应上面用户的正确密码，我126邮箱对应的密码

def plot_curve1(date,data,title):
    fig1 = plt.figure(figsize=(15,5))
    ax1 = fig1.add_subplot(1,1,1)
    ax1.xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d'))#设置时间标签显示格式
    date['report_week'] = pd.to_datetime(date['report_week'])
    date.set_index('report_week',inplace=True)
    x1=date.index.to_pydatetime()
    y1=date.values
    yearsFmt = DateFormatter('%m-%d')
    ax1.xaxis.set_major_locator(dates.DayLocator())  # 设置时间间隔
    ax1.xaxis.set_major_formatter(yearsFmt)  # 设置时间显示格式
    for a, b in zip(x1, y1):
        plt.text(a, b, '%d' % b, ha='center', va='bottom', fontsize=9)
        
    plt.plot_date(x1, y1, 'v-')
    for label in ax1.xaxis.get_ticklabels():
        label.set_rotation(90)


conn = pymysql.connect(host='',port=3306 ,user='work', passwd='', db='o2o', charset='utf8',
                                   connect_timeout=5000, cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()
cur.execute(' SELECT `report_week`,count(*) as `count` FROM .dm_commodity_style_week_report group by `report_week` having TO_DAYS( NOW( ) ) - TO_DAYS( report_week) <= 30;')
temp=cur.fetchall()
cur.execute('SELECT count(*) as `count` FROM .dm_commodity_style_week_report group by `report_week` having TO_DAYS( NOW( ) ) - TO_DAYS( report_week)=1; ')
today=cur.fetchall()
cur.close()
conn.close()
temp=pd.DataFrame(temp)
if today:
    today=int(today[0]['count'])
else:
    today=0
f = BytesIO()
plot_curve1(temp,temp.values,'近30天item')
plt.savefig(f,format='png')
# temp=base64.b64encode(f.getvalue())
img=f.getvalue()


def get_table_content(imgf):
    me = mail_login_user + "<" + mail_login_user + ">"
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = '爆款周报数据迁移情况'
    msgRoot['From'] = me
    msgRoot['To'] = ";".join(to_list)
    html = """\
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <body>
    <b>今天item数</b>:%s<br>
    <b>Some 近30天数据迁移表格:</b><br>
    <img src="cid:1"><br>
    </body>
    </html>
          """ %today
    msgText = MIMEText(html, _subtype='html', _charset='utf-8')  # 解决乱码
    msgRoot.attach(msgText)
    msgImage = MIMEImage(imgf)
    msgImage.add_header('Content-ID', '<' + str(1) + '>')
    msgRoot.attach(msgImage)
    s = smtplib.SMTP()
    s.connect(mail_server)
    s.login(mail_login_user,mail_passwd)
    s.sendmail(me, to_list, msgRoot.as_string())
    s.close()

get_table_content(img)