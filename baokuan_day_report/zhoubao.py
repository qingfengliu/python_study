import argparse
import pymysql
import time
import web
import datetime
'''
  `commodity_style_no`  '款号',   1
   `commodity_pic_url`  '图片链接', 2
   `nianfenji` varchar(60)  '年份季',  3
   `commodity_brand_name`  '品牌',    4
   `commodity_catname_one`  '一级分类 女鞋男鞋',    5
   `commodity_catname_two`  '二级分类', 6
   `commodity_catname_three` '三级分类',    7    
   `channel`  '渠道', 8
   `commodity_first_sell_date` '发布天数',  9
   `payitemqty_paixu_benzhou` '当周销售排名', 10
   `payitemqty_paixu_shangzhou` '前一周销售排名',  11
   `payitemqty_paixu_bianhua` '销售排名变化', 12
   `nianfenji_payitemqty_paixu_benzhou` '当周季分组销售排名',    13
   `nianfenji_payitemqty_paixu_shangzhou` '前一周季分组销售排名', 14
   `nianfenji_payitemqty_paixu_bianhua` '季分组销售排名变化',    15
   `payitemqty_benzhou` '当周销售量',    16
   `payitemqty_shangzhou` '前一周销售量', 17
   `payitemqty_bianhua` '销售涨跌幅',    18
   `itemuv_paixu_benzhou` '当周访客排名', 19
   `itemuv_paixu_shangzhou` '前一周访客排名',  20
   `itemuv_paixu_bianhua` '访客排名变化', 21
   `nianfenji_itemuv_paixu_benzhou` '当周季分组访客排名',    22
   `nianfenji_itemuv_paixu_shangzhou` '前一周季分组访客排名', 23
   `nianfenji_itemuv_paixu_bianhua` '季分组访客排名变化',    24
   `itemuv_benzhou` '当周访客量',    25
   `itemuv_shangzhou` '前一周访客量', 26
   `itemuv_binahua` '访客涨跌幅',    27
   `view_label` '访客量季分组标签', 28
   `pay_avg_change` '支付转化率',    29  
   `nianfenji_pay_change_paixu` '支付转化率季分组排名',   30
   `payrate_label` '支付转化率季分组标签',    31
   `add_avg_change` '加购转化率',    32
   `addcartitemcnt` '加车人数', 33
   `favbuyercnt` '收藏人数',    34
   `add_shou_change` '加购收藏转化率', 35
   `nianfenji_add_shou_change_paixu` '加购收藏转化率季分组排名',    36
   `addrate_label` '加购收藏转化率季分组标签',  37
   `pay_asp_benzhou` '当周平均支付均价',    38
   `pay_asp_shangzhou` '前一周平均支付均价', 39
   `pay_asp_bianhua` '支付价格涨跌幅', 40
   `pay_sale_rate` '折扣率',   41
   `pay_rate` '毛利率',    42
   `kesale_quantit` '可售库存', 43
   `notship_quantity` '未出货',    44
   `non_arrival` '订补未到',    45
   'zongjie'    '总结'          46
   'jianyi'     '建议'          47
   `report_week` '统计时间',    48
'''
def sql_query(tablename,**values):

    def q(x):
        return "(" + x + ")"
    sql_query=''
    if values:
        sorted_values = sorted(values.items(), key=lambda t: t[0])
        _keys = web.SQLQuery.join(map(lambda t: t[0], sorted_values), ', ')
        _values = web.SQLQuery.join([web.sqlparam(v) for v in map(lambda t: t[1], sorted_values)], ', ')
        sql_query = "replace INTO %s " % tablename + q(_keys) + ' VALUES ' + q(_values)
    return sql_query.__str__()

def time_parse(t):
    t = int(t)
    today = datetime.date.today()
    oneday = datetime.timedelta(days=t)
    yesterday = today - oneday
    return yesterday.strftime("%Y-%m-%d")

def get_attr():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--start_time",help="start time of script")
    args = parser.parse_args()
    try :
        start_time=args.start_time
    except Exception as e:
        start_time=time_parse(1)
    if not start_time:
        start_time = time_parse(1)
    return start_time
db_ali=web.database(dbn='mysql', db='belle', user='yougou', pw='09E636cd', port=3306, host='rm-m5e2m5gr559b3s484.mysql.rds.aliyuncs.com')

temp = db_ali.query('select * from belle.dm_commodity_style_week_brand where `report_week`>="%s";' %get_attr())
temp=[x for x in temp]

view_label={'middle':'中访客','high':'高访客','low':'低访客'}
payrate_label={'middle':'中支付转化','high':'高支付转化','low':'低支付转化'}
addrate_label={'middle':'中收藏加购转化','high':'高收藏加购转化','low':'低收藏加购转化'}
jianyi={'低0高':'增加流量','高0高':'可以考虑增加流量','高0低':'重点关注查找问题','高低中':'需关注可寻找问题'}
'''
jianyi  key组成:高、中、低、0   其中0代表没有指定这个项目;
        ____    _____      ____
        访客   支付转化  收藏加购转化
'''
conn = pymysql.connect(host='', port=3306, user='work',
                       passwd='', db='', charset='utf8', connect_timeout=50000,
                       cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()
for i,tmp in enumerate(temp):
    tmp['view_label'] = view_label[tmp['view_label']]
    tmp['payrate_label'] = payrate_label[tmp['payrate_label']]
    tmp['addrate_label'] = addrate_label[tmp['addrate_label']]
    tmp['zongjie'] =tmp['view_label']+ tmp['addrate_label']+tmp['payrate_label']
    jianyi_jieguo=''
    for k in jianyi:
        tmp_view=k[0]
        tmp_payrate=k[1]
        tmp_addrate=k[2]
        if tmp_view=='0' or tmp_view==tmp['view_label'][0]:
            if tmp_payrate=='0' or tmp_view==tmp['payrate_label'][0]:
                if tmp_addrate=='0' or tmp_addrate==tmp['addrate_label'][0]:
                    jianyi_jieguo=jianyi[k]
    tmp['jianyi']=jianyi_jieguo
    sql=sql_query('.dm_commodity_style_week_report',**tmp)
    cur.execute(sql)
    if i%1000==0:
        conn.commit()
        cur.close()
        conn.close()
        conn = pymysql.connect(host='', port=3306, user='work',
                               passwd='', db='', charset='utf8', connect_timeout=50000,
                               cursorclass=pymysql.cursors.DictCursor)
        cur = conn.cursor()
conn.commit()
cur.close()
conn.close()

