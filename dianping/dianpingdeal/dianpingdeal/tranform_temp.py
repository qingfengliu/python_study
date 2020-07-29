import sys

import time

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import web
import json

db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')

def web_db_insert(item):
#     try:
#         db.insert('t_hh_dianping_tuangou_deal_info',**data)
#     except:
#         pass
    key_str = ','.join('`%s`' % k for k in item.keys())
    value_str = ','.join(
        'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v) for v in
        item.values())
    kv_str = ','.join(
        "`%s`=%s" % (k, 'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v))
        for (k, v)
        in item.items())
    print kv_str
    print key_str

    sql = "INSERT INTO t_hh_dianping_tuangou_deal_info(%s) VALUES(%s)" % (key_str, value_str)
    sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
    print sql
    # with open('dddddddd','a') as f:
    #     f.write(sql)
    # time.sleep(100)
    db.query(sql)
    # return item


def write_file(item):
    list_ = []
    # print item
    for key,value in item.items():
        list_.append(str(value))
    lll = ','.join(list_)
    print lll
    if '25951670' in lll:
        with open('25951670_', 'a') as f:
            f.write(lll)

for line in sys.stdin:
    try:
        line = line.replace('"},','"}')
        line_json = json.loads(line)
        new_price = line_json.get('new_price')
        if not new_price:
            line_json['new_price'] = 0
        old_price = line_json.get('old_price')
        if not old_price or old_price == []:
            line_json['old_price'] = 0
        sales = line_json.get('sales')
        if not sales or sales == []:
            line_json['sales'] = 0
        web_db_insert(line_json)
        # write_file(line_json)
    except Exception,e:
        print e