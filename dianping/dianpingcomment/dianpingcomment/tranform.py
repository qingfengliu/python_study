import sys

import web
import json

db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')

def web_db_insert(data):
    try:
        db.insert('t_hh_dianping_shop_comments',**data)
    except:
        pass


for line in sys.stdin:
    line_json = json.loads(line)
    web_db_insert(line_json)
    #print line
