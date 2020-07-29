# coding=utf8
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import time

import MySQLdb
import web
dt = time.strftime('%Y-%m-%d', time.localtime())
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
    # print kv_str
    # print key_str

    sql = "INSERT INTO t_hh_dianping_shop_info_pet_hospital(%s) VALUES(%s)" % (key_str, value_str)
    sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
    sql = sql.replace('NULL','0')
    print sql
    db.query(sql)

def parse(line):
    item = {}
    line_json = json.loads(line)
    shop_response = line_json.get('shop_response')
    # meta = line_json.get('meta')
    # shop_info = meta.get('shop_info')

    shop_response_json = json.loads(shop_response)
    shopInfo = shop_response_json.get('msg').get('shopInfo')

    shop_id = shopInfo.get('shopId')
    item['shop_id'] = shop_id
    category1_id = shopInfo.get('shopType')
    item['category1_id'] = category1_id
    # category2_id = shop_info.get('categoryId')
    # item['category2_id'] = category2_id
    # category2_name = shop_info.get('categoryName')
    # item['category2_name'] = category2_name
    category1_name = '宠物'
    item['category1_name'] = category1_name
    city_id = shopInfo.get('cityId')
    item['city_id'] = city_id
    # biz_name = shop_info.get('regionName')
    # item['biz_name'] = biz_name
    shop_power = shopInfo.get('power')
    item['shop_power'] = shop_power






    shopGroupId = shopInfo.get('shopGroupId')
    item['group_id'] = shopGroupId
    hits = shopInfo.get('hits')
    if hits:
        item['hits'] = hits
    else:
        item['hits'] = 0
    monthlyHits = shopInfo.get('monthlyHits')
    if monthlyHits:
        item['month_hits'] = monthlyHits
    else:
        item['month_hits'] = 0
    create_dt = time.strftime('%Y-%m-%d',time.localtime(int(shopInfo.get('addDate'))/1000))
    item['create_dt'] = create_dt
    district_id = shopInfo.get('newDistrict')
    item['district_id'] = district_id
    address = shopInfo.get('address')
    item['address'] = address
    lng = shopInfo.get('glng')
    item['lng'] = lng
    lat = shopInfo.get('glat')
    item['lat'] = lat
    avgPrice = shopInfo.get('avgPrice')
    item['avg_price'] = avgPrice
    branch_total = shopInfo.get('branchTotal')
    item['branch_total'] = branch_total

    shop_name = shopInfo.get('shopName')
    branchName = shopInfo.get('branchName')
    if branchName:
        item['shop_name'] = shop_name + '(%s)' % branchName
    else:
        item['shop_name'] = shop_name
    item['display_score'] = shopInfo.get('score')
    item['display_score1'] = shopInfo.get('score1')
    item['display_score2'] = shopInfo.get('score2')
    item['display_score3'] = shopInfo.get('score3')
    phoneNo = shopInfo.get('phoneNo')
    item['phone_no'] = phoneNo
    item['pic_total'] = shopInfo.get('picTotal')
    item['popularity'] = shopInfo.get('popularity')
    item['shop_type'] = shopInfo.get('newShopType')
    item['vote_total'] = shopInfo.get('voteTotal')
    item['wish_total'] = shopInfo.get('wishTotal')
    # item['shop_status'] = shop_info.get('status')
    item['dt'] = dt
    prevWeeklyHits = shopInfo.get('prevWeeklyHits')
    if prevWeeklyHits:
        item['prev_weekly_hits'] = prevWeeklyHits
    else:
        item['prev_weekly_hits'] = 0
    todayHits = shopInfo.get('todayHits')
    if todayHits:
        item['today_hits'] = todayHits
    else:
        item['today_hits'] = 0
    weeklyHits = shopInfo.get('weeklyHits')
    if weeklyHits:
        item['weekly_hits'] = weeklyHits
    else:
        item['weekly_hits'] = 0
    web_db_insert(item)

for line in sys.stdin:
    parse(line)