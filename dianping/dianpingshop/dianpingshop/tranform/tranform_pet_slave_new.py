# coding=utf8
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time

import MySQLdb
import web

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
    sql = sql.replace('NULL', '0')
    print sql
    try:
        db.query(sql)
        pass
    except:
        pass
    item.pop('prev_weekly_hits')
    item.pop('today_hits')
    item.pop('weekly_hits')
    item.pop('dt')
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

    sql = "INSERT INTO t_hh_dianping_shop_info(%s) VALUES(%s)" % (key_str, value_str)
    sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
    sql = sql.replace('NULL', '0')
    print sql
    try:
        db.query(sql)
        pass
    except:
        pass


def parse(line):
    try:
        item = {}
        # print line
        result_json = json.loads(line)
        meta = result_json.get('meta')
        shop_detal = meta.get('shop_detail')
        shop_detal = json.loads(shop_detal)
        category1_name = shop_detal.get('category1_name')

        item['category1_name'] = category1_name
        category1_id = shop_detal.get('category1_id')
        item['category1_id'] = category1_id
        city_id = shop_detal.get('city_id')
        item['city_id'] = city_id
        # district_id = result_json.get('district_id')
        # item['district_id'] = district_id
        city_name = shop_detal.get('city_name')
        item['city_name'] = city_name
        # district_name = result_json.get('district_name')
        # item['district_name'] = district_name

        category2_name = shop_detal.get('shop_info').get('categoryName')
        item['category2_name'] = category2_name
        category2_id = shop_detal.get('shop_info').get('categoryId')
        item['category2_id'] = category2_id

        # dt = result_json.get('dt')
        item['dt'] = result_json.get('dt')
        shopInfo = result_json.get('shop_response')
        shopInfo = json.loads(shopInfo)
        shopInfo = shopInfo.get('msg').get('shopInfo')
        shop_id = shopInfo.get('shopId')
        # print shop_id
        item['shop_id'] = shop_id
        city_id = shopInfo.get('cityId')
        item['city_id'] = city_id
        shop_power = shopInfo.get('shopPower')
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
        # print shopInfo.get('addDate')
        create_dt = time.strftime('%Y-%m-%d', time.localtime(int(shopInfo.get('addDate')) / 1000))
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
        item['district_id'] = shopInfo.get('newDistrict')
        # item['shop_status'] = shop_info.get('status')
        item['dt'] = meta.get('dt')
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
        # print item
        web_db_insert(item)

    except Exception,e:
        print e

for line in sys.stdin:
    parse(line)