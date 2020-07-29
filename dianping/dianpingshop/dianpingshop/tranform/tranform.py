import sys
import json

import time

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import web


# db = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=3306, host='xxxx')
# db = web.database(dbn='mysql', db='o2o', user='root', pw='123456', port=3306, host='127.0.0.1')
db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


def multi_insert(table,data_list):

    try:
        db.multiple_insert(table, values=data_list)
    except Exception, e:
        print e


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

    sql = "INSERT INTO t_hh_dianping_shop_info_search(%s) VALUES(%s)" % (key_str, value_str)
    sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
    print sql
    # with open('dddddddd','a') as f:
    #     f.write(sql)
    # time.sleep(100)
    try:
        db.query(sql)
    except Exception,e:
        print e
        pass
    # return item


def parse(line):
    line_json = json.loads(line)
    # print line_json
    meta = line_json.get('meta')
    response_content = line_json.get('response_content')
    # print meta
    city_id = meta.get('city_id')
    city_name = meta.get('city_name')
    district_id = meta.get('district_id')
    district_name = meta.get('district_name')
    category1_id = meta.get('category1_id')
    category1_name = meta.get('category1_name')
    category2_id = meta.get('category2_id')
    category2_name = meta.get('category2_name')
    response_content = json.loads(response_content)
    # print response_content
    shopRecordBeanList = response_content.get('shopRecordBeanList')
    if shopRecordBeanList:
        data_list = []
        for shopRecordBeanl in shopRecordBeanList:
            item = {}
            # print shopRecordBean
            shopRecordBean = shopRecordBeanl.get('shopRecordBean')
            # print shopRecordBean
            item['shop_id'] = shopRecordBean.get('shopId')
            item['shop_name'] = shopRecordBean.get('shopName')
            # if not category1_id:
            category1_id = shopRecordBean.get('shopType')
            item['category1_id'] = category1_id

            item['category1_name'] = category1_name
            # if not category2_id:
            category2_id = shopRecordBean.get('categoryList')
            if category2_id:
                category2_id = category2_id[0]
            item['category2_id'] = category2_id

            item['category2_name'] = category2_name
            item['group_id'] = shopRecordBean.get('shopGroupID')
            item['group_name'] = shopRecordBean.get('shopName')
            create_dt = shopRecordBean.get('addDate')
            if create_dt:
                create_dt = create_dt.split('T')[0]
            item['create_dt'] = create_dt
            item['city_id'] = city_id
            item['city_name'] = city_name
            item['district_id'] = district_id
            item['district_name'] = district_name
            item['biz_id'] = shopRecordBean.get('bussinessAreaId')
            item['biz_name'] = shopRecordBean.get('bizRegionName')
            item['address'] = shopRecordBean.get('address')
            item['lng'] = shopRecordBean.get('geoLng')
            item['lat'] = shopRecordBean.get('geoLat')
            item['avg_price'] = shopRecordBean.get('avgPrice')
            item['shop_power'] = shopRecordBean.get('shopPower')
            item['shop_power_title'] = shopRecordBean.get('shopPowerTitle')
            item['branch_total'] = shopRecordBean.get('branchTotal')
            item['dish_tags'] = shopRecordBean.get('dishTags')
            item['display_score'] = shopRecordBean.get('displayScore')
            item['display_score1'] = shopRecordBean.get('displayScore1')
            item['display_score2'] = shopRecordBean.get('displayScore2')
            item['display_score3'] = shopRecordBean.get('displayScore3')
            item['hits'] = shopRecordBean.get('hits')
            item['month_hits'] = shopRecordBean.get('monthlyHits')
            item['phone_no'] = shopRecordBean.get('phoneNo')
            item['pic_total'] = shopRecordBean.get('picTotal')
            item['popularity'] = shopRecordBean.get('popularity')
            item['primary_tag'] = shopRecordBean.get('primaryTag')
            item['shop_tags'] = shopRecordBean.get('shopTags')
            item['shop_type'] = shopRecordBean.get('shopType')
            item['vote_total'] = shopRecordBean.get('voteTotal')
            item['wish_total'] = shopRecordBean.get('wishTotal')
            item['last_update_dt'] = time.strftime('%Y-%m-%d', time.localtime())
            web_db_insert(item)
        #     if len(data_list)>1000:
        #         multi_insert('t_hh_dianping_shop_info',data_list)
        #         data_list = []
        #     else:
        #         data_list.append(item)
        # if len(data_list)>0:
        #     multi_insert('t_hh_dianping_shop_info', data_list)





for line in sys.stdin:
    parse(line)
    # print line



# vote_default,

# shop_status,
