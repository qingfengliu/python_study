import sys
import json

import time
import web
reload(sys)
sys.setdefaultencoding('utf-8')

# db = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=3306, host='xxxx')
db = web.database(dbn='mysql', db='o2o', user='root', pw='123456', port=3306, host='127.0.0.1')
# db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


def multi_insert(table,data_list):

    try:
        db.multiple_insert(table, values=data_list)
    except Exception, e:
        print e

def db_insert(table,data):
    try:
        db.insert(table,**data)
    except Exception,e:
        print e

def parse(line):
    print line
    line_json = json.loads(line)
    # print line_json
    meta = line_json.get('meta')
    response_content = line_json.get('response_content')
    # print meta
    city_id = meta.get('city_id')
    city_name = meta.get('city_name')
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
            item['category1_id'] = '1'
            item['category1_name'] = '1'
            item['category2_id'] = '1'
            item['category2_name'] = '1'
            item['group_id'] = shopRecordBean.get('shopGroupID')
            item['group_name'] = shopRecordBean.get('shopName')
            create_dt = shopRecordBean.get('addDate')
            if create_dt:
                create_dt = create_dt.split('T')[0]
            item['create_dt'] = create_dt
            item['city_id'] = city_id
            item['city_name'] = city_name
            item['district_id'] = '1'
            item['district_name'] = '1'
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
            db_insert('t_hh_dianping_shop_info',item)
        #     if len(data_list)>1000:
        #         multi_insert('t_hh_dianping_shop_info',data_list)
        #         data_list = []
        #     else:
        #         data_list.append(item)
        # if len(data_list)>0:
        #     multi_insert('t_hh_dianping_shop_info', data_list)





# for line in sys.stdin:
#     if len(line)>10:
#         parse(line.replace('\n','')[:-1])
def open_file():
    with open('shop_temp.json') as f:
        data = f.readlines()
    for d in data:
        line = d
        if len(line)>10:
            parse(line.replace('\n','')[:-1])

if __name__ == '__main__':
    open_file()

# vote_default,

# shop_status,
