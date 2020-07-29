# -*- encoding:utf-8 -*-
import sys
import json
import re

reload(sys)
sys.setdefaultencoding('utf-8')

def get_json_hierarchy(_json_obj, arch_ele_list):
    for e in arch_ele_list:
        if e not in _json_obj:
            return None
        _json_obj = _json_obj[e]
    return _json_obj

def format_list(data):
    result = []
    if data:
        for item in data:
            tmp = ''
            if item:
                if type(item) == unicode:
                    tmp = item.encode('utf-8')
                    tmp = tmp.replace('\u0001', '')
                    tmp = tmp.replace('\n', ' ')
                    tmp = tmp.replace('\t', ' ')
                    tmp = tmp.replace('\r', ' ')
                    tmp = tmp.strip()
                elif type(item) == int:
                    tmp = str(item)
                elif type(item) == str:
                    tmp = item.encode('utf-8').replace("\u0001", '')
                    tmp = tmp.replace('\n', ' ')
                    tmp = tmp.replace('\t', ' ')
                    tmp = tmp.replace('\r', ' ')
                    tmp = tmp.decode('utf-8').strip()
                else:
                    tmp = item
            result.append(tmp)
    return result

def parse_hive(item):
    result = []
    result.append('')
    result.append(item['shop_id'])
    result.append(item['shop_name'])
    result.append(item['category1_id'])
    result.append(item['category1_name'])
    result.append(item['category2_id'])
    result.append(item['category2_name'])
    result.append('')
    result.append('')
    result.append(item['group_id'])
    result.append(item['group_name'])
    result.append(item['create_dt'])
    result.append('')
    result.append(item['city_id'])
    result.append(item['city_name'])
    result.append(item['district_id'])
    result.append(item['district_name'])
    result.append(item['biz_id'])
    result.append(item['biz_name'])
    result.append(item['address'])
    result.append(str(item['lng']))
    result.append(str(item['lat']))
    result.append(item['avg_price'])
    result.append(item['shop_power'])
    result.append(item['shop_power_title'])
    result.append(item['branch_total'])
    result.append(item['dish_tags'])
    result.append(item['display_score'])
    result.append('')
    result.append('')
    result.append('')
    result.append('')
    result.append('')
    result.append('')
    result.append(item['hits'])
    result.append(item['month_hits'])
    result.append(item['phone_no'])
    result.append(item['pic_total'])
    result.append(item['popularity'])
    result.append(item['primary_tag'])
    result.append(item['shop_tags'])
    result.append(item['shop_type'])
    result.append(item['vote_total'])
    result.append('')
    result.append(item['wish_total'])
    result.append('')
    result.append('')
    result.append('')
    result.append(item['dt'])
    print "\t".join(format_list(result))

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
        for shopRecordBeanl in shopRecordBeanList:
            item = {}
            # print shopRecordBean
            shopRecordBean = shopRecordBeanl.get('shopRecordBean')
            # print shopRecordBean
            item['shop_id'] = shopRecordBean.get('shopId')
            shop_name = shopRecordBean.get('shopName')
            branchName = shopRecordBean.get('branchName')
            if branchName:
                item['shop_name'] = shop_name+'(%s)' % branchName
            else:
                item['shop_name'] = shop_name
            item['category1_id'] = category1_id
            item['category1_name'] = category1_name
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
            item['dt'] = meta.get('dt')
            parse_hive(item)

for line in sys.stdin:
    parse(line)
    # line = json.loads(line)
    # content = line
    # result = []
    # result.append(get_json_hierarchy(content, ['goods_id']))
    # result.append(get_json_hierarchy(content, ['shop_url']))
    # result.append(get_json_hierarchy(content, ['category_id']))
    # result.append(get_json_hierarchy(content, ['user_id']))
    # result.append(get_json_hierarchy(content, ['shop_name']))
    # result.append(get_json_hierarchy(content, ['dt']))
    # result.append(get_json_hierarchy(content, ['fg_category_id']))

    # print "\t".join(format_list(result))