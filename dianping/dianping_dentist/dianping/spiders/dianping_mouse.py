# -*- coding: utf-8 -*-
import scrapy
import re
import json
from dianping.items import DianpingItem
import web


db = web.database(dbn='mysql', db='o2o', user='xxxx', pw='xxxx', port=3306, host='xxxx')
headers = {
    "User-Agent": "Mozilla/5.0 (compatible; Baiduspider-render/2.0;+http://www.baidu.com/search/spider.html)",
    "referer": 'www.baidu.com'
}


class Dianping_Mouse(scrapy.Spider):

    name = 'dianping_mouse_spider'

    def start_requests(self):
        data = list(db.query("select city_name,city_id,district_name,district_id from t_hh_dianping_business_area"))
        for item in data[62761:62982]:
            page = 1
            url = 'http://www.dianping.com/search/map/ajax/json?cityId=' + str(item.city_id) + \
            '&categoryId=182&regionId=' + str(item.district_id) + '&page=' + str(page)
            city_name = item.city_name
            district_name = item.district_name
            yield scrapy.Request(url, headers=headers, meta={'city_name': city_name, 'district_name': district_name,
                                                             'page': page, 'city_id': item.city_id,
                                                             'district_id': item.district_id}, callback=self.jsonparse,
                                 dont_filter=True)

    def jsonparse(self, response):
        content = json.loads(response.body)
        city_name = response.meta['city_name']
        district_name = response.meta['district_name']
        page = response.meta['page']
        city_id = response.meta['city_id']
        district_id = response.meta['district_id']
        shop_list = content['shopRecordBeanList']
        for info in shop_list:
            items = DianpingItem()
            items['city_name'] = city_name
            items['district_name'] = district_name
            items['shop_id'] = info['shopId']
            items['brand_name'] = info['shopRecordBean']['shopName']
            items['shop_name'] = info['shopRecordBean']['shopTotalName']
            items['shop_address'] = info['address']

            yield items
        pagecount = content['pageCount']
        if page < int(pagecount):
            page += 1
            url = 'http://www.dianping.com/search/map/ajax/json?cityId=' + str(city_id) + \
                  '&categoryId=182&regionId=' + str(district_id) + '&page=' + str(page)
            yield scrapy.Request(url, headers=headers, meta={'city_name': city_name, 'district_name': district_name,
                                                             'page': page, 'city_id': city_id,
                                                             'district_id': district_id}, callback=self.jsonparse,
                                 dont_filter=True)