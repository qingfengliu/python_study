# -*- coding: utf-8 -*-
import json

import scrapy
import time
from scrapy.http import Request
import sys
import web, re
from dianpingshop.items import DianPIngAllStoreJson
import redis

reload(sys)
sys.setdefaultencoding('utf-8')

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0'
}


class PetSpider(scrapy.Spider):
    name = "pet_hospital_tt"
    allowed_domains = ["dianping.com"]

    def start_requests(self):
        ll = [['5637847','北京','2','14','朝阳区'],
              ['22126770','石家庄','24','505','桥西区'],
              ['19484736', '上海', '1', '2', '徐汇区'],
              ['43615785', '成都', '8', '36', '金牛区']]
        for l in ll:
            url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId='+str(l[0])
            yield Request(url, callback=self.parse_shop,
                                  meta={'city_id': l[2], 'city_name': l[1], 'district_id': l[3],
                                        'district_name': l[4], 'page': 1, 'failure_time': 0,
                                        'category1_id': 95, 'category1_name': '医疗健康',
                                        'category2_id': 25148, 'category2_name': '宠物医院'},
                                  dont_filter=True, headers=header)

    def parse_shop(self,response):
        item = DianPIngAllStoreJson()
        item['meta'] = response.meta
        item['shop_response'] = response.body
        yield item

