# -*- coding: utf-8 -*-
import json
import random
from scrapy import Request
import redis
import scrapy
import time
import sys
import web,json
from dianpingshop.items import DianPIngAllStoreJson
reload(sys)
sys.setdefaultencoding('utf-8')

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())

ua_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv,2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv,2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    'Mozilla/5.0(Macintosh;U;IntelMacOSX10_6_8;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50'
]
header1 = {
        'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Accept': 'application/json, text/javascript',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8;',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }

class PetSpider(scrapy.Spider):
    name = "pet_hospital_slave_new"
    allowed_domains = ["dianping.com"]

    def start_requests(self):
        redis_ = redis.Redis(host='10.15.1.11', port=6379)
        data = redis_.lrange('dianping:pet_hospital', 0, 1000)
        # data = redis_.lrange('dianping:pet_hospital', 0, -1)
        for shop_detail in data:
            shop_detail_json = json.loads(shop_detail)
            shop_url = shop_detail_json.get('shop_url')
            shop_id = shop_url.replace('http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=','')
            url = 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?shopId=%s&platform=1&partner=150&originUrl=http://www.dianping.com/shop/%s' % (shop_id,shop_id)
            print url
            yield Request(url,headers=header1,meta={'shop_detail':shop_detail,'dt':dt},callback=self.parse_shop)



    def parse_shop(self, response):
        data = response.body
        if data:
            item = DianPIngAllStoreJson()
            item['meta'] = response.meta
            item['shop_response'] = response.body
            yield item
        else:
            yield Request(response.url, headers=header1, meta=response.meta, callback=self.parse_shop)
