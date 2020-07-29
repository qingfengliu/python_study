# -*- coding: utf-8 -*-
import json
import random

import scrapy
import time

from scrapy import Selector
from scrapy.http import Request
import sys
import web, re
from dianpingshop.items import DianPIngAllStoreJson
import redis

reload(sys)
sys.setdefaultencoding('utf-8')

# db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
# db_update = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=3306, host='xxxx')

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=13306, host='127.0.0.1')
db_update = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=13306, host='127.0.0.1')


dt = time.strftime('%Y-%m-%d', time.localtime())

ua_list = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_2_1 like Mac OS X) AppleWebKit/602.4.6 (KHTML, like Gecko) Version/10.0 Mobile/14D27 Safari/602.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 520)',
    'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
    'Mozilla/5.0 (Linux; Android 6.0.1; SM-G900V Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.98 Mobile Safari/537.36'
]

header = {
        'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Accept': 'application/json, text/javascript',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8;',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }

Cookie = {
    'cy':2
}
# redis_ = redis.Redis(host='127.0.0.1', port=6379)
# redis_ = redis.Redis(host='10.15.1.11', port=6379)


class SearchShopSpider(scrapy.Spider):
    name = "kw_search_new"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, search_name='眼科', *args, **kwargs):
        super(SearchShopSpider, self).__init__(*args, **kwargs)
        self.search_name = search_name

    def start_requests(self):
        data = db.query(
            # "select a.city_id,a.city_name,a.amap_city,b.name from t_spider_baidu_amap_city as a left join xxxx.t_xsd_gaode_sports as b on a.amap_city=b.cityname;"
            "select distinct city_id,city_name from "
            "t_hh_dianping_business_area where city_id=2"
        )
        for d in data:
            city_id = d.city_id
            city_name = d.city_name
            url = 'https://mapi.dianping.com/searchshop.json?limit=50&sortid=0&cityid={}&keyword={}&callback=&start=0'.format(city_id,self.search_name)
            header_ = {}
            header_['User-Agent'] = random.choice(ua_list)
            yield Request(url, callback=self.parse,
                          meta={'city_id': city_id, 'city_name': city_name,'page':1,'name':self.search_name},
                          dont_filter=True, headers=header_)

    def parse(self, response):
        response_json = json.loads(response.body)
        nextStartIndex = response_json.get('nextStartIndex')
        recordCount = response_json.get('recordCount')
        meta = response.meta
        _list = response_json.get('list')
        if _list:
            for _l in _list:
                content = _l
                shop_id = content.get('id')
                url = 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?' \
                      'shopId='+str(shop_id)+'&platform=1&partner=150&originUrl=' \
                      'http%3A%2F%2Fwww.dianping.com%2Fshop%2F'+str(shop_id)
                header_ = header
                header_['Referer'] = 'http://www.dianping.com/shop/%s' % shop_id
                # header_['User-Agent'] = random.choice(ua_list)
                meta['shop_info'] = content
                meta['dt'] = dt
                meta['shop_id'] = shop_id
                yield Request(url,callback=self.parse_detail,meta=meta,headers=header_,
                              dont_filter=True,cookies=Cookie)
        if nextStartIndex!=recordCount:
            url = response.url
            url = url.split('&start=')[0]
            url = url+'&start=%s' % str(nextStartIndex)
            header_ = {}
            header_['User-Agent'] = random.choice(ua_list)
            yield Request(url, callback=self.parse,
                          meta={'city_id': meta.get('city_id'), 'city_name': meta.get('city_name'), 'page': 1, 'name': meta.get('name')},
                          dont_filter=True, headers=header_)

    def parse_detail(self,response):
        item = DianPIngAllStoreJson()
        shop_response = response.body
        print shop_response
        meta = response.meta
        if shop_response:

            item['meta'] = meta
            item['shop_response'] = shop_response
            yield item
        else:
            # shop_id = meta.get('shop_id')
            # header_ = header
            # header_['Referer'] = 'http://www.dianping.com/shop/%s' % shop_id
            # yield Request(response.url, callback=self.parse_detail, meta=meta, headers=header_, dont_filter=True)
            # print response.request.headers
            no_result = {}
            no_result['url'] = response.url
            no_result['shop_id'] = meta.get('shop_id')
            with open('no_result_item','a') as f:
                f.write(json.dumps(no_result)+'\n')

