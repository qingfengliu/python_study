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


header1 = {
    'Host': 'www.dianping.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate',
    'Cookie': 'cy=2; _lxsdk_cuid=15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8; _lxsdk=15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8; _hc.v=957103d7-ead0-5cba-a1c8-c987a61dd6cf.1510646941; s_ViewType=10; aburl=1; cye=hongkong; __mta=141996557.1512988506320.1513153215300.1513250934951.3; JSESSIONID=530162FCCE7C09FCB3438EA73C3AFC3D; _lxsdk_s=160677c8599-63-020-abb%7C%7C26; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache'
}


class SearchShopSpider(scrapy.Spider):
    name = "shop_cate_search"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, search_cate='157', *args, **kwargs):
        super(SearchShopSpider, self).__init__(*args, **kwargs)
        self.search_cate = search_cate

    def start_requests(self):
        data = db.query(
            "select distinct city_id,city_name from t_hh_dianping_business_area where city_name in ('北京','上海','杭州','重庆');")

        for d in data:
            city_id = d.city_id
            city_name = d.city_name
            # url = 'http://www.dianping.com/search/map/ajax/json?cityId=%s&categoryId=%s' % (city_id, self.search_cate)
            url = 'http://www.dianping.com/search/map/ajax/json?cityId=%s&keyword=星客多' % (city_id)
            print url
            yield Request(url, callback=self.parse,
                          meta={'city_id': city_id, 'city_name': city_name, 'page': 1},
                          dont_filter=True, headers=header1)

    def parse(self, response):
        page = response.meta['page']
        response_json = json.loads(response.body)
        meta = response.meta
        meta['retry_times'] = 0
        if response_json:
            shopRecordBeanList = response_json.get('shopRecordBeanList')
            if shopRecordBeanList:
                # for shopRecordBean1 in shopRecordBeanList:
                item = DianPIngAllStoreJson()
                item['response_content'] = response.body
                meta['search_kw_cate'] = self.search_cate
                item['meta'] = meta
                yield item

        if page == 1:
            # 找到最有一页的页码，比对是否为当前页
            next_page = response_json.get('pageCount')
            if next_page:
                # print next_page
                if int(next_page) == page:
                    pass
                else:
                    for i in xrange(2, int(next_page) + 1):
                        meta['page'] = i
                        next_page_link = response.url + '&page=%s' % i
                        print next_page_link
                        yield Request(next_page_link, callback=self.parse, meta=meta, dont_filter=True, headers=header1)
