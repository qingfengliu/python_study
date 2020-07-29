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

# redis_ = redis.Redis(host='127.0.0.1', port=6379)
# redis_ = redis.Redis(host='10.15.1.11', port=6379)


class SearchShopSpider(scrapy.Spider):
    name = "search_shop"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, search_name='视光', *args, **kwargs):
        super(SearchShopSpider, self).__init__(*args, **kwargs)
        self.search_name = search_name

    def start_requests(self):
        data = db.query(
            "select distinct city_id,city_name from "
            "t_hh_dianping_business_area;")


        for d in data:
            city_id = d.city_id
            city_name = d.city_name

            url = 'https://www.dianping.com/search/map/ajax/json?cityId={}&keyword={}'.format(
                city_id,self.search_name)
            print url
            yield Request(url, callback=self.parse,
                          meta={'city_id': city_id, 'city_name': city_name,'page':1},
                          dont_filter=True, headers=header)
        # url = 'https://www.dianping.com/search/map/ajax/json?cityId={}&keyword={}'.format(
        #         16,self.search_name)
        # yield Request(url, callback=self.parse,
        #               meta={'city_id': 16, 'city_name': '武汉','page':1},
        #               dont_filter=True, headers=header, errback=self.parse_failure)


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
                        yield Request(next_page_link, callback=self.parse, meta=meta, dont_filter=True, headers=header)

    def parse_failure1(self, failure):
        meta = failure.request.meta
        meta['retry_times'] = 0
        failure_time = meta['failure_time']
        if failure_time < 5:
            meta['failure_time'] += 1
            error_resion = failure.value
            redis_.hset('dianping:error_resion', str(error_resion)[:38], '1')
            if 'Connection refused' in str(error_resion) or 'timeout' in str(
                    error_resion) or 'Could not open CONNECT tunnel with proxy' in str(
                error_resion) or 'TCP connection timed out' in str(error_resion) or 'twisted' in str(error_resion):
                # print type(error_resion)
                url = failure.request.url
                if 'search' in url:
                    yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True, meta=meta,
                                  headers=header)
            else:
                try:
                    error_resion = failure.value.response._body
                    if 'aboutBox errorMessage' in error_resion or '没有找到相应的商户' in error_resion:
                        print error_resion
                        print error_resion
                        print error_resion
                        print error_resion
                    else:
                        url = failure.request.url
                        if 'search' in url:
                            yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True,
                                          meta=meta,
                                          headers=header)
                except Exception, e:
                    print e

    def parse_failure(self, failure):
        meta = failure.request.meta
        meta['retry_times'] = 0
        failure_time = meta['failure_time']
        if failure_time < 50:
            meta['failure_time'] = failure_time + 1
            try:
                error_resion = failure.value
                # redis_.hset('dianping:error_resion', str(error_resion)[:38], '1')
                error_resion = failure.value.response._body
                if '没有找到相应的商户' in error_resion or '您要查看的内容不存在' in error_resion:
                    url = failure.value.response.url
                    url_list = url.split('r')
                    print len(url_list)
                    if len(url_list) == 4:
                        url = url.replace('r%s' % url_list[-1], 'c%s' % url_list[-1])
                        yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True,
                                      meta=meta,
                                      headers=header)
                    else:
                        pass
                else:
                    if 'aboutBox errorMessage' in error_resion or '您要查看的内容不存在' in error_resion:
                        # if '您要查看的内容不存在' in error_resion:
                        # print error_resion
                        print '========================='
                        print failure.value.response.url
                        # redis_.hset('dianping:bucunzai', failure.value.response.url, '1')
                        print '========================='
                        # print error_resion
                        # print error_resion
                        # print error_resion
                    else:
                        url = failure.request.url
                        if 'search' in url:
                            yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True,
                                          meta=meta,
                                          headers=header)
            except Exception, e:
                url = failure.request.url
                if 'search' in url:
                    yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True,
                                  meta=meta,
                                  headers=header)
