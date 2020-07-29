# -*- coding: utf-8 -*-
import json

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

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
db_update = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0'
}


# redis_ = redis.Redis(host='127.0.0.1', port=6379)
# redis_ = redis.Redis(host='10.15.1.11', port=6379)


class SearchShopSpider(scrapy.Spider):
    name = "kw_search_shop"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, search_name='座上客', *args, **kwargs):
        super(SearchShopSpider, self).__init__(*args, **kwargs)
        self.search_name = search_name

    def start_requests(self):
        data = db.query(
            # "select a.city_id,a.city_name,a.amap_city,b.name from t_spider_baidu_amap_city as a left join xxxx.t_xsd_gaode_sports as b on a.amap_city=b.cityname;"
            "select a.city_id,a.city_name,a.baidu_city,b.name from t_spider_baidu_amap_city as a left join xxxx.t_xsd_baidu_sports_county as b on a.baidu_city=b.city;"
        )
        for d in data:
            city_id = d.city_id
            city_name = d.city_name
            # amap_city = d.amap_city
            name = d.name
            url = 'https://www.dianping.com/search/keyword/{}/0_{}'.format(city_id,name)
            print url
            yield Request(url, callback=self.parse,
                          meta={'city_id': city_id, 'city_name': city_name,'page':1,'name':name},
                          dont_filter=True, headers=header, errback=self.parse_failure)
        # url = 'https://www.dianping.com/search/keyword/2/0_%E5%B0%8F%E5%90%8A%E6%A2%A8%E6%B1%A4%E6%96%B0%E5%A5%A5%E5%BA%97'
        # yield Request(url, callback=self.parse,
        #               meta={'city_id': 1, 'city_name': '北京', 'page': 1, 'name': '小吊梨汤'},
        #               dont_filter=True, headers=header)

    def parse(self, response):
        sel = Selector(response)
        meta = response.meta
        data = sel.xpath('//div[@id="shop-all-list"]/ul/li/div[@class="pic"]/a/@href')
        if data:
            search_name = sel.xpath('//div[@class="tit"]/a/h4/text()').extract_first()
            if search_name == meta['name']:
                url_link = data.extract_first()
                meta['retry_times'] = 0
                yield Request(url_link, callback=self.parse_detail, meta=meta, headers=header)

    def parse_detail(self, response):
        content = response.body
        # print content
        meta = response.meta
        name = meta['name']
        open_hours = re.findall('营业时间：</span>([\s\S]*?)</span>', content)
        # print open_hours
        if open_hours:
            open_hours = ''.join(open_hours)
            open_hours = open_hours.replace('\t', '').replace('\n', '').replace('\r', '').replace(' ', '')
            open_hours = re.sub('<.*?>','',open_hours)
            # sql = 'update t_xsd_gaode_sports set opening_hours="%s" where name="%s"' % (open_hours,name)
            sql = 'update t_xsd_baidu_sports_county set opening_hours="%s" where name="%s"' % (open_hours,name)
            db_update.query(sql)

    def parse_failure1(self, failure):
        meta = failure.request.meta
        # meta['retry_times'] = 0
        # failure_time = meta['failure_time']
        # if failure_time < 5:
        #     meta['failure_time'] += 1
        #     error_resion = failure.value
        #     redis_.hset('dianping:error_resion', str(error_resion)[:38], '1')
        #     if 'Connection refused' in str(error_resion) or 'timeout' in str(
        #             error_resion) or 'Could not open CONNECT tunnel with proxy' in str(
        #         error_resion) or 'TCP connection timed out' in str(error_resion) or 'twisted' in str(error_resion):
        #         # print type(error_resion)
        #         url = failure.request.url
        #         if 'search' in url:
        #             yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True, meta=meta,
        #                           headers=header)
        #     else:
        #         try:
        #             error_resion = failure.value.response._body
        #             if 'aboutBox errorMessage' in error_resion or '没有找到相应的商户' in error_resion:
        #                 print error_resion
        #                 print error_resion
        #                 print error_resion
        #                 print error_resion
        #             else:
        #                 url = failure.request.url
        #                 if 'search' in url:
        #                     yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True,
        #                                   meta=meta,
        #                                   headers=header)
        #         except Exception, e:
        #             print e

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
