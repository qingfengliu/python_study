# -*- coding: utf-8 -*-
import json

import scrapy
import time

import redis
from scrapy.selector import Selector
from scrapy import Request
import sys
import web, re
from scrapy_redis.spiders import RedisSpider
from dianpingdeal.items import PetServicesItem

reload(sys)
sys.setdefaultencoding('utf-8')

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())
db_insert = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0'
}
redis_ = redis.Redis(host='10.15.1.11', port=6379)


class ChongwuSpiderSpider(RedisSpider):
    name = "pet_spider"
    allowed_domains = ["dianping.com"]
    redis_key = 'dianping:chongwu6'

    start_urls = ['http://t.dianping.com/citylist']

    # def spider_idle(self):
    #     self.schedule_next_requests()

    def make_requests_from_url(self, url):
        try:
            meta = json.loads(url)
            meta['error_times'] = 0
            detail_url = meta['detail_url']
            return Request(url=str(detail_url), callback=self.parse_detail, meta=meta,
                           headers=header, dont_filter=True, errback=self.parse_failure)
        except Exception, e:
            print e

    #
    # def parse(self, response):
    #     pass

    # def start_requests(self):
    #     url = 'http://t.dianping.com/deal/24132360'
    #     yield Request(url,headers=header, callback=self.parse_detail, meta={'city_id': 2, 'city_name': '北京'})

    def parse_detail(self, response):
        item = PetServicesItem()
        # sel = Selector(response)
        content = response.body
        deal_id = ''.join(re.findall('dealGroupId:(\d+),', content))
        # category = ''.join(re.findall("category:'(.*?)'", content))
        title = ''.join(re.findall("shortTitle:'(.*?)'", content))
        new_price = re.findall('"price":(.*?),', content)
        if new_price:
            new_price = new_price[0]
        else:
            new_price = ''
        old_price = re.findall('"marketPrice":(.*?),', content)
        if old_price:
            old_price = old_price[0]
        sales = re.findall('J_current_join">(\d+)<', content)
        if sales:
            sales = sales[0]
        start_time = ''.join(re.findall("beginDate:'(.*?)'", content))
        end_time = ''.join(re.findall("endDate:'(.*?)'", content))
        description = ''.join(
            re.findall('summary summary-comments-big J_summary Fix[\s\S]*?<div class="bd">([\s\S]*?)</h2>', content))
        description = re.sub('<.*?>', '', description)
        description = description.replace('\n', '').replace('\r', '').replace(' ', '')
        city_id = response.meta['city_id']
        city_name = response.meta['city_name']
        shop_id = response.meta['shop_id']
        if not new_price:
            new_price = 0

        if not old_price:
            old_price = 0

        if not sales:
            sales = 0
        item['dt'] = dt
        item['deal_id'] = deal_id
        item['category'] = '宠物服务'
        item['title'] = title
        item['new_price'] = new_price
        item['old_price'] = old_price
        item['sales'] = sales
        item['start_time'] = start_time
        item['end_time'] = end_time
        item['description'] = description
        item['city_id'] = city_id
        item['city_name'] = city_name
        item['shop_id'] = shop_id
        # yield item
        self.web_db_insert(item)

    def parse_failure1(self, failure):
        meta = failure.request.meta
        meta['retry_times'] = 0
        error_resion = failure.value
        if 'Connection refused' in str(error_resion) or 'timeout' in str(
                error_resion) or 'Could not open CONNECT tunnel with proxy' in str(error_resion):
            url = failure.request.url
            if 'deal' in url:
                yield Request(url, callback=self.parse_detail, errback=self.parse_failure, meta=meta,
                              headers=header)
        else:

            try:
                error_resion = failure.value.response._body
                if 'aboutBox errorMessage' in error_resion:
                    pass
                else:
                    url = failure.request.url
                    if 'deal' in url:
                        yield Request(url, callback=self.parse_detail, errback=self.parse_failure,
                                      meta=meta, headers=header)

            except Exception, e:
                print e

    def parse_failure(self, failure):
        meta = failure.request.meta
        meta['retry_times'] = 0
        error_times = meta['error_times']
        if error_times < 30:
            meta['error_times'] = error_times + 1
            error_resion = failure.value
            redis_.hset('dianping:error_resion', str(error_resion)[:38], '1')
            # if 'Connection refused' in str(error_resion) or 'timeout' in str(
            #         error_resion) or 'Could not open CONNECT tunnel with proxy' in str(error_resion):
            url = failure.request.url
            if 'deal' in url:
                yield Request(url, callback=self.parse_detail, errback=self.parse_failure, meta=meta,
                              headers=header, dont_filter=True)

    def web_db_insert(self, data):
        db_insert.insert('t_hh_dianping_tuangou_deal_info_tmp', **data)
