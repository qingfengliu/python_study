# -*- coding: utf-8 -*-
import json
import re

from dianpingdeal.items import PetServicesItem
import scrapy
import time
from scrapy.selector import Selector
from scrapy.http import Request
from urlparse import urljoin
import sys
import web
import redis

reload(sys)
sys.setdefaultencoding('utf-8')

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())
# db_insert = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0'
}

# redis_ = redis.Redis(host='127.0.0.1', port=6679)
redis_ = redis.Redis(host='10.15.1.11', port=6379)

class YaKeSpider(scrapy.Spider):
    name = "chike"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']


    def start_requests(self):
        data = db.query('select distinct city_id,city_name from t_hh_dianping_business_area order by city_id;')
        for d in data:
            city_id = d.city_id
            city_name = d.city_name
            print city_id, city_name
            url = 'http://www.dianping.com/search/category/%s/80/g182m3' % city_id
            yield Request(url, callback=self.parse,
                          meta={'city_id': city_id, 'city_name': city_name, 'page': 1, 'failure_time': 0},
                          dont_filter=True, headers=header, errback=self.parse_failure)
        # yield Request('https://www.dianping.com/search/category/883/95/m3',callback=self.parse,meta={'city_id': 883, 'city_name': '上虞区', 'page': 1, 'failure_time': 0},dont_filter=True, headers=header, errback=self.parse_failure)

    def parse(self, response):
        page = response.meta['page']

        sel = Selector(response)
        shop_list = sel.xpath('//div[@class="shop-list J_shop-list shop-all-list"]/ul/li')
        if shop_list:
            for shop_like in shop_list:
                shop_id = ''.join(shop_like.xpath('./div[@class="txt"]/div[@class="tit"]/a[1]/@href').extract())
                if shop_id:
                    shop_id = shop_id.replace('/shop/','')
                else:
                    shop_id=0
                deal_links = shop_like.xpath('./div[@class="svr-info"]/div/a[@target]/@href')
                meta = response.meta
                meta['retry_times'] = 0
                meta['shop_id'] = shop_id
                if deal_links:
                    for deal_link in deal_links:
                        detail_url = urljoin(response.url, ''.join(deal_link.extract()))
                        meta['detail_url'] = detail_url
                        # url_meta = json.dumps(meta)
                        # redis_.lpush('dianping:chongwu6', url_meta)
                        # __redis.lpush(detail_url)
                        yield Request(detail_url, callback=self.parse_detail, meta=meta, dont_filter=True,
                                      headers=header, errback=self.parse_failure)
                else:
                    deal_links = shop_like.xpath('./div[@class="svr-info"]/a[@target]/@href')
                    if deal_links:
                        for deal_link in deal_links:
                            detail_url = urljoin(response.url, ''.join(deal_link.extract()))
                            if 't.dianping.com/deal' in detail_url:
                                meta['detail_url'] = detail_url
                                yield Request(detail_url, callback=self.parse_detail, meta=meta, dont_filter=True,
                                              headers=header, errback=self.parse_failure)
        if page == 1:
            # 找到最有一页的页码，比对是否为当前页
            next_page = ''.join(sel.xpath('//a[@class="PageLink"][last()]/@title').extract())
            if next_page:
                # print next_page
                if int(next_page) == page:
                    pass
                else:
                    for i in xrange(2, int(next_page) + 1):
                        meta['page'] = i
                        next_page_link = response.url + 'p%s' % i
                        yield Request(next_page_link, callback=self.parse, meta=meta, dont_filter=True, headers=header,
                                      errback=self.parse_failure)

    def parse_failure1(self, failure):
        meta = failure.request.meta
        meta['retry_times'] = 0
        failure_time = meta['failure_time']
        if failure_time < 5:
            meta['failure_time'] += 1
            error_resion = failure.value
            redis_.hset('dianping:error_resion',str(error_resion)[:38],'1')
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
            meta['failure_time'] += 1
            try:
                error_resion = failure.value
                redis_.hset('dianping:error_resion',str(error_resion)[:38],'1')
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
                    elif 'deal' in url:
                        yield Request(url, callback=self.parse_detail, errback=self.parse_failure, dont_filter=True,
                                      meta=meta, headers=header)
            except Exception, e:
                url = failure.request.url
                if 'search' in url:
                    yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True,
                                  meta=meta,
                                  headers=header)
                elif 'deal' in url:
                    yield Request(url, callback=self.parse_detail, errback=self.parse_failure, dont_filter=True,
                                  meta=meta, headers=header)

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
        item['category'] = '齿科'
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
        yield item
        # self.web_db_insert(item)

    # def web_db_insert(self, data):
    #     db_insert.insert('t_hh_dianping_tuangou_deal_info_tmp1', **data)