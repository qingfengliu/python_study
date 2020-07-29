# -*- coding: utf-8 -*-
import json
import random

import requests
import scrapy
import time
from scrapy.http import Request
import sys
import web, re
from scrapy_redis.spiders import RedisSpider
from dianpingshop.items import DianPIngAllStoreJson
import redis

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

redis_ = redis.Redis(host='10.15.1.11', port=6379)

header_phone = {
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    # 'Cookie': 'cy=2; cityid=2',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0'
}

header_phone_safari = {
    'Referer': 'http://www.dianping.com/shop/74597797',
    'Host': 'www.dianping.com',
    'Accept': 'application/json, text/javascript',
    'Connection': 'keep-alive',
    'Accept-Language': 'zh-cn',
    'Accept-Encoding': 'gzip, deflate',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5',
    'X-Request': 'JSON',
    'X-Requested-With': 'XMLHttpRequest',
}

header_phone_chrome_windows = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

header_phone_opera = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36 OPR/49.0.2725.64',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

header_phone_firefox_ubuntu = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:51.0) Gecko/20100101 Firefox/51.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.5'
}

header_phone_chrome_ubuntu = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
}

header_phone_IE_windows = {

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

header_phone_chrome1 = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'http://www.dianping.com/shop/74597797',
}

cookie_phone = {

    'cityid': '2',
    'cy': '2',

}


class PetSpider(RedisSpider):
    name = "pet_hospital_phone_spider"
    allowed_domains = ["dianping.com"]
    redis_key = 'dianping:pet_hospital'


    # def spider_idle(self):
    #     self.schedule_next_requests()

    def make_requests_from_url(self, url):
        try:
            meta = json.loads(url)
            meta['retry_times'] = 0
            cookie_phone['cy'] = meta.get('city_id')
            cookie_phone['cityid'] = meta.get('city_id')
            shop_url = meta['shop_url']
            header11 = random.choice(
                [header_phone_chrome_windows, header_phone_safari, header_phone_opera,
                 header_phone_firefox_ubuntu, header_phone_chrome_ubuntu])
            # header11['User-Agent'] = random.choice(ua_list)
            header11['Referer'] = 'http://www.dianping.com/shop/%s' % shop_url.replace(
                'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=',
                '').replace('\n', '')
            return Request(url=shop_url, callback=self.parse_shop, headers=header11,
                          cookies=cookie_phone, meta=meta, dont_filter=True, priority=1)

            # return Request(url=str(detail_url), callback=self.parse_detail, meta=meta,
            #                headers=header, dont_filter=True, errback=self.parse_failure)
        except Exception, e:
            print e

    def parse_shop(self, response):
        # if response.status == 403:
        #     with open('error_url', 'a') as f:
        #         f.write(response.url + '\n')
        # print response.headers
        item = DianPIngAllStoreJson()
        item['meta'] = response.meta
        item['shop_response'] = response.body
        yield item
