# -*- coding: utf-8 -*-
import json
import random

import requests
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
# redis_ = redis.Redis(host='127.0.0.1', port=6379)
# redis_ = redis.Redis(host='10.15.1.11', port=6379)


post_data_hhh = {"pageEnName": "shopList", "moduleInfoList": [{"moduleName": "mapiSearch", "query": {
    "search": {"start": 1, "categoryid": "95", "limit": 200, "cityid": 2}, "loaders": "list"}}]}
post_url_hhh = 'https://m.dianping.com/isoapi/module'

header_phone = {
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    # 'Cookie': 'cy=2; cityid=2',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0',
    'Referer': 'http://www.dianping.com/shop/74597797',
}

header_phone_chrome = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'http://www.dianping.com/shop/74597797',
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
'''

Accept-Encoding: gzip, deflate
Accept-Language: zh-CN
Cache-Control: no-cache
Connection: Keep-Alive
Content-Length: 592
Content-Type: text/plain
Host: report.meituan.com
Origin: http://www.dianping.com
Referer: http://www.dianping.com/shop/56637267
UA-CPU: AMD64
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; Trident/7.0; rv:11.0) like Gecko



'''

ddd = {'Connection': 'keep-alive',
       'Cookie': 'cy=2; cityid=2',
       'Accept-Encoding': 'gzip, deflate',
       'Accept': '*/*',
       'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0'}

cookie_phone = {
    '_lxsdk_cuid': '15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8',
    '_lx_utm': 'utm_source%3DBaidu%26utm_medium%3Dorganic',
    '__mta': '141996557.1512988506320.1513250934951.1513570493729.4',
    's_ViewType': '10',
    'Hm_lvt_dbeeb675516927da776beeb1d9802bd4': '1513570353',
    'default_ab': 'index%3AA%3A1',
    'cityid': '2',
    'cy': '2',
    'cye': 'beijing',
    'm_flash2': '1',
    'aburl': '1',
    '_lxsdk': '15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8',
    '_hc.v': '957103d7-ead0-5cba-a1c8-c987a61dd6cf.1510646941'
}

cookie_phone_chrome = {
    # '_lxsdk_cuid': '15e9d172822c8-05dcbc4b4e4312-143c6c55-13c680-15e9d172822c8',

    # '_lxsdk_s': '1608d78091f-db7-5ba-5ce%7C%7C29',
    # 's_ViewType': '10',
    'cy': '2',
    'cye': 'beijing',
    'cityid': '2',
    # 'aburl': '1',
    # '_lxsdk': '15e9d172822c8-05dcbc4b4e4312-143c6c55-13c680-15e9d172822c8',
    # '_hc.v': '""4a82d47c-c063-4fa7-ac59-32b2b10c3195.1489996100""'
}


class PetSpider(scrapy.Spider):
    name = "pet_hospital_error"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, category_id='20', little_category_id='33759', category_name='health', city_scope='0,15', *args,
                 **kwargs):
        super(PetSpider, self).__init__(*args, **kwargs)
        self.category_id = category_id
        self.little_category_id = little_category_id
        self.category_name = category_name
        self.city_scope = city_scope
        self.proxys = ''
        self.dt_proxy = 0

    def start_requests(self):
        with open('error_url') as f:
            data = f.readlines()

        for shop_url in data:
            print shop_url
            header11 = random.choice([header_phone_chrome_windows, header_phone_safari,header_phone_opera,header_phone_firefox_ubuntu,header_phone_chrome_ubuntu])
            header11['User-Agent'] = random.choice(ua_list)
            header11['Referer'] = 'http://www.dianping.com/shop/%s' % shop_url.replace(
                'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=', '').replace('\n', '')
            shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId='+shop_url
            yield Request(url=shop_url.replace('\n', ''), callback=self.parse_shop, headers=header11,
                          cookies=cookie_phone_chrome, meta={}, dont_filter=True, priority=1)

    def parse_shop(self, response):
        # if response.status == 403:
        #     with open('error_url', 'a') as f:
        #         f.write(response.url + '\n')
        # print response.headers
        item = DianPIngAllStoreJson()
        item['meta'] = response.meta
        item['shop_response'] = response.body
        yield item
