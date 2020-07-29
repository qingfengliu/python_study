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

header1 = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'www.baidu.com'
}

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


header2 = {
    'User-Agent': 'Mozilla/5.0 (Linux; U; Android 6.0;zh_cn; Letv X500 Build/DBXCNOP5902605181S) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/49.0.2623.91 Mobile Safari/537.36 EUI Browser/1.6.1.71',
    'content-type': 'application/json',
    'Referer': 'www.baidu.com'
}

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

ddd = {'Connection': 'keep-alive',
       'Cookie': 'cy=2; cityid=2',
       'Accept-Encoding': 'gzip, deflate',
       'Accept': '*/*',
       'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0'}

cookie_phone = {
    # '_lxsdk_cuid': '15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8',
    # '_lx_utm': 'utm_source%3DBaidu%26utm_medium%3Dorganic',
    # '__mta': '141996557.1512988506320.1513250934951.1513570493729.4',
    # 's_ViewType': '10',
    # 'Hm_lvt_dbeeb675516927da776beeb1d9802bd4': '1513570353',
    # 'default_ab': 'index%3AA%3A1',
    'cityid': '2',
    'cy': '2',
    # 'cye': 'beijing',
    # 'm_flash2': '1',
    # 'aburl': '1',
    # '_lxsdk': '15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8',
    # '_hc.v': '957103d7-ead0-5cba-a1c8-c987a61dd6cf.1510646941'}
}


class PetSpider(scrapy.Spider):
    name = "pet_hospital_phone_1"
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
        data = db.query(
            "select distinct city_id,city_name,district_id,district_name from "
            "t_hh_dianping_business_area where city_id=2;")
        data_category = db.query(
            "select distinct category1_id,category1_name,category2_id,category2_name from t_hh_dianping_category where category1_id=95 and category2_id!=0;")
        list_category = []
        for dc in data_category:
            category_item = {}
            category_item['category1_id'] = dc.category1_id
            category_item['category1_name'] = dc.category1_name
            category_item['category2_id'] = dc.category2_id
            category_item['category2_name'] = dc.category2_name
            list_category.append(category_item)
        for d in data:
            city_id = d.city_id
            city_name = d.city_name
            district_id = d.district_id
            district_name = d.district_name
            for dc in list_category:
                category1_id = dc.get('category1_id')
                category1_name = dc.get('category1_name')
                category2_id = dc.get('category2_id')
                category2_name = dc.get('category2_name')
                post_url = 'https://m.dianping.com/isoapi/module'
                print post_url
                post_data = {"pageEnName": "shopList", "moduleInfoList": [{"moduleName": "mapiSearch", "query": {
                    "search": {"start": 1, "categoryid": category1_id, "limit": 200, "cityid": city_id,
                               "regionid": district_id}, "loaders": "list"}}]}
                post_data = json.dumps(post_data)
                header1['User-Agent'] = random.choice(ua_list)
                yield Request(url=post_url, method='POST', headers=header2,
                              body=post_data, callback=self.parse,
                              meta={'city_id': city_id, 'city_name': city_name, 'district_id': district_id,
                                    'district_name': district_name, 'page': 1, 'failure_time': 0,
                                    'category1_id': category1_id, 'category1_name': category1_name,
                                    'post_data': post_data}, dont_filter=True)

                # post_url = 'https://m.dianping.com/isoapi/module'
                # print post_url
                #
                # post_data = {"pageEnName": "shopList", "moduleInfoList": [{"moduleName": "mapiSearch", "query": {
                #     "search": {"start": 1, "categoryid": "95", "limit": 200, "cityid": 1452, "regionid": 4744,}, "loaders": "list"}}]}
                # post_data = json.dumps(post_data)
                # yield Request(url=post_url, method='POST', headers=header2,
                #               body=post_data, callback=self.parse, meta={'city_id': 1452, 'city_name': '北京', 'district_id': 4744,
                #                                                          'district_name': '朝阳', 'page': 1, 'failure_time': 0,
                #                                                          'category1_id': 95, 'category1_name': '宠物',
                #                                                          'post_data': post_data}, dont_filter=True)

    def parse(self, response):
        response_json = json.loads(response.body)
        data = response_json.get('data')
        meta = response.meta
        if data:
            moduleInfoList = data.get('moduleInfoList')
            if moduleInfoList:
                moduleInfoList = moduleInfoList[0]
                moduleData = moduleInfoList.get('moduleData')
                if moduleData:
                    data = moduleData.get('data')
                    if data:
                        listData = data.get('listData')
                        if listData:
                            list = listData.get('list')
                            if list:

                                for ll in list:
                                    shop_id = ll.get('id')
                                    print shop_id
                                    shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=' + str(
                                        shop_id)
                                    meta['shop_info'] = ll
                                    meta['dt'] = dt

                                    cookie_phone['cy'] = meta.get('city_id')
                                    cookie_phone['cityid'] = meta.get('city_id')
                                    header11 = random.choice(
                                        [header_phone_chrome_windows, header_phone_safari, header_phone_opera,
                                         header_phone_firefox_ubuntu, header_phone_chrome_ubuntu])
                                    header11['User-Agent'] = random.choice(ua_list)
                                    header11['Referer'] = 'http://www.dianping.com/shop/%s' % shop_url.replace(
                                        'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=',
                                        '').replace('\n', '')
                                    yield Request(url=shop_url, callback=self.parse_shop, headers=header11,
                                                  cookies=cookie_phone, meta=meta, dont_filter=True, priority=1)

                                recordCount = listData.get('recordCount')
                                print recordCount
                                nextStartIndex = listData.get('nextStartIndex')
                                print nextStartIndex
                                if int(recordCount) > int(nextStartIndex):
                                    startIndex = nextStartIndex
                                    post_data = meta['post_data']
                                    post_data = json.loads(post_data)
                                    post_data['moduleInfoList'][0]['query']['search']['start'] = startIndex
                                    print post_data
                                    # print response.url
                                    meta = response.meta
                                    meta['retry_times'] = 0
                                    post_data = json.dumps(post_data)
                                    meta['post_data'] = post_data

                                    yield Request(response.url, method='POST', headers=header2, body=post_data,
                                                  callback=self.parse, meta=meta, dont_filter=True)

        print response.meta

    def parse_shop(self, response):
        if response.status == 403:
            with open('error_url', 'a') as f:
                f.write(response.url + '\n')
        print response.headers
        item = DianPIngAllStoreJson()
        item['meta'] = response.meta
        item['shop_response'] = response.body
        yield item
