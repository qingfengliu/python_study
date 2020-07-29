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

redis_ = redis.Redis(host='10.15.1.11', port=6379)

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


# 手机端子分类采集'https://m.dianping.com/shoplist/2/r/14/c/25148/s/s_-1'
# 手机端总分类采集'https://m.dianping.com/shoplist/2/r/14/c/95/s/s_-1'
# 手机端总分类page2 'https://mapi.dianping.com/searchshop.json?start=20&categoryid=25148&parentCategoryId=25148&locatecityid=2&limit=20&sortid=0&cityid=2&regionid=14&maptype=0&callback='


class PetSpider(scrapy.Spider):
    name = "pet_hospital_phone_job_new"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, category_id='20', little_category_id='33759', category_name='health', city_scope='0,15',page_num=50, *args,
                 **kwargs):
        super(PetSpider, self).__init__(*args, **kwargs)
        self.category_id = category_id
        self.little_category_id = little_category_id
        self.category_name = category_name
        self.city_scope = city_scope
        self.proxys = ''
        self.dt_proxy = 0
        self.page_num = page_num

    def start_requests(self):
        data = db.query(
            "select distinct city_id,city_name from "
            "t_hh_dianping_business_area where city_id='417';")
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
            # district_id = d.district_id
            # district_name = d.district_name
            for dc in list_category:
                category1_id = dc.get('category1_id')
                category1_name = dc.get('category1_name')
                category2_id = dc.get('category2_id')
                category2_name = dc.get('category2_name')


                list_url = 'http://mapi.dianping.com/searchshop.json?categoryid=%s&limit=50&cityid=%s&callback=&start=0' % (category1_id,city_id)
                print list_url
                header1['User-Agent'] = random.choice(ua_list)
                yield Request(url=list_url, headers=header2,
                              callback=self.parse,
                              meta={'city_id': city_id, 'city_name': city_name, 'page': 0, 'failure_time': 0,
                                    'category1_id': category1_id, 'category1_name': category1_name}, dont_filter=True)

        # url = 'https://mapi.dianping.com/searchshop.json?categoryid=25148&limit=50&cityid=2&regionid=14&callback=&start=0'
        # yield Request(url=url, headers=header2, callback=self.parse,
        #               meta={'city_id': 2, 'city_name': '', 'district_id': 14, 'district_name': '', 'page': 0,
        #                     'failure_time': 0, 'category1_id': 95, 'category1_name': ''}, dont_filter=True)

    def parse(self, response):
        response_json = json.loads(response.body)
        print response_json
        # data = response_json.get('list')
        meta = response.meta

        list_ = response_json.get('list')
        if list_:
            for ll in list_:
                shopType = ll.get('shopType')
                if shopType:
                    if str(shopType) == str(95):
                        shop_id = ll.get('id')
                        print shop_id
                        if not redis_.hexists('dianping:pet_hospital_hash', shop_id):
                            redis_.hset('dianping:pet_hospital_hash', shop_id, 1)
                            shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=' + str(
                                shop_id)
                            meta['shop_info'] = ll
                            meta['dt'] = dt
                            meta['shop_url'] = shop_url
                            url_meta = json.dumps(meta)
                            redis_.lpush('dianping:pet_hospital', url_meta)
            if str(meta.get('page'))=='0':
                recordCount = response_json.get('recordCount')
                # print recordCount
                page_total,page_mod = divmod(int(recordCount),int(self.page_num))
                if page_mod>0:
                    page_total+=1
                url_list = response.url.split('&start=')

                for i in xrange(1,page_total):
                    start = i*int(self.page_num)
                    url = url_list[0]+'&start='+str(start)
                    meta['page'] = i
                    yield Request(url, headers=header2,
                                  callback=self.parse, meta=meta, dont_filter=True)
