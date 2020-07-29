# -*- coding: utf-8 -*-
import json
import random

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

'''curl -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' --compressed 'http://www.dianping.com/search/map/ajax/json?cityId=2&categoryId=25148&regionId=14'''

header = {
    'Host': 'www.dianping.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Cache-Control': 'no-cache'
}

header1 = {
    'Host': 'www.dianping.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br',
    # 'Connection': 'keep-alive',
    # 'Upgrade-Insecure-Requests': '1',
    # 'Pragma': 'no-cache',
    # 'Cache-Control': 'no-cache',
}

header2 = {'Host': 'www.dianping.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'}

ua_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"
]
# redis_ = redis.Redis(host='127.0.0.1', port=6379)
# redis_ = redis.Redis(host='10.15.1.11', port=6379)

'''curl 'http://www.dianping.com/search/map/ajax/json' -H 'Host: www.dianping.com' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0' -H 'Accept: application/json, text/javascript' -H 'Accept-Language: zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' --compressed -H 'Referer: http://www.dianping.com/search/map/category/1/0' -H 'X-Requested-With: XMLHttpRequest' -H 'X-Request: JSON' -H 'Content-Type: application/x-www-form-urlencoded;charset=UTF-8;' -H 'Cookie: cy=2; _lxsdk_cuid=15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8; _lxsdk=15fb993041fc8-09b50eeadd16918-49576f-13c680-15fb9930420c8; _hc.v=957103d7-ead0-5cba-a1c8-c987a61dd6cf.1510646941; s_ViewType=10; aburl=1; cye=beijing; __mta=141996557.1512988506320.1513250934951.1513570493729.4; Hm_lvt_dbeeb675516927da776beeb1d9802bd4=1513570353; cityid=2; default_ab=index%3AA%3A1%7Cshopreviewlist%3AA%3A1%7Csinglereview%3AA%3A1; _lxsdk_s=160bacebf7f-39d-4e2-036%7C%7C55; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic' -H 'Connection: keep-alive' -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' --data 'cityId=1&cityEnName=shanghai&promoId=0&shopType=95&categoryId=95&regionId=1&sortMode=2&shopSortItem=0&keyword=&searchType=1&branchGroupId=0&aroundShopId=0&shippingTypeFilterValue=0&page=1'''

cookie = {
    'cy':'2',
    'm_flash2':'1',
    'cityid':'2',
    'testName':'test2',
    'seflAB':'a',
    's_ViewType':'10'
}

class PetSpider(scrapy.Spider):
    name = "pet_hospital99999"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']
    def __init__(self, category_id='20', little_category_id='33759', category_name='health', *args, **kwargs):
        super(PetSpider, self).__init__(*args, **kwargs)
        self.category_id = category_id
        self.little_category_id = little_category_id
        self.category_name = category_name

    def start_requests(self):
        data = db.query(
            "select distinct city_id,city_name,district_id,district_name from "
            "t_hh_dianping_business_area where city_id=2;")
        data_category = db.query(
            "select distinct category1_id,category1_name,category2_id,category2_name "
            "from t_hh_dianping_category where category2_name in ('宠物医院','宠物店');")
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
                # url = 'http://www.dianping.com/search/map/ajax/json?cityId={}&categoryId={}&regionId={}'.format(
                #     city_id,
                #     category2_id, district_id)
                # url = 'http://www.dianping.com/search/map/ajax/json?cityId=%s' \
                #       '&promoId=0&shopType=%s&categoryId=%s&regionId=%s' \
                #       '&sortMode=2&shopSortItem=0&searchType=1&branchGroupId=0' \
                #       '&aroundShopId=0&shippingTypeFilterValue=0' % (city_id,category1_id,category1_id,district_id)
                url = 'http://www.dianping.com/search/map/ajax/json?cityId=%s' \
                      '&promoId=0&shopType=%s&categoryId=%s&regionId=%s&sortMode=2' \
                      '&shopSortItem=0&keyword=&searchType=1&branchGroupId=0&' \
                      'aroundShopId=0&shippingTypeFilterValue=0&page=1' % \
                      (city_id, category1_id, category1_id, district_id)
                print url
                header2['User-Agent'] = random.choice(ua_list)
                header2[
                    'Referer'] = 'https://www.baidu.com/link?url=6bHIcRVwxE5p1jNSamTaLAXEeV5RL1LR-0kCH1VsKEud6XJRkBbzHOG7aJWF0Hu_&wd=&eqid=dcaaa61a0002ad50000000025a4c59c0'

                yield Request(url, callback=self.parse,
                              meta={'city_id': city_id, 'city_name': city_name, 'district_id': district_id,
                                    'district_name': district_name, 'page': 1, 'failure_time': 0,
                                    'category1_id': category1_id, 'category1_name': category1_name,
                                    'category2_id': category2_id, 'category2_name': category2_name},
                              headers=header2,cookies=cookie)
                # url = 'http://www.dianping.com/search/map/ajax/json?cityId={}&categoryId={}&regionId={}'.format(
                #             2,
                #     25148, 14)
                # header['User-Agent'] = random.choice(ua_list)
                # yield Request(url, callback=self.parse,
                #                       meta={'city_id': 2, 'city_name': '北京', 'district_id': 14,
                #                             'district_name': '朝阳区', 'page': 1, 'failure_time': 0,
                #                             'category1_id': 95, 'category1_name': '医疗健康',
                #                             'category2_id': 25148, 'category2_name': '宠物医院'},
                #                       dont_filter=True, headers=header)

    def parse(self, response):
        if response.status == 403:
            print response.request.headers
            self.write_file(response.request.headers.get('User-Agent'))
        else:
            page = response.meta['page']
            response_json = json.loads(response.body)
            meta = response.meta
            meta['retry_times'] = 0
            if response_json:
                shopRecordBeanList = response_json.get('shopRecordBeanList')
                if shopRecordBeanList:
                    for shopRecordBean1 in shopRecordBeanList:
                        # item = DianPIngAllStoreJson()
                        # item['response_content'] = shopRecordBean1
                        # item['meta'] = meta
                        # meta['item'] = item
                        shop_id = shopRecordBean1.get('shopId')
                        t = time.time()
                        stamp = (int(round(t * 1000)))
                        # shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?_nr_force='+str(stamp)+'&shopId='+str(shop_id)+'&_token=eJx1T8FqwzAM%2FRedTSzVdtMEesjoDlmbHUYyGKOHLA1ONpoE2zQdY%2F8%2BlXaHHQaCJ7330JO%2BwOUHSAkRNQk4tQ5SoAgjAwKCZ8WQTpZamxUlsYDmL6eNEfDmnjeQvpLWKJSi%2FYV5YuLKJIh7cW1XxOJCc108OVugC2FKpZznOTr09TD1g42a8Sh9N04yXihDGCOf8q%2BvrV3TyaYOrR3dpyRJKC0RAkccS45g%2FLhhfcPwOxf8LC%2F3vR24ax%2FO5dbH%2BU7eFb6sVPH%2BQo%2BbTFVZnu2G6t7a9Rq%2BfwA%2FhVRh&uuid=%22766a30e5-13e0-458d-8323-a7951f5e77f5.1490013351%22&platform=1&partner=150&originUrl=http%3A%2F%2Fwww.dianping.com%2Fshop%2F'+str(shop_id)
                        # test_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=13720593'
                        shop_url = 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?_nr_force=' + str(
                            stamp) + '&shopId=' + str(shop_id)
                        meta['shopRecordBean'] = shopRecordBean1
                        header1['User-Agent'] = random.choice(ua_list)
                        header1[
                            'Referer'] = 'https://www.baidu.com/link?url=6bHIcRVwxE5p1jNSamTaLAXEeV5RL1LR-0kCH1VsKEud6XJRkBbzHOG7aJWF0Hu_&wd=&eqid=dcaaa61a0002ad50000000025a4c59c0'
                        # yield Request(shop_url, callback=self.parse_shop, meta=meta, dont_filter=True, headers=header1,
                        #               priority=1)

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
                            next_page_link = next_page_link.replace('https', 'http')
                            header1['User-Agent'] = random.choice(ua_list)
                            header1['Referer'] = 'https://www.baidu.com'
                            yield Request(next_page_link, callback=self.parse, meta=meta, dont_filter=True,
                                          headers=header1)

    def parse_shop(self, response):
        if response.status == 403:
            print response.request.headers
            self.write_file(response.request.headers.get('User-Agent'))
        else:
            item = DianPIngAllStoreJson()
            item['meta'] = response.meta
            item['shop_response'] = response.body
            yield item

    def write_file(self, data):
        # with open('403_headers.csv','a') as f:
        #     f.write(str(data)+'\n')
        pass
