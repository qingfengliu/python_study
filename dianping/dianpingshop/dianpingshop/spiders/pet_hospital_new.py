# -*- coding: utf-8 -*-

import json
import random
import scrapy
import time
from scrapy.http import Request
import sys
import web
from dianpingshop.items import DianPIngAllStoreJson
reload(sys)
sys.setdefaultencoding('utf-8')

db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())

ua_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"
]

'''curl 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?_nr_force=1514964458458&shopId=72351070&_token=eJx1T8FqwzAM%2FRedTSzVdtMEesjoDlmbHUYyGKOHLA1ONpoE2zQdY%2F8%2BlXaHHQaCJ7330JO%2BwOUHSAkRNQk4tQ5SoAgjAwKCZ8WQTpZamxUlsYDmL6eNEfDmnjeQvpLWKJSi%2FYV5YuLKJIh7cW1XxOJCc108OVugC2FKpZznOTr09TD1g42a8Sh9N04yXihDGCOf8q%2BvrV3TyaYOrR3dpyRJKC0RAkccS45g%2FLhhfcPwOxf8LC%2F3vR24ax%2FO5dbH%2BU7eFb6sVPH%2BQo%2BbTFVZnu2G6t7a9Rq%2BfwA%2FhVRh&uuid=%22766a30e5-13e0-458d-8323-a7951f5e77f5.1490013351%22&platform=1&partner=150&originUrl=http%3A%2F%2Fwww.dianping.com%2Fshop%2F72351070' \
-XGET \
-H 'Referer: http://www.dianping.com/shop/72351070' \
-H 'Host: www.dianping.com' \
-H 'Accept: application/json, text/javascript' \
-H 'Connection: keep-alive' \
-H 'Accept-Language: zh-cn' \
-H 'Accept-Encoding: gzip, deflate' \
-H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5' \
-H 'Cookie: _lxsdk_s=160baead820-514-8fc-e5a%7C%7C28; s_ViewType=10; cy=1; cye=shanghai; aburl=1; _lxsdk=15ea24522e9c8-0f750dd75d62ee-3e636f4f-13c680-15ea24522e9c8; _lxsdk_cuid=15ea24522e9c8-0f750dd75d62ee-3e636f4f-13c680-15ea24522e9c8; _hc.v="\"766a30e5-13e0-458d-8323-a7951f5e77f5.1490013351\""' \
-H 'X-Request: JSON' \
-H 'X-Requested-With: XMLHttpRequest'''


class PetSpider(scrapy.Spider):
    name = "pet_hospital999991111"
    allowed_domains = ["dianping.com"]

    def __init__(self, *args, **kwargs):
        super(PetSpider, self).__init__(*args, **kwargs)


    def start_requests(self):
        data = db.query(
            "select distinct city_id,city_name,district_id,district_name from "
            "t_hh_dianping_business_area;")
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
                url = 'http://www.dianping.com/search/map/ajax/json?cityId={}&categoryId={}&regionId={}'.format(
                    city_id,
                    category2_id, district_id)
                print url
                header1['User-Agent'] = random.choice(ua_list)
                header1['Referer'] = 'https://www.baidu.com/link?url=6bHIcRVwxE5p1jNSamTaLAXEeV5RL1LR-0kCH1VsKEud6XJRkBbzHOG7aJWF0Hu_&wd=&eqid=dcaaa61a0002ad50000000025a4c59c0'

                yield Request(url, callback=self.parse,
                              meta={'city_id': city_id, 'city_name': city_name, 'district_id': district_id,
                                    'district_name': district_name, 'page': 1, 'failure_time': 0,
                                    'category1_id': category1_id, 'category1_name': category1_name,
                                    'category2_id': category2_id, 'category2_name': category2_name},
                               headers=header1)
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
        else:
            page = response.meta['page']
            response_json = json.loads(response.body)
            meta = response.meta
            meta['retry_times'] = 0
            if response_json:
                shopRecordBeanList = response_json.get('shopRecordBeanList')
                if shopRecordBeanList:
                    for shopRecordBean1 in shopRecordBeanList:
                        shop_id = shopRecordBean1.get('shopId')
                        shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId='+str(shop_id)
                        meta['shopRecordBean'] = shopRecordBean1
                        header1['User-Agent'] = random.choice(ua_list)
                        header1['Referer'] = 'https://www.baidu.com/link?url=6bHIcRVwxE5p1jNSamTaLAXEeV5RL1LR-0kCH1VsKEud6XJRkBbzHOG7aJWF0Hu_&wd=&eqid=dcaaa61a0002ad50000000025a4c59c0'
                        yield Request(shop_url,callback=self.parse_shop,meta=meta, dont_filter=True, headers=header1,priority=1)

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
                            next_page_link = next_page_link.replace('https','http')
                            header1['User-Agent'] = random.choice(ua_list)
                            header1['Referer'] = 'https://www.baidu.com'
                            yield Request(next_page_link, callback=self.parse, meta=meta, dont_filter=True, headers=header1)

    def parse_shop(self,response):
        if response.status == 403:
            print response.request.headers

        else:
            item = DianPIngAllStoreJson()
            item['meta'] = response.meta
            item['shop_response'] = response.body
            yield item
