# -*- coding: utf-8 -*-
import scrapy
import re
from dianping.items import DianpingProvince

headers = {
    "User-Agent": "Mozilla/5.0 (compatible; Baiduspider-render/2.0;+http://www.baidu.com/search/spider.html)",
    "referer": 'www.baidu.com'
}


class Dianping_City_Province(scrapy.Spider):

    name = 'dianping_city_spider'

    def start_requests(self):
        url = 'http://www.dianping.com/citylist/citylist?citypage=1'
        yield scrapy.Request(url, headers=headers, callback=self.cityparse, dont_filter=True)

    def cityparse(self, response):
        content = response.body
        pattern = re.compile('<dl class="terms">([\s\S]*?)">更多')
        province_list = re.findall(pattern, content)
        for pro in province_list:
            # items = DianpingProvince()
            pattern1 = re.search('<dt>(.*?)<', pro)
            province = pattern1.group(1)
            pattern2 = re.compile('<strong>(.*?)<')
            city_list = re.findall(pattern2, pro)
            for city in city_list:
                items = DianpingProvince()
                items['province'] = province
                items['city'] = city

                yield items