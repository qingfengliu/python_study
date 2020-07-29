# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DianpingItem(scrapy.Item):
    # define the fields for your item here like:
    city_name = scrapy.Field()
    district_name = scrapy.Field()
    shop_id = scrapy.Field()
    brand_name = scrapy.Field()
    shop_name = scrapy.Field()
    shop_address = scrapy.Field()

class DianpingProvince(scrapy.Item):
    province = scrapy.Field()
    city = scrapy.Field()