# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DianpingdealItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class PetServicesItem(scrapy.Item):
    dt = scrapy.Field()
    deal_id = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    new_price = scrapy.Field()
    old_price = scrapy.Field()
    sales = scrapy.Field()
    start_time = scrapy.Field()
    end_time = scrapy.Field()
    description = scrapy.Field()
    city_id = scrapy.Field()
    city_name = scrapy.Field()
    shop_id = scrapy.Field()