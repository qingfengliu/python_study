# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class DianpingcommentItem(scrapy.Item):
    comment_id = scrapy.Field()
    shop_id = scrapy.Field()
    name = scrapy.Field()
    user_id = scrapy.Field()
    total_score = scrapy.Field()
    score1_name = scrapy.Field()
    score1 = scrapy.Field()
    score2_name = scrapy.Field()
    score2 = scrapy.Field()
    score3_name = scrapy.Field()
    score3 = scrapy.Field()
    comment_text = scrapy.Field()
    comment_dt = scrapy.Field()
    user_contrib_val = scrapy.Field()
    user_name = scrapy.Field()
    search_kw_cate = scrapy.Field()


class DianpingcommentMeishiItem(scrapy.Item):
    comment_id = scrapy.Field()
    shop_id = scrapy.Field()
    name = scrapy.Field()
    user_id = scrapy.Field()
    total_score = scrapy.Field()
    score1_name = scrapy.Field()
    score1 = scrapy.Field()
    score2_name = scrapy.Field()
    score2 = scrapy.Field()
    score3_name = scrapy.Field()
    score3 = scrapy.Field()
    comment_text = scrapy.Field()
    comment_dt = scrapy.Field()
    user_contrib_val = scrapy.Field()
    user_name = scrapy.Field()
    dt = scrapy.Field()