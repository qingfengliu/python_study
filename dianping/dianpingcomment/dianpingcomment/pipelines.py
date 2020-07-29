# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import web
db_insert = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


class DianpingcommentPipeline(object):
    def process_item(self, item, spider):
        db_insert.insert('t_hh_dianping_shop_comments_food',**item)
        return item
