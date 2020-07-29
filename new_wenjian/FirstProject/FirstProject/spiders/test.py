# -*- coding: utf-8 -*-
import scrapy


class TestSpider(scrapy.Spider):
    name = 'test'
    allowed_domains = ['zb.yfb.qianlima']
    start_urls = ['http://zb.yfb.qianlima/']

    def parse(self, response):

        pass
