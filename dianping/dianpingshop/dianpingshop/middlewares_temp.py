# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import json

import redis
from scrapy import signals


class DianpingshopSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


import random
import time
import web

# db = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=3306, host='xxxx')
dt = 0
class ProxyMiddleware(object):

    def __init__(self):
        self.proxys = ''
        self.dt = 0

    def process_request(self, request, spider):
        if int(time.time()) - self.dt > 5:
            self.dt = int(time.time())
            self.proxys = self.redis_conn1()
        if self.proxys:
            proxy = random.choice(self.proxys)
            # if not divmod(int(time.time()), 5)[1]:

            request.meta['proxy'] = "%s" % proxy
            request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'

    def __get_proxy(self):

        # data = db.query('''select ip from t_spider_proxy''')
        # if data:
        #     proxy_res = []
        #     for item in data:
        #         proxy_res.append('http://' + item['ip'])
        #     return proxy_res
        # return []
        return ['http://180.116.92.225:52384']

    def redis_conn1(self):
        r = redis.Redis(host='116.196.71.111', port=52385, db=0)
        data = r.smembers('proxy')
        if data:
            proxy_res = []
            for d in data:
                dd = json.loads(d)
                proxy_res.append('http://' + str(dd['ip']).replace(':52384', ':42384'))
            return proxy_res

        return []