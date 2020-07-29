# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import base64

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

from scrapy.conf import settings

class RandomUserAgentMiddleware(object):

    def process_request(self, request, spider):
        ua = random.choice(settings.get('USER_AGENT_LIST'))
        if ua:
            request.headers.setdefault('User-Agent', ua)


import random
import time
import redis
import json

dt = 0

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


class ProxyMiddleware(object):
    def __init__(self):
        self.proxys = ''
        self.dt = 0

    def process_request1(self, request, spider):
        if not self.proxys:
            self.proxys = self.redis_conn()
        if self.proxys:
            proxy = random.choice(self.proxys)
            # if not divmod(int(time.time()), 5)[1]:
            if int(time.time()) - self.dt > 5:
                self.dt = int(time.time())
                self.proxys = self.redis_conn()
            request.meta['proxy'] = "%s" % proxy
            proxy_user_pass = "avcspider:aowei123"
            encoded_user_pass = base64.b64encode(proxy_user_pass)
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass
            request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            print request.headers['User-Agent']

    def process_request(self, request, spider):
        if not self.proxys:
            self.proxys = self.redis_conn1()
        if self.proxys:
            proxy = random.choice(self.proxys)
            # if not divmod(int(time.time()), 5)[1]:
            if int(time.time()) - self.dt > 5:
                self.dt = int(time.time())
                self.proxys = self.redis_conn1()
            # proxy = 'http://127.0.0.1:8888'
            request.meta['proxy'] = "%s" % proxy
            request.headers['User-Agent'] = random.choice(ua_list)
            # request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            # print request.headers['User-Agent']

    def redis_conn1(self):
        r = redis.Redis(host='116.196.71.111', port=52385, db=0)
        data = r.smembers('proxy_data5u')
        if data:
            proxy_res = []
            for d in data:
                dd = json.loads(d)
                proxy_res.append('http://' + str(dd['ip']))
            return proxy_res
        return []

    def redis_conn(self):
        r = redis.Redis(host='117.23.4.139', port=15480, db=0)
        ll = ['proxy:iplist1', 'proxy:iplist']
        data = r.smembers(random.choice(ll))
        if data:
            proxy_res = []
            for d in data:
                dd = json.loads(d)
                proxy_res.append('http://' + str(dd['ip']))
            return proxy_res
        return []

    def redis_conn_tt(self):
        r = redis.Redis(host='116.196.71.111', port=52385, db=0)
        data = r.smembers('proxy_tt')
        if data:
            proxy_res = []
            for d in data:
                dd = json.loads(d)
                proxy_res.append('http://' + str(dd['ip']))
            return proxy_res
        return []


import requests
from scrapy.http import HtmlResponse
import time

header_phone = {'Connection': 'keep-alive', 'Cookie': 'cy=2; cityid=2', 'Accept-Encoding': 'gzip, deflate',
                'Accept': '*/*',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0'}


class requestsMiddleware(object):
    def __init__(self):
        self.proxys = ''
        self.dt = 0

    def process_request(self, request, spider):
        url = request.url
        if 'BasicHideInfoAjaxFP' in url:
            if not self.proxys:
                self.proxys = self.redis_conn1()
            if self.proxys:
                if int(time.time()) - self.dt > 5:
                    self.dt = int(time.time())
                    self.proxys = self.redis_conn1()
            proxies = {"http": "%s" % random.choice(self.proxys)}
            header_phone['Cookie'] = 'cy=%s; cityid=%s' % (request.meta.get('city_id'), request.meta.get('city_id'))
            data = requests.get(request.url, headers=header_phone, proxies=proxies, timeout=5)
            return HtmlResponse(url=data.url, body=data.content, encoding='utf-8', request=request, meta=request.meta,
                                status=200)
        else:
            if not self.proxys:
                self.proxys = self.redis_conn1()
            if self.proxys:
                proxy = random.choice(self.proxys)
                # if not divmod(int(time.time()), 5)[1]:
                if int(time.time()) - self.dt > 5:
                    self.dt = int(time.time())
                    self.proxys = self.redis_conn1()
                request.meta['proxy'] = "%s" % proxy
                request.headers['User-Agent'] = random.choice(ua_list)
                request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                print request.headers['User-Agent']

    def redis_conn1(self):
        r = redis.Redis(host='116.196.71.111', port=52385, db=0)
        data = r.smembers('proxy_xingyu')
        if data:
            proxy_res = []
            for d in data:
                dd = json.loads(d)
                proxy_res.append('http://' + str(dd['ip']))
            return proxy_res
        return []
