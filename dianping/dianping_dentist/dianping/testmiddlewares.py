# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random
from scrapy.utils.project import get_project_settings
from scrapy import signals
import web

settings = get_project_settings()
# from redisUtil import RedisQueue

import redis
import json

db = web.database(dbn='mysql', db='xxxx', user='rd', pw='rd', port=3306, host='xxxx')


# redis1 = RedisQueue('proxy')
class ProxyMiddleware(object):
    # overwrite process request
    # def process_request(self, request, spider):
    #     # Set the location of the proxy
    #
    #     request.meta['proxy'] = "http://122.142.77.85:80"

    # Use the following lines if your proxy requires authentication
    # proxy_user_pass = "USERNAME:PASSWORD"
    # setup basic authentication for the proxy
    # encoded_user_pass = base64.encodestring(proxy_user_pass)
    # request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass
    def process_request(self, request, spider):
        domain = getattr(spider, 'allowed_domains', None)
        if domain:
            data = db.query('''select url from t_hh_proxy_list where domain ="{}"
                        and valid>0 order by update_time desc'''.format(domain))
            if data:
                proxy_res = []
                for item in data:
                    proxy_res.append(item['url'])
                if proxy_res:
                    proxy = random.choice(proxy_res)
                    request.meta['proxy'] = "%s" % proxy
                    # ua = random.choice(settings.get('USER_AGENT_LIST'))
                    # request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                    # if ua:
                    #     request.headers['User-Agent'] = ua
                        # request.meta['proxy'] = 'http://1.1.1.1:1'
                    print proxy

    def process_exception(self, request, exception, spider):
        """
        处理由于使用代理导致的连接异常 则重新换个代理继续请求
        """
        # print '错误类型', exception.message
        if isinstance(exception, self.DONT_RETRY_ERRORS):
            new_request = request.copy()
            try:
                domain = getattr(spider, 'allowed_domains', None)
                if domain:
                    data = db.query('''select url from t_hh_proxy_list where domain ="{}"
                                        and valid>0 order by update_time desc'''.format(domain[0]), )
                    if data:
                        proxy_res = []
                        for item in data:
                            proxy_res.append(item['url'])
                        if proxy_res:
                            proxy = random.choice(proxy_res)
                            request.meta['proxy'] = "%s" % proxy
            except:
                pass
            return new_request

        # def _get_proxy_redis(self, spider):
        #     proxy_res = []
        #     for i in xrange(100):
        #         proxy =redis.get()
        #         if proxy:
        #             proxy_res.append("http://%s" % proxy)
        #     return proxy_res
        #
        # def redis_conn(self,spider):
        #     r = redis.Redis(host='117.122.192.50', port=6479, db=0)
        #     data = r.smembers('proxy:iplist5')
        #     if data:
        #         proxy_res = []
        #         for d in data:
        #             dd = json.loads(d)
        #             proxy_res.append('http://'+str(dd['ip']))
        #         return proxy_res
        #     return []
