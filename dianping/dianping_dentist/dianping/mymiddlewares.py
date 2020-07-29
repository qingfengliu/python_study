# -*- coding: UTF-8 -*-
# Created by dev on 16-5-27.

import redis, json
from twisted.web._newclient import ResponseNeverReceived
from twisted.internet.error import TimeoutError, ConnectionRefusedError, ConnectError
proxy_key = "proxy:iplist"


class ProxyMiddleware(object):
    REDIS_HOST = 'xxxx'
    REDIS_PORT = 6479
    redisclient = redis.Redis(REDIS_HOST, REDIS_PORT)
    DONT_RETRY_ERRORS = (TimeoutError, ConnectionRefusedError, ResponseNeverReceived, ConnectError, ValueError)

    def process_request(self, request, spider):
        """
        将request设置为使用代理
        """
        try:
            self.redisclient = redis.Redis(self.REDIS_HOST, self.REDIS_PORT)
            proxy = self.redisclient.srandmember(proxy_key)
            proxyjson = json.loads(proxy)
            ip = proxyjson["ip"]
            print ip
            request.meta['proxy'] = "http://%s" % ip
        except Exception, ee:
            # print '------------------------------', ee
            pass

    def process_exception(self, request, exception, spider):
        """
        处理由于使用代理导致的连接异常 则重新换个代理继续请求
        """
        # print '错误类型', exception.message
        if isinstance(exception, self.DONT_RETRY_ERRORS):
            new_request = request.copy()
            try:
                self.redisclient = redis.Redis(self.REDIS_HOST, self.REDIS_PORT)
                proxy = self.redisclient.srandmember(proxy_key)
                proxyjson = json.loads(proxy)
                ip = proxyjson["ip"]
                new_request.meta['proxy'] = "http://%s" % ip
            except:
                pass
            return new_request