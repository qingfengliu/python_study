# -*- coding: utf-8 -*-

import random
import time
import web
import redis
import json


dt = 0
class ProxyMiddleware(object):
    
    def __init__(self):
        self.proxys = ''
        self.dt = 0
    
    def process_request(self, request, spider):
        if not self.proxys:
            self.proxys = self.redis_conn1()
        if self.proxys:
            proxy = random.choice(self.proxys)
            # if not divmod(int(time.time()), 5)[1]:
            if int(time.time())-self.dt>5:
                self.dt = int(time.time())
                self.proxys = self.redis_conn1()
            request.meta['proxy'] = "%s" % proxy
            # print '**************%s******************' % proxy
            request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'

def redis_conn(self):
    r = redis.Redis(host='117.122.192.50', port=6479, db=0)
    data = r.smembers('proxy:iplist2')
    if data:
        proxy_res = []
        for d in data:
            dd = json.loads(d)
            proxy_res.append('http://' + str(dd['ip']))
            return proxy_res
        return []
    
    def redis_conn1(self):
        r = redis.Redis(host='xxxx', port=52385, db=0)
        data = r.smembers('proxy')
        if data:
            proxy_res = []
            for d in data:
                dd = json.loads(d)
                proxy_res.append('http://' + str(dd['ip']).replace(':52384',':42384'))
            return proxy_res
        
        return []
