# coding=utf8
import json
import threading

import MySQLdb
import redis
# import threadpool
import time,random
import sys

import threadpool

reload(sys)
sys.setdefaultencoding('utf-8')
dt = time.strftime('%Y-%m-%d', time.localtime())

redis_ = redis.Redis(host='10.15.1.11', port=6379)
def hello1(str):
    # time.sleep(2)
    return str

def print_ret(request, result):
    print "%r\n" % (result)
    redis_.hset('dianping:pet_hospital_hash_tt',result,'1')

def deal_task(pool):
    try:
        pool.poll(True)
    except Exception, e:
        print str(e)

#lst = [1,2,3,4,5,6,7]
def main():
    lst = redis_.hgetall('dianping:pet_hospital_hash')

    pool = threadpool.ThreadPool(100)
    requests = threadpool.makeRequests(hello1, lst, print_ret)
    for req in requests:
        pool.putRequest(req)
        # print pool.workers
        #deal_task(pool)

    pool.wait()


def web_db_insert(item):
    #     try:
    #         db.insert('t_hh_dianping_tuangou_deal_info',**data)
    #     except:
    #         pass
    key_str = ','.join('`%s`' % k for k in item.keys())
    value_str = ','.join(
        'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v) for v in
        item.values())
    kv_str = ','.join(
        "`%s`=%s" % (k, 'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v))
        for (k, v)
        in item.items())
    # print kv_str
    # print key_str

    sql = "INSERT INTO t_hh_dianping_shop_info_pet_hospital(%s) VALUES(%s)" % (key_str, value_str)
    sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
    sql = sql.replace('NULL','0')
    print sql

def sql_tt():
    item = {'pic_total': 0, 'city_id': 2, 'address': u'\u751c\u6c34\u56ed\u4e1c\u91cc45\u53f7\u697c\u5e95\u5546',
     'shop_id': 69442816, 'shop_power': 50, 'category2_id': 25147, 'shop_type': 95, 'lng': 116.48148,
     'month_hits': 1830, 'vote_total': None, 'create_dt': '2007-08-13',
     'shop_name': u'\u7f8e\u8054\u4f17\u5408\u52a8\u7269\u533b\u9662(\u7231\u5eb7\u5206\u9662)', 'today_hits': 81,
     'category1_name': u'\u5ba0\u7269', 'category1_id': 95, 'prev_weekly_hits': 374, 'wish_total': None,
     'display_score1': 90, 'display_score3': 86, 'display_score2': 91, 'avg_price': 563, 'lat': 39.928033,
     'weekly_hits': 413, 'dt': '2017-12-26', 'branch_total': None, 'hits': 41862,
     'biz_name': u'\u671d\u9633\u516c\u56ed/\u56e2\u7ed3\u6e56', 'display_score': 90, 'popularity': None,
     'district_id': 14, 'phone_no': u'65042085', 'category2_name': u'\u5ba0\u7269\u5e97', 'shop_status': 0,
     'group_id': 1772935}
    web_db_insert(item)


header_phone_safari = {
    'Referer': 'http://www.dianping.com/shop/74597797',
    'Host': 'www.dianping.com',
    'Accept': 'application/json, text/javascript',
    'Connection': 'keep-alive',
    'Accept-Language': 'zh-cn',
    'Accept-Encoding': 'gzip, deflate',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5',
    'X-Request': 'JSON',
    'X-Requested-With': 'XMLHttpRequest',
}

header_phone_chrome_windows = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

header_phone_opera = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36 OPR/49.0.2725.64',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

header_phone_firefox_ubuntu = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:51.0) Gecko/20100101 Firefox/51.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.5'
}

header_phone_chrome_ubuntu = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
}

header_phone_IE_windows = {

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

header_phone_chrome1 = {
    'Host': 'www.dianping.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'http://www.dianping.com/shop/74597797',
}

import requests
import web
db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


class dianping(object):

    def __init__(self):
        self.redis_ = redis.Redis(host='10.15.1.11', port=6379)
        self.dt_proxy = 0
        self.proxys = ''

    def get_hash_data(self):
        data = self.redis_.hgetall('dianping:pet_hospital_hash_tttt')
        ll = [i for i in data]
        return ll

    def get_dp_detail1(self):
        data = self.get_hash_data()
        pool = threadpool.ThreadPool(100)
        requests = threadpool.makeRequests(hello1, data[:1000], self.get_dp_data)
        for req in requests:
            pool.putRequest(req)
        pool.wait()

    def get_dp_data1(self,request, result):
        print result
        while True:
            header11 = random.choice(
                    [header_phone_chrome_windows, header_phone_safari, header_phone_opera,
                     header_phone_firefox_ubuntu, header_phone_chrome_ubuntu])
            header11['Cookie'] = 'cy=%s; cityid=%s' % (2,2)
            if not self.proxys:
                self.proxys = self.redis_conn1()
            if self.proxys:
                if int(time.time()) - self.dt_proxy > 5:
                    self.dt_proxy = int(time.time())
                    self.proxys = self.redis_conn1()
            proxies = {"http": "%s" % random.choice(self.proxys)}
            shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=%s' % result
            # data = requests.get(,headers=header11)
            try:
                data = requests.get(shop_url, headers=header11, proxies=proxies, timeout=5)
                if data.status_code == 200:
                    print data.content
                    break
            except:

                time.sleep(0.1)

    def get_dp_detail(self):
        data = self.get_dp_url()
        for d in data:
            while True:
                if threading.activeCount()<2:
                    threading.Thread(target=self.get_dp_data,args=(d,)).start()
                    break
                else:
                    time.sleep(1)
            # self.get_dp_data(d)

    def get_dp_data(self, result):
        item = {}
        print result
        result_json = json.loads(result)
        print result_json
        category1_name = result_json.get('category1_name')
        item['category1_name'] = category1_name
        category1_id = result_json.get('category1_id')
        item['category1_id'] = category1_id
        city_id = result_json.get('city_id')
        item['city_id'] = city_id
        # district_id = result_json.get('district_id')
        # item['district_id'] = district_id
        city_name = result_json.get('city_name')
        item['city_name'] = city_name
        # district_name = result_json.get('district_name')
        # item['district_name'] = district_name

        category2_name = result_json.get('shop_info').get('categoryName')
        item['category2_name'] = category2_name
        category2_id = result_json.get('shop_info').get('categoryId')
        item['category2_id'] = category2_id


        # dt = result_json.get('dt')
        item['dt'] = dt
        shop_url = result_json.get('shop_url')
        count = 0
        while count<50:
            print count
            header11 = random.choice(
                    [header_phone_chrome_windows, header_phone_safari, header_phone_opera,
                     header_phone_firefox_ubuntu, header_phone_chrome_ubuntu])
            header11['Cookie'] = 'cy=%s; cityid=%s' % (2,2)
            if not self.proxys:
                self.proxys = self.redis_conn1()
            if self.proxys:
                if int(time.time()) - self.dt_proxy > 5:
                    self.dt_proxy = int(time.time())
                    self.proxys = self.redis_conn1()
            proxies = {"http": "%s" % random.choice(self.proxys)}
            # shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=%s' % result
            # data = requests.get(,headers=header11)
            try:
                data = requests.get(shop_url, headers=header11, proxies=proxies, timeout=5)
                if data.status_code == 200:
                    detail_data = data.content
                    shop_response_json = json.loads(detail_data)
                    shopInfo = shop_response_json.get('msg').get('shopInfo')

                    shop_id = shopInfo.get('shopId')
                    item['shop_id'] = shop_id
                    city_id = shopInfo.get('cityId')
                    item['city_id'] = city_id
                    shop_power = shopInfo.get('shopPower')
                    item['shop_power'] = shop_power
                    shopGroupId = shopInfo.get('shopGroupId')
                    item['group_id'] = shopGroupId
                    hits = shopInfo.get('hits')
                    if hits:
                        item['hits'] = hits
                    else:
                        item['hits'] = 0
                    monthlyHits = shopInfo.get('monthlyHits')
                    if monthlyHits:
                        item['month_hits'] = monthlyHits
                    else:
                        item['month_hits'] = 0
                    create_dt = time.strftime('%Y-%m-%d', time.localtime(int(shopInfo.get('addDate')) / 1000))
                    item['create_dt'] = create_dt
                    district_id = shopInfo.get('newDistrict')
                    item['district_id'] = district_id
                    address = shopInfo.get('address')
                    item['address'] = address
                    lng = shopInfo.get('glng')
                    item['lng'] = lng
                    lat = shopInfo.get('glat')
                    item['lat'] = lat
                    avgPrice = shopInfo.get('avgPrice')
                    item['avg_price'] = avgPrice
                    branch_total = shopInfo.get('branchTotal')
                    item['branch_total'] = branch_total

                    shop_name = shopInfo.get('shopName')
                    branchName = shopInfo.get('branchName')
                    if branchName:
                        item['shop_name'] = shop_name + '(%s)' % branchName
                    else:
                        item['shop_name'] = shop_name
                    item['display_score'] = shopInfo.get('score')
                    item['display_score1'] = shopInfo.get('score1')
                    item['display_score2'] = shopInfo.get('score2')
                    item['display_score3'] = shopInfo.get('score3')
                    phoneNo = shopInfo.get('phoneNo')
                    item['phone_no'] = phoneNo
                    item['pic_total'] = shopInfo.get('picTotal')
                    item['popularity'] = shopInfo.get('popularity')
                    item['shop_type'] = shopInfo.get('newShopType')
                    item['vote_total'] = shopInfo.get('voteTotal')
                    item['wish_total'] = shopInfo.get('wishTotal')
                    item['district_id'] = shopInfo.get('newDistrict')
                    # item['shop_status'] = shop_info.get('status')
                    item['dt'] = dt
                    prevWeeklyHits = shopInfo.get('prevWeeklyHits')
                    if prevWeeklyHits:
                        item['prev_weekly_hits'] = prevWeeklyHits
                    else:
                        item['prev_weekly_hits'] = 0
                    todayHits = shopInfo.get('todayHits')
                    if todayHits:
                        item['today_hits'] = todayHits
                    else:
                        item['today_hits'] = 0
                    weeklyHits = shopInfo.get('weeklyHits')
                    if weeklyHits:
                        item['weekly_hits'] = weeklyHits
                    else:
                        item['weekly_hits'] = 0
                    self.web_db_insert(item)
                    break
            except Exception,e:
                print e
                print shop_url
                time.sleep(0.1)
                count+=1

    def insert_data_redis(self):
        ll = self.redis_.lrange('dianping:pet_hospital',0,10)
        for l in ll:
            print l
            self.redis_.lpush('dianping:pet_hospital_tttt',l)

    def hello1(self,str):
        # time.sleep(2)
        return str

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

    def web_db_insert(self,item):
        #     try:
        #         db.insert('t_hh_dianping_tuangou_deal_info',**data)
        #     except:
        #         pass
        key_str = ','.join('`%s`' % k for k in item.keys())
        value_str = ','.join(
            'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v) for v in
            item.values())
        kv_str = ','.join(
            "`%s`=%s" % (k, 'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v))
            for (k, v)
            in item.items())
        # print kv_str
        # print key_str

        sql = "INSERT INTO t_hh_dianping_shop_info_pet_hospital(%s) VALUES(%s)" % (key_str, value_str)
        sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
        sql = sql.replace('NULL', '0')
        print sql
        try:
            db.query(sql)
        except:
            pass
        item.pop('prev_weekly_hits')
        item.pop('today_hits')
        item.pop('weekly_hits')
        item.pop('dt')
        key_str = ','.join('`%s`' % k for k in item.keys())
        value_str = ','.join(
            'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v) for v in
            item.values())
        kv_str = ','.join(
            "`%s`=%s" % (k, 'NULL' if v is None or v == 'NULL' else "'%s'" % MySQLdb.escape_string('%s' % v))
            for (k, v)
            in item.items())
        # print kv_str
        # print key_str

        sql = "INSERT INTO t_hh_dianping_shop_info(%s) VALUES(%s)" % (key_str, value_str)
        sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, kv_str)
        sql = sql.replace('NULL', '0')
        print sql
        try:
            db.query(sql)
        except:
            pass

    def get_dp_url(self):
        data = self.redis_.lrange('dianping:pet_hospital',0,1000)
        # data = self.redis_.lrange('dianping:pet_hospital', 0, -1)
        ll = [i for i in data]
        return ll

    def get_dp_map_url(self):
        with open('map_url') as f:
            data = f.readlines()
        for d in data:
            url = d.replace('\n','')
            print url
            while True:
                header11 = random.choice(
                    [header_phone_chrome_windows, header_phone_safari, header_phone_opera,
                     header_phone_firefox_ubuntu, header_phone_chrome_ubuntu])
                header11['Cookie'] = 'cy=%s; cityid=%s' % (2, 2)
                if not self.proxys:
                    self.proxys = self.redis_conn1()
                if self.proxys:
                    if int(time.time()) - self.dt_proxy > 5:
                        self.dt_proxy = int(time.time())
                        self.proxys = self.redis_conn1()
                proxies = {"http": "%s" % random.choice(self.proxys)}
                # shop_url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=%s' % result
                # data = requests.get(,headers=header11)
                try:
                    data = requests.get(url, headers=header11, proxies=proxies, timeout=5)
                    if data.status_code == 200:
                        print data.content
                    break
                except Exception,e:

                    print e


if __name__ == '__main__':
    dp = dianping()
    dp.get_dp_detail()
    # get_dddddd()
    # dp.get_dp_map_url()