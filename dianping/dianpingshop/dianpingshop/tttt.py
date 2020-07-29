# coding=utf8
import json
import random

import redis
import requests
import web


def tt():
    data = {'category1_name': u'\u5ba0\u7269', 'category2_name': u'\u5ba0\u7269\u5e97', 'category1_id': 95, 'city_id': 6, 'category2_id': 25147, 'city_name': u'\u82cf\u5dde', 'dt': '2018-01-17'}
    print json.dumps(data)

def redis_conn1():
    r = redis.Redis(host='116.196.71.111', port=52385, db=0)
    data = r.smembers('proxy_data5u')
    if data:
        proxy_res = []
        for d in data:
            dd = json.loads(d)
            proxy_res.append('http://' + str(dd['ip']))
        return proxy_res
    return []

ua_list = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/58.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/56.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/55.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/54.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/53.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/52.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/51.0"
    ]

def ttt():

    db = web.database(dbn='mysql', db='o2o', user='reader', pw='xxxx', port=3306, host='xxxx')

    header2 = {'Host': 'www.dianping.com',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
               'Accept-Encoding': 'gzip, deflate',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1',
               'Pragma': 'no-cache',
               'Cache-Control': 'no-cache'}
    data = db.query(
        "select distinct city_id,city_name,district_id,district_name from "
        "t_hh_dianping_business_area where city_id=2;")
    data_category = db.query(
        "select distinct category1_id,category1_name,category2_id,category2_name "
        "from t_hh_dianping_category where category2_name in ('宠物医院','宠物店');")
    list_category = []
    for dc in data_category:
        category_item = {}
        category_item['category1_id'] = dc.category1_id
        category_item['category1_name'] = dc.category1_name
        category_item['category2_id'] = dc.category2_id
        category_item['category2_name'] = dc.category2_name
        list_category.append(category_item)

    for d in data:
        city_id = d.city_id
        city_name = d.city_name
        district_id = d.district_id
        district_name = d.district_name
        for dc in list_category:
            category1_id = dc.get('category1_id')
            category1_name = dc.get('category1_name')
            category2_id = dc.get('category2_id')
            category2_name = dc.get('category2_name')
            # url = 'http://www.dianping.com/search/map/ajax/json?cityId={}&categoryId={}&regionId={}'.format(
            #     city_id,
            #     category2_id, district_id)
            # url = 'http://www.dianping.com/search/map/ajax/json?cityId=%s' \
            #       '&promoId=0&shopType=%s&categoryId=%s&regionId=%s' \
            #       '&sortMode=2&shopSortItem=0&searchType=1&branchGroupId=0' \
            #       '&aroundShopId=0&shippingTypeFilterValue=0' % (city_id,category1_id,category1_id,district_id)
            url = 'http://www.dianping.com/search/map/ajax/json?cityId=%s' \
                  '&promoId=0&shopType=%s&categoryId=%s&regionId=%s&sortMode=2' \
                  '&shopSortItem=0&keyword=&searchType=1&branchGroupId=0&' \
                  'aroundShopId=0&shippingTypeFilterValue=0&page=1' % \
                  (city_id, category1_id, category1_id, district_id)
            print url
            header2['User-Agent'] = random.choice(ua_list)
            header2[
                'Referer'] = 'https://www.baidu.com/link?url=6bHIcRVwxE5p1jNSamTaLAXEeV5RL1LR-0kCH1VsKEud6XJRkBbzHOG7aJWF0Hu_&wd=&eqid=dcaaa61a0002ad50000000025a4c59c0'
            data = requests.get(url,headers=header2,proxies={'http':random.choice(proxy_list)})
            print data.status_code


def post_tt():
    '''-H  -H  -H  --data 'cityId=1&cityEnName=shanghai&promoId=0&shopType=95&categoryId=25148&sortMode=2&shopSortItem=0&keyword=&searchType=1&branchGroupId=0&aroundShopId=0&shippingTypeFilterValue=0&page=1&regionId=1'''
    url = 'http://www.dianping.com/search/map/ajax/json'
    header1 = {
        'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Accept': 'application/json, text/javascript',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Referer': 'http://www.dianping.com/search/map/category/1/0',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Request': 'JSON',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8;',
        'Cookie': 'cy=2',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    proxy_list = redis_conn1()
    body = 'cityId=1&cityEnName=shanghai&promoId=0&shopType=95&categoryId=25148&sortMode=2&shopSortItem=0&keyword=&searchType=1&branchGroupId=0&aroundShopId=0&shippingTypeFilterValue=0&page=1&regionId=1'
    for i in xrange(100):
        try:
            header1['User-Agent'] = random.choice(ua_list)
            data = requests.post(url,headers=header1,data=body,proxies={'http':random.choice(proxy_list)},timeout=2)
            print data.status_code
        except Exception,e:
            print e

def basic_tt():
    '''-H  -H  -H  --data 'cityId=1&cityEnName=shanghai&promoId=0&shopType=95&categoryId=25148&sortMode=2&shopSortItem=0&keyword=&searchType=1&branchGroupId=0&aroundShopId=0&shippingTypeFilterValue=0&page=1&regionId=1'''
    url = 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?shopId=2233282&platform=1&partner=150&originUrl=http://www.dianping.com/shop/2233282'
    header1 = {
        'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Accept': 'application/json, text/javascript',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8;',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    proxy_list = redis_conn1()
    for i in xrange(100):
        try:
            # header1['User-Agent'] = random.choice(ua_list)
            data = requests.get(url,headers=header1,proxies={'http':random.choice(proxy_list)},timeout=2)
            print data.status_code
        except Exception,e:
            print e


def selenium_tt():
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.support.ui import WebDriverWait  #  available since 2.4.0
    import time
    driver = webdriver.Chrome()
    driver.get("http://www.dianping.com/shop/56637267")
    # print driver.execute_script('Rohr_Opt.reload("http://t.dianping.com/ajax/dealGroupShopDetail?dealGroupId=22309627&cityId=2&action=shops&page=2&regionId=0");')
    print driver.execute_script('return Rohr_Opt.reload("http://t.dianping.com/ajax/dealGroupShopDetail?dealGroupId=22309627&cityId=2&action=shops&page=2&regionId=0");')

    time.sleep(10)
    driver.close()

if __name__ == '__main__':
    selenium_tt()