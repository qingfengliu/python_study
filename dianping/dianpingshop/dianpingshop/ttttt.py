# coding=utf8

import json
import random
import os
import redis
import requests
import time

header = {
        'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Accept': 'application/json, text/javascript',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8;',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Cookie':'cy=2'
    }


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

def tt():
    with open('no_result_item') as f:
        data = f.readlines()
    for d in data:
        d_json = json.loads(d.replace('\n',''))
        url = d_json.get('url')
        shop_id = d_json.get('shop_id')
        print url
        print shop_id
        proxies = redis_conn1()
        header['Referer'] = 'http://www.dianping.com/shop/%s' % shop_id
        try:
            data_no = requests.get(url,headers=header,timeout=5,proxies={'http':random.choice(proxies)})
            print data_no.content
        except:
            pass


def get_data():
    header = {
        'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Accept': 'application/json, text/javascript',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'http://www.dianping.com/shop/74590440',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Request': 'JSON',
        'Cookie': 'cy=2;',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    print int(time.time()*1000)
    url = 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?_nr_force='+str(int(time.time()*1000))+'&shopId=74590440&platform=1&partner=150&originUrl=http%3A%2F%2Fwww.dianping.com%2Fshop%2F74590440'
    print url
    proxies = redis_conn1()
    pp = random.choice(proxies)
    print pp
    # data = requests.get(url,headers=header,proxies={"http":pp,})
    data = requests.get(url, headers=header)
    print data.content


success = 0
def shop_id_tt(shop_id):
    global success
    url = 'http://0.0.0.0:52525/%s/' % shop_id
    # body = {'shop_id':'6232395'}
    # data= requests.post(url,data=body)
    data = requests.get(url)
    token = data.content
    print token
    header = {
        'Host': 'www.dianping.com',
        # 'Connection': 'keep-alive',
        'Accept': 'application/json, text/javascript',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Request': 'JSON',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        'Referer': 'http://www.dianping.com/shop/%s' % shop_id,
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': '_lxsdk_cuid=162cd4bb8d2c8-010bb08a97e19f-33697b04-13c680-162cd4bb8d2c8; _lxsdk=162cd4bb8d2c8-010bb08a97e19f-33697b04-13c680-162cd4bb8d2c8; _hc.v=05446d47-a781-be7d-d29a-15f8a9a87848.1523862715; _lxsdk_s=162cda1312c-956-8b2-0e8%7C%7C0'

    }
    url = 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?&shopId=%s&_token=%s' % (shop_id,token.replace('"',''))
    print url
    try:
        proxy = {'http':'%s' % random.choice(redis_conn1())}
        print proxy
        data = requests.get(url,headers=header,proxies=proxy,timeout=5)
        result = data.content
        if 'parkReviewCount' in result:
            success += 1
            print success
        if 'userIp' in result:
            pass
        else:
            print result
        print result


    except:
        pass


def wxs_topic_list_tt():
    header = {
        'charset': 'utf-8',
        'referer': 'https://servicewechat.com/wxa344448166586158/140/page-frame.html',
        'content-type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Letv X500 Build/DBXCNOP5902812084S; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/043909 Mobile Safari/537.36 MicroMessenger/6.6.5.1280(0x26060536) NetType/4G Language/zh_CN MicroMessenger/6.6.5.1280(0x26060536) NetType/WIFI Language/zh_CN',
        'Host': '39916353.share1diantong.com'
    }
    body = {
        'pagenum':1,
        'orderby_index':'0',
        'cate':'全部',
        'topic_status':'1',
        'topic_is_new':'0',
        'topic_is_local':'0',
        'version':'2',
        # 'uuid':'67a18a680f76f4e643e28f5f75933cbc',
        'release_version':'20180223',
        'AppID':'wxa344448166586158'
    }
    url = 'https://39916353.share1diantong.com/index/topiclst_v2/'
    data = requests.post(url,data=body,headers=header)
    print data.content

def wxs_topicinfo_tt():
    header = {
        'charset': 'utf-8',
        'referer': 'https://servicewechat.com/wxa344448166586158/140/page-frame.html',
        'content-type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Letv X500 Build/DBXCNOP5902812084S; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/043909 Mobile Safari/537.36 MicroMessenger/6.6.5.1280(0x26060536) NetType/4G Language/zh_CN MicroMessenger/6.6.5.1280(0x26060536) NetType/WIFI Language/zh_CN',
        'Host': '39916353.share1diantong.com'
    }
    body = {
        'topic_id':'159205',
        'release_version':'20180223',
        'AppID':'wxa344448166586158',
        'uuid':'67a18a680f76f4e643e28f5f75933cbc'
    }
    url = 'https://39916353.share1diantong.com/topic/topicinfo'
    data = requests.post(url,data=body,headers=header)
    print data.content

def wxs_get_comments_lst_tt():
    header = {
        'charset': 'utf-8',
        'referer': 'https://servicewechat.com/wxa344448166586158/140/page-frame.html',
        'content-type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Letv X500 Build/DBXCNOP5902812084S; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/043909 Mobile Safari/537.36 MicroMessenger/6.6.5.1280(0x26060536) NetType/4G Language/zh_CN MicroMessenger/6.6.5.1280(0x26060536) NetType/WIFI Language/zh_CN',
        'Host': '39916353.share1diantong.com'
    }
    body = {
        'topic_id':'159205',
        'release_version':'20180223',
        'AppID':'wxa344448166586158',
    }
    url = 'https://39916353.share1diantong.com/topic/get_comments_lst'
    data = requests.post(url,data=body,headers=header)
    print data.content

def ttttttttt():
    print os.system('''curl 'http://www.dianping.com/ajax/json/shopDynamic/basicHideInfo?_nr_force=1523878923596&shopId=56637267&_token=eJx1T8FOwzAM%2FRefo8ZJ0ySttAMIDoGVAyq7TD1sXdWWaV3VVAQV8e84Ag4ckCz5vefnJ%2FsDZneCQiCiEgze2hkKEAkmGhgsniaZTK2xuUxTtAyav1qW09Jx3t1BsdcqYzq3dRSeie%2BFUshyxJp9Qyt0zaSiih5HFuiXZSo4DyEkp%2BEwTsPYJc31wn1%2FnXimdWqkNnTJf75jO7wS5k0vkHdCIFD0pYrREg2TUkThHAXqh5%2B%2B%2FPKSnqV0P3QjofbhvXr0xm35bemrF1GupXxa3boL7mY74%2F05bDbw%2BQX4oFPr&uuid=83bf478b-2e08-c798-aeb7-84e773c6cfda.1523878709&platform=1&partner=150&originUrl=http%3A%2F%2Fwww.dianping.com%2Fshop%2F56637267' -H 'Cookie: cy=2; cye=beijing; _lxsdk_cuid=162ce3fc847c8-0bd6f33a784062-33697b04-13c680-162ce3fc847c8; _lxsdk=162ce3fc847c8-0bd6f33a784062-33697b04-13c680-162ce3fc847c8; _hc.v=83bf478b-2e08-c798-aeb7-84e773c6cfda.1523878709; s_ViewType=10; _lxsdk_s=162ce3fc849-50c-038-c6f%7C%7C59' -H 'Accept-Encoding: gzip, deflate' -H 'X-Request: JSON' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36' -H 'Accept-Language: zh-CN,zh;q=0.9' -H 'Accept: application/json, text/javascript' -H 'Referer: http://www.dianping.com/shop/56637267' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed''')

if __name__ == '__main__':
    # count = 0
    # while count<200:
    #     shop_id_tt('92465668')
    #     count+=1
    # wxs_topic_list_tt()
    # wxs_topicinfo_tt()
    wxs_get_comments_lst_tt()
    # while True:
    #     ttttttttt()