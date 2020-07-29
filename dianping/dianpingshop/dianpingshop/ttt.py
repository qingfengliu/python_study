# coding=utf8
import random
import sys
import json
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import time
import web
import requests,re

def tt():
    # db = web.database(dbn='mysql', db='xxxx', user='writer', pw='xxxx', port=3306, host='xxxx')
    db = web.database(dbn='mysql', db='o2o', user='root', pw='123456', port=3306, host='127.0.0.1')
    # db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


    with open('eeeeddddd.sql') as f:
        data = f.readlines()
    for d in data:
        line = d.replace('\n','')
        print line
        db.query(line)

def write_file(content):
    with open('swj.txt','a') as f:
        f.write(content)

def get_content(title,id):
    head = {

        'Host': 'www.bxwx9.org',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'http://www.bxwx9.org/b/75/75550/index.html'
    }
    content_data = requests.get('http://www.bxwx9.org/b/75/75550/'+str(id)+'.html', headers=head)
    # print content_data.content
    content = re.findall('<div id="content"><div id="adright"></div>(.*?)</div>', content_data.content)
    print content
    try:
        content = ''.join(content).decode('gbk')
    except:
        content = ''.join(content)
    content = content.replace('<br />', '').replace('&nbsp;', '')
    # print content
    write_file(title+' '+content+ '\n')

def swj_tt():
    data = requests.get('http://www.bxwx9.org/b/75/75550/index.html')
    # print data.content
    url_list = re.findall('<a href="(\d+).html">(第.*?)</a><',data.content)
    dict_swj = {}
    for url in url_list:
        dict_swj[url[1]]=url[0]
    print dict_swj
    dict_swj = sorted(dict_swj.items(), key=lambda d: d[1])
    # for key,val in dict_swj.items():
    #     print val,key
    for dd in dict_swj:
        print dd[0],dd[1]
        get_content(dd[0],dd[1])

def encode22(s):
    return ''.join([bin(ord(c)).replace('0b', '') for c in s])


def get_data_pet_hospital_phone():
    url = 'https://m.dianping.com/isoapi/module'
    header = {
        'User-Agent': 'Mozilla/5.0 (Linux; U; Android 6.0;zh_cn; Letv X500 Build/DBXCNOP5902605181S) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/49.0.2623.91 Mobile Safari/537.36 EUI Browser/1.6.1.71',
        'content-type': 'application/json'
    }
    # proxies = {"http": "http://58.216.241.181:4129"}
    post_data = {"pageEnName":"shopList","moduleInfoList":[{"moduleName":"mapiSearch","query":{"search":{"start":1,"categoryid":"95","limit":200,"cityid":2},"loaders":"list"}}]}
    # data = requests.post(url,data=urllib.urlencode(post_data),headers=header, proxies=proxies)
    data = requests.post(url, data=json.dumps(post_data), headers=header)
    print data.content
    # print data.text
    # print data.status_code
import re
def get_get(data):
    dd = re.findall('shop_id": (\d+),',data)
    with open('result.csv','a') as f:
        f.write(''.join(dd)+'\n')

def get_shop_id():
    ll = []
    with open('pet_hospital_phone122111.jsonlines') as f:
        data = f.readlines()
    for d in data:
        shop_id = ''.join(re.findall('shop_id": (\d+),',d))
        ll.append(shop_id)
    return ll

import threading

def gg_dd(l):
    url = 'http://www.dianping.com/ajax/json/shopfood/wizard/BasicHideInfoAjaxFP?shopId=%s' % l
    print url
    header_tt = {
        # 'Host': 'www.dianping.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:57.0) Gecko/20100101 Firefox/57.0',
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        # 'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding': 'gzip, deflate',
        'Cookie': 'cy=2; cityid=2',
        # 'Connection': 'keep-alive',
        # 'Upgrade-Insecure-Requests': '1',
        # 'Pragma': 'no-cache',
        # 'Cache-Control': 'no-cache'
    }
    ll = [i for i in xrange(4129,4169)]
    proxies = {"http": "http://58.216.241.181:%s" % random.choice(ll)}
    try:
        data = requests.get(url, headers=header_tt, proxies=proxies, timeout=5)
        print data.content
        # print data.headers
        # print data.request.headers


    except Exception, e:
        # print e
        pass

def get_shop_info_data_tt():
    ll = get_shop_id()
    count = 0
    for l in ll:
        while True:
            if threading.activeCount()<100:
                threading.Thread(target=gg_dd,args=(l,)).start()
                count+=1
                print count
                break
            else:
                time.sleep(1)

def tttttt():
    for request_count in xrange(1,1000):
        if divmod(request_count, 64)[1] == 0:
            print request_count

def tttttttt():
    sss = '''_hc.v="\"4a82d47c-c063-4fa7-ac59-32b2b10c3195.1489996100\""; _lxsdk_cuid=15e9d172822c8-05dcbc4b4e4312-143c6c55-13c680-15e9d172822c8; _lxsdk=15e9d172822c8-05dcbc4b4e4312-143c6c55-13c680-15e9d172822c8; s_ViewType=10; aburl=1; cy=2; cye=beijing; _lxsdk_s=1608d78091f-db7-5ba-5ce%7C%7C29'''
    dd = {}
    for l in sss.split('; '):
        l_l = l.split('=')
        dd[l_l[0]] = l_l[1]
    print dd

import PyV8

class v8Doc(PyV8.JSClass):
     def write(self, s):
             print s.decode('utf-8')

class Global(PyV8.JSClass):
     def __init__(self):
             self.document = v8Doc()
import execjs
glob = Global()
ctxt = PyV8.JSContext(glob)
ctxt.enter()
#or ctxt.eval(u'document.write("你好，中国")') for Linux
ctxt.eval(u'document.write("你好，中国")'.encode('utf-8'))
jiaMiPasswd = execjs.compile(open(r"shop_id.js").read().decode("utf-8")).call('rohr.reload', 'http://www.dianping.com?shopId=9964442')
print jiaMiPasswd

if __name__ == '__main__':
    # swj_tt()
    # get_content('第51章 四大学院的比试','13788315')
    # print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    # get_data_pet_hospital_phone()
    # for i in sys.stdin:
    #     get_get(i)
    # count = 0
    # while True:
    #     try:
    #         get_shop_info_data_tt()
    #         count+=1
    #         print count
    #     except Exception,e:
    #         print e
    #     time.sleep(0.1)

    # tttttttt()
    pass