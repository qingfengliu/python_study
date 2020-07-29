# coding=utf8
import json
import re
import time

import redis
import web
import requests
db_insert = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')

header = {
    'Host': 't.dianping.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    # 'Cookie': '_hc.v="\"0f6e7827-bc24-4e37-a02d-22712123f3b9.1487061681\""; cy=2; cye=beijing; __utma=1.1489577661.1497328059.1497499189.1498534352.4; __utmz=1.1497328059.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); s_ViewType=10; aburl=1; _lxsdk_cuid=15d5e54f3cba-01cf8580d990b58-49556d-13c680-15d5e54f3ccc8; _lxsdk=15d5e54f3cba-01cf8580d990b58-49556d-13c680-15d5e54f3ccc8; PHOENIX_ID=0a0107c7-15d5e54fd77-341cb35; JSESSIONID=F685CCCF2AF61B530EAE7A880C2DC155; __mta=250010169.1500538919483.1500538923009.1500538940585.3',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0, no-cache',
    'Pragma': 'no-cache'
}


def redis_conn1():
    r = redis.Redis(host='www.fxtome.com', port=52385, db=0)
    data = r.smembers('proxy')
    if data:
        proxy_res = {}
        for d in data:
            dd = json.loads(d)
            proxy_res['http']='http://' + str(dd['ip'])
        return proxy_res
    return []

def get_data(url,deal_id):
    proxy = redis_conn1()
    data = requests.get(url=url,headers=header, proxies=proxy)
    # data = requests.get(url=str(url), headers=header)
    print data.status_code
    start_time = ''.join(re.findall("beginDate:'(.*?)',",data.content))
    end_time = ''.join(re.findall("endDate:'(.*?)',",data.content))
    print start_time
    print end_time
    if start_time:
        # sql = "update t_hh_dianping_tuangou_deal_info set start_time='%s',end_time='%s' where deal_id='%s';" % (start_time,end_time,deal_id)
        # print sql
        # db_insert.query(sql)
        with open('deal_id_se','a') as f:
            f.write(deal_id+','+start_time+','+end_time+'\n')
    else:
        print deal_id


def open_file():
    with open('../deal_id.csv') as f:
        data = f.readlines()
    for d in data:
        line = d.replace('\n','').replace('\r','')
        url = 'http://t.dianping.com/deal/'+line
        # url = 'http://t.dianping.com/deal/19124266'
        print url
        get_data(url,line)
        # time.sleep(2)

def tt():
    url = 'http://t.dianping.com/deal/21860206'
    # url = 'http://t.dianping.com/deal/19124266'
    data = requests.get(url,headers=header)
    print data.status_code

def qie():
    str = '渝北区邹氏牙科门诊'
    str = str[:str.index('牙')]
    print str

if __name__ == '__main__':
    # tt()
    qie()