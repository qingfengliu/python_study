# -*- coding: utf-8 -*-
import json
import random
import sys

import redis
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.proxy import ProxyType
from selenium.webdriver.common.proxy import Proxy

reload(sys)
sys.setdefaultencoding('utf-8')
import time
from selenium import webdriver


def redis_conn1():
    r = redis.Redis(host='xxxx', port=52385, db=0)
    data = r.smembers('proxy')
    if data:
        proxy_res = []
        for d in data:
            dd = json.loads(d)
            proxy_res.append(dd.get('ip'))
            # proxy_res.append('http://' + str(dd['ip']))
        return proxy_res
    return []


cookies = {
    'domain': '.dianping.com',
    'cy': '2; ',
    'cye': 'beijing; ',
    '__utma': '1.2016944627.1503913333.1508380646.1508410598.4; ',
    '__utmz': '1.1508380646.3.3.utmcsr=dianping.com|utmccn=(referral)|utmcmd=referral|utmcct=/;',
    '__mta': '218390182.1508410622795.1508410622795.1508411087614.2; ',
    's_ViewType': '10; ',
    'aburl': '1; ',
    '_lxsdk_cuid': '15f34478accc8-0036528b7bf6dd-49576f-13c680-15f34478accc8; ',
    '_lxsdk': '15f34478accc8-0036528b7bf6dd-49576f-13c680-15f34478accc8; ',
    '_lx_utm': 'utm_source%3DBaidu%26utm_medium%3Dorganic; ',
    'JSESSIONID': 'B6B683D3F115418041ED0AFCE0971147; ',
    '_lxsdk_s': '15f77681817-db4-bf6-14e%7C%7C13',
}

def get_data(shop_id):
    url = 'http://www.dianping.com/shop/6232395'
    # load PhantomJS
    driver = webdriver.PhantomJS()
    proxy_list = redis_conn1()
    if proxy_list:
        print proxy_list
        desired_capabilities = webdriver.DesiredCapabilities.PHANTOMJS.copy()
        desired_capabilities[
            "phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0"
        # desired_capabilities[
        #     "phantomjs.page.settings.cookies"] = ''' _hc.v="\"0f6e7827-bc24-4e37-a02d-22712123f3b9.1487061681\""; cy=2; cye=beijing; __utma=1.2016944627.1503913333.1508380646.1508410598.4; __utmz=1.1508380646.3.3.utmcsr=dianping.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __mta=218390182.1508410622795.1508410622795.1508411087614.2; s_ViewType=10; aburl=1; _lxsdk_cuid=15f34478accc8-0036528b7bf6dd-49576f-13c680-15f34478accc8; _lxsdk=15f34478accc8-0036528b7bf6dd-49576f-13c680-15f34478accc8; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; JSESSIONID=B6B683D3F115418041ED0AFCE0971147; _lxsdk_s=15f77681817-db4-bf6-14e%7C%7C13'''

        proxy = webdriver.Proxy()
        proxy.proxy_type = ProxyType.MANUAL
        proxy.http_proxy = random.choice(proxy_list)
        # 将代理设置添加到webdriver.DesiredCapabilities.PHANTOMJS中
        proxy.add_to_capabilities(desired_capabilities)
        driver.start_session(desired_capabilities)
        driver.add_cookie({'path': '/', 'name': 'JSESSIONID', 'value': 'B6B683D3F115418041ED0AFCE0971147;',
                           'domain': '.dianping.com'})
        # for key,value in cookies.items():
        #     driver.add_cookie({
        #         'name': key,
        #         'value': value,
        #         'path': '/',
        #         'domain': '.dianping.com'
        #     })
        # print driver.get_cookies()
    print driver.desired_capabilities
    driver.set_page_load_timeout(5)
    driver.get(url)
    print driver.get_cookies()
    # start Scrollbar
    js1 = 'return document.body.scrollHeight'
    js2 = 'window.scrollTo(0, document.body.scrollHeight)'
    old_scroll_height = 0
    while (driver.execute_script(js1) > old_scroll_height):
        old_scroll_height = driver.execute_script(js1)
        driver.execute_script(js2)
        time.sleep(3)
    # get url  by xpath
    print driver.page_source
    list1 = driver.find_elements_by_xpath('//div[@class="comment-condition J-comment-condition Fix"]/div/span/a')
    for l in list1:
        print l.text


def get_data1():
    # url = 'http://www.dianping.com/shop/6232395'
    url = 'http://www.ip138.com/'
    from selenium import webdriver
    # 假定9999端口开启tor服务
    service_args = ['--proxy=%s' % random.choice(redis_conn1()), '--proxy-type=http', ]
    driver = webdriver.PhantomJS(service_args=service_args)
    driver.get(url)
    js1 = 'return document.body.scrollHeight'
    js2 = 'window.scrollTo(0, document.body.scrollHeight)'
    old_scroll_height = 0
    # while (driver.execute_script(js1) > old_scroll_height):
    #     old_scroll_height = driver.execute_script(js1)
    #     driver.execute_script(js2)
    #     time.sleep(1)
    # get url  by xpath
    print driver.page_source
    list1 = driver.find_elements_by_xpath('//div[@class="comment-condition J-comment-condition Fix"]/div/span/a')
    for l in list1:
        print l.text


def get_data2():
    ip_list = random.choice(redis_conn1())

    from selenium import webdriver
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--proxy-server=http://%s' % ip_list)
    chrome = webdriver.Chrome(chrome_options=chrome_options)
    chrome.set_page_load_timeout(10)
    time.sleep(1)
    # chrome = webdriver.Chrome()
    try:
        chrome.get('http://www.dianping.com/shop/6232395')
        print(chrome.page_source)
        print chrome.get_cookies()
        if '403 Forbidden' in chrome.page_source:
            time.sleep(20)
    except:
        pass
    chrome.quit()


def dynamic_load(url):
    desired_capabilities = DesiredCapabilities.PHANTOMJS.copy()
    # 从USER_AGENTS列表中随机选一个浏览器头，伪装浏览器
    desired_capabilities[
        "phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0"
    # 不载入图片，爬页面速度会快很多
    desired_capabilities["phantomjs.page.settings.loadImages"] = False
    # 利用DesiredCapabilities(代理设置)参数值，重新打开一个sessionId，我看意思就相当于浏览器清空缓存后，加上代理重新访问一次url
    proxy = webdriver.Proxy()
    proxy.proxy_type = ProxyType.MANUAL
    proxy.http_proxy = random.choice(redis_conn1())
    proxy.add_to_capabilities(desired_capabilities)
    # 打开带配置信息的phantomJS浏览器
    driver = webdriver.PhantomJS(desired_capabilities=desired_capabilities)
    # driver = webdriver.PhantomJS()
    # driver.start_session(desired_capabilities)
    # 隐式等待5秒，可以自己调节
    driver.implicitly_wait(5)
    # 设置10秒页面超时返回，类似于requests.get()的timeout选项，driver.get()没有timeout选项
    # 以前遇到过driver.get(url)一直不返回，但也不报错的问题，这时程序会卡住，设置超时选项能解决这个问题。
    driver.set_page_load_timeout(100)
    # 设置10秒脚本超时时间
    driver.set_script_timeout(100)

    driver.get(url)
    # next_page=driver.find_element_by_id (idd)#.get_attribute('href')
    # driver.get(next_page)
    # next_page
    # html = BeautifulSoup(driver.page_source, 'xml').prettify()
    print driver.page_source
    # return html


def tt():
    import requests
    from bs4 import BeautifulSoup
    from selenium import webdriver
    import time
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    params = DesiredCapabilities.PHANTOMJS  # 这本身是一个dict格式类属性
    params['phantomjs.page.settings.userAgent'] = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/53 "
                                                   "(KHTML, like Gecko) Chrome/15.0.87")  # 在这个类属性里加上一个“phantomjs.page.settings.userAgent”
    driver = webdriver.PhantomJS(desired_capabilities=params)
    print(params)  # 打印下这个属性看看还有那些参数
    driver.get(
        'http://www.dianping.com/shop/6232395')  # 这时候get方法请求的时候调用DesiredCapabilities.PHANTOMJS属性的时候就会用上指定的header
    time.sleep(2)
    content = driver.page_source
    print content


def tt1():
    desired_capabilities = DesiredCapabilities.PHANTOMJS.copy()
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Host': 'www.dianping.com',
               }
    for key, value in headers.iteritems():
        desired_capabilities['phantomjs.page.customHeaders.{}'.format(key)] = value
    desired_capabilities[
        'phantomjs.page.customHeaders.User-Agent'] = \
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) ' \
        'AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 ' \
        'Safari/604.1.38'
    ip_port = random.choice(redis_conn1())
    print ip_port
    proxy = Proxy(
        {
            'proxyType': ProxyType.MANUAL,
            'httpProxy': '%s' % ip_port  # 代理ip和端口
        }
    )

    proxy.add_to_capabilities(desired_capabilities)
    print desired_capabilities
    driver = webdriver.PhantomJS(desired_capabilities=desired_capabilities)
    driver.set_page_load_timeout(10)
    driver.get("http://www.dianping.com/shop/%s" % ['76964345', '15855144', ])
    list1 = driver.find_elements_by_xpath('//div[@class="comment-condition J-comment-condition Fix"]/div/span/a')
    for l in list1:
        print l.text
    if '403 Forbidden' in driver.page_source:
        print driver.page_source
    driver.close()




if __name__ == '__main__':
    get_data('111')
    # get_data1()
    # get_data2()
    # dynamic_load('http://www.dianping.com/shop/6232395')
    # for i in xrange(10):
    #     try:
    #         tt1()
    #     except Exception,e:
    #         print e

