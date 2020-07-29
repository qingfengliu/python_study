# -*- coding: utf-8 -*-
import hashlib

import scrapy
import time
from scrapy.selector import Selector
from scrapy import Request
from urlparse import urljoin
import sys
import web, re
from dianpingcomment.items import DianpingcommentItem

reload(sys)
sys.setdefaultencoding('utf-8')
db_insert = web.database(dbn='mysql', db='o2o', user='xxxx', pw='xxxx', port=3306, host='xxxx')

db = web.database(dbn='mysql', db='xxxx', user='xxxx', pw='xxxx', port=3306, host='xxxx')
dt = time.strftime('%Y-%m-%d', time.localtime())

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0',
    'Host': 'Host: www.dianping.com',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    # 'Referer': 'https://www.dianping.com/shop/6124971/review_more?pageno=3',
    'Connection': 'keep-alive',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'max-age=0'

}


class DianpingtuanSpider(scrapy.Spider):
    name = "yayicomments"
    allowed_domains = ["dianping.com"]

    # start_urls = ['http://t.dianping.com/citylist']


    def __md5sum(self, w):
        m = hashlib.md5()
        m.update(w)
        return m.hexdigest()

    def start_requests(self):
        data = db.query(
            'SELECT distinct shop_id FROM t_spider_dianping_mouth;')
        for d in data:
            dianping_id = d.shop_id

            print dianping_id
            url = 'http://www.dianping.com/shop/%s/review_more?pageno=1' % dianping_id
            Cookie = {'_hc.v': '9c20f3b2-274d-8559-c306-1785c4b96ebc.%s;' % (int(time.time())),
                      'JSESSIONID': '%s' % self.__md5sum("%s" % time.time()),
                      's_ViewType': '10',
                      'PHOENIX_ID': '0a0102f1-15c114151d0-436b141'
                      }
            yield Request(url, callback=self.parse, dont_filter=True, headers=header, errback=self.parse_failure,
                          cookies=Cookie)

    def parse(self, response):
        # print response.body
        if '页面不存在' in response.body:
            Cookie = {'_hc.v': '9c20f3b2-274d-8559-c306-1785c4b96ebc.%s;' % (int(time.time())),
                      'JSESSIONID': '%s' % self.__md5sum("%s" % time.time()),
                      's_ViewType': '10',
                      'PHOENIX_ID': '0a0102f1-15c114151d0-436b141'
                      }
            yield Request(response.url, errback=self.parse_failure,
                          callback=self.parse, headers=header, cookies=Cookie, dont_filter=True)
        else:
            sel = Selector(response)
            detail_list = sel.xpath('//div[@class="comment-list"]/ul/li')
            if detail_list:
                for detail in detail_list:
                    item = DianpingcommentItem()
                    comment_id = ''.join(detail.xpath('@data-id').extract())
                    item['comment_id'] = comment_id
                    shop_id = ''.join(re.findall('com/shop/(.*?)/review', response.url))
                    item['shop_id'] = shop_id
                    href = ''.join(
                        detail.xpath('./div[@class="pic"]/p[@class="name"]/a/@href').extract()).strip().replace(
                        '\n', '')
                    name = ''.join(
                        detail.xpath('./div[@class="pic"]/p[@class="name"]/a/text()').extract()).strip().replace(
                        '\n', '')
                    # print href
                    item['user_name'] = name
                    user_id = href.replace('/member/', '').strip().replace('\n', '')
                    item['user_id'] = user_id
                    total_score = ''.join(detail.xpath(
                        './div[@class="content"]/div[@class="user-info"]/span[1]/@class').extract()).strip().replace(
                        '\n',
                        '')
                    if not total_score:
                        total_score = ''.join(detail.xpath(
                            './div[@class="content"]/p[@class="shop-info"]/span[1]/@class').extract()).strip().replace(
                            '\n',
                            '')
                    total_score = total_score.replace('item-rank-rst irr-star', '')
                    if total_score:
                        total_score = int(total_score) / 10
                    item['total_score'] = total_score
                    scores = detail.xpath('./div[@class="content"]/div[@class="user-info"]/div/span/text()').extract()
                    if scores:
                        if len(scores) == 3:
                            score1 = scores[0]
                            score2 = scores[1]
                            score3 = scores[2]
                            score1_name = score1[:-1]
                            score1 = score1[-1:]
                            item['score1_name'] = score1_name
                            item['score1'] = score1

                            score2_name = score2[:-1]
                            score2 = score2[-1:]
                            item['score2_name'] = score2_name
                            item['score2'] = score2

                            score3_name = score3[:-1]
                            score3 = score3[-1:]
                            item['score3_name'] = score3_name
                            item['score3'] = score3
                    comment_txt = ''.join(detail.xpath(
                        './div[@class="content"]/div[@class="comment-txt"]/div/text()').extract()).strip().replace('\n',
                                                                                                                   '')
                    item['comment_text'] = comment_txt
                    comment_dt = ''.join(detail.xpath(
                        './div[@class="content"]/div[@class="misc-info"]/span/text()').extract()).strip().replace('\n',
                                                                                                                  '')
                    if comment_dt:
                        comment_dt = comment_dt.split(u'\xa0')
                        if comment_dt:
                            comment_dt = comment_dt[0]

                        if len(comment_dt) == 5:
                            comment_dt = '2017-' + comment_dt
                        elif len(comment_dt) == 8:
                            comment_dt = '20' + comment_dt
                    item['comment_dt'] = comment_dt
                    contribution = ''.join(
                        detail.xpath(
                            './div[@class="pic"]/p[@class="contribution"]/span/@title').extract()).strip().replace(
                        '\n', '')
                    contribution = contribution.replace('贡献值', '').strip()
                    item['user_contrib_val'] = contribution
                    # try:
                    #     db_insert.insert('t_hh_dianping_shop_comments', **item)
                    # except:
                    #     pass
                    yield item
                next_page = sel.xpath('//a[@class="NextPage"]/@href')
                if next_page:
                    next_page = ''.join(next_page.extract())
                    next_page = urljoin(response.url, next_page)
                    print next_page
                    Cookie = {'_hc.v': '9c20f3b2-274d-8559-c306-1785c4b96ebc.%s;' % (int(time.time())),
                              'JSESSIONID': '%s' % self.__md5sum("%s" % time.time()),
                              's_ViewType': '10',
                              'PHOENIX_ID': '0a0102f1-15c114151d0-436b141'
                              }
                    yield Request(next_page, errback=self.parse_failure,
                                  callback=self.parse, headers=header, cookies=Cookie, dont_filter=True, )
            else:
                print response.body

    def parse_failure(self, failure):
        meta = failure.request.meta
        meta['retry_times'] = 0
        error_resion = failure.value
        if 'Connection refused' in str(error_resion) or 'timeout' in str(
                error_resion) or 'Could not open CONNECT tunnel with proxy' in str(error_resion):
            # print type(error_resion)
            url = failure.request.url
            if 'shop' in url:
                Cookie = {'_hc.v': '9c20f3b2-274d-8559-c306-1785c4b96ebc.%s;' % (int(time.time())),
                          'JSESSIONID': '%s' % self.__md5sum("%s" % time.time()),
                          's_ViewType': '10',
                          'PHOENIX_ID': '0a0102f1-15c114151d0-436b141'
                          }
                yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True, meta=meta,
                              headers=header, cookies=Cookie)

        else:

            try:
                error_resion = failure.value.response._body
                if 'aboutBox errorMessage' in error_resion:
                    pass
                else:
                    url = failure.request.url
                    if 'search' in url:
                        yield Request(url, callback=self.parse, errback=self.parse_failure, dont_filter=True, meta=meta,
                                      headers=header)
                    elif 'deal' in url:
                        yield Request(url, callback=self.parse_detail, errback=self.parse_failure, dont_filter=True,
                                      meta=meta, headers=header)

            except Exception, e:
                print e






