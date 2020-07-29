# coding=utf8

import urlparse

url = 'https://www.dianping.com/member/1111'
str_ = '/shop/121'
url = urlparse.urljoin(url,str_)
print url