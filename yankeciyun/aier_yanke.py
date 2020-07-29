# -*- coding: utf-8 -*-
from collections import Counter
import web
import re
from operator import itemgetter
import jieba
import jieba.posseg as pseg
import datetime
import time


db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')
# db2 = web.database(dbn='mysql', db='', user='writer', pw='xxxx', port=13306, host='127.0.0.1')
brand_list = ['爱尔眼科', '普瑞', '新视界', '光明', '何氏', '林顺潮', '华厦', '同仁', '东南', '爱瑞阳光', '尖峰眼科']
# results_brand = db.query("SELECT a.* FROM (SELECT DISTINCT brand FROM o2o.t_hh_meituan_shop_info_tmp_copy WHERE (shop_name LIKE '%视光%' OR "
#                         "shop_name LIKE '%眼科%') AND shop_id IN (SELECT shop_id FROM o2o.t_hh_meituan_shop_comments_tmp)) AS a limit 10")
# for k in results_brand:
#     brand_list.append(str(k.brand))
jieba.load_userdict("word.txt")
# jieba.enable_parallel(4)
allow_speech_tags = ['a', 'ad', 'i', 'm', 'n', 'q', 'v', 'vd', 'vn', 'an', 'u', 'z', 't', 'r', 'p', 'f']


# kw_list2 = []
# 美团评论
def meituan():
    for brand in brand_list:
        kw_list = []
        sql = "SELECT DISTINCT shop_id, user_name, comment_text,comment_dt FROM " \
              ".t_hh_meituan_shop_comments_yanke where  shop_id IN (SELECT shop_id FROM o2o.t_hh_meituan_shop_info_tmp WHERE shop_name like '%{}%' )".format(brand)

        results = db.query(sql)
        for result in results:
            sentences = re.split("，|？|！|。|\s|~|、|,|&hellip;|\.|～|…| ", str(result.comment_text))
            if len(sentences) == 1 and len(sentences[0]) > 90:
                print(sentences[0])
                continue
            for sentence in sentences:
                phrase = ''
                word_is_one = []
                word_flag = []
                words = pseg.cut(sentence)
                for word in words:
                    if word.flag in allow_speech_tags or word.word == '没' or word.word == '不':
                        phrase = phrase + word.word
                        word_is_one.append(word)
                        word_flag.append(word.flag)

                if (len(word_is_one) == 1 and word_is_one[0].flag in ['v', 'n']) or 'n' not in word_flag:
                    continue
                elif phrase == '' or phrase == '用户没有填写评论' or phrase == '.':
                    continue
                else:
                    kw_list.append(phrase)
        if len(kw_list) == 0:
            continue
        word_count = Counter(kw_list)
        swd = sorted(word_count.items(), key=itemgetter(1), reverse=True)
        swd = swd[0:30]
        for k in swd:
            key_dict = {}
            key_dict['brand'] = brand
            key_dict['seg_time'] = datetime.datetime.now().strftime('%Y-%m') + '-01'
            key_dict['keyword'] = k[0]
            key_dict['count_num'] = k[1]
            key_dict['source_'] = '美团'
            key_dict['other1'] = ''
            key_dict['other2'] = ''
            key_dict['other3'] = ''
            key_dict['dt'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            db.insert('t_hh_yanke_keywords', **key_dict)

# 点评评论
def dianping():
    for brand in brand_list:
        kw_list = []
        sql = "SELECT DISTINCT comment_id, comment_text,comment_dt FROM " \
                  "o2o.t_hh_dianping_shop_comments_yanke where  " \
                   "shop_id IN (SELECT shop_id FROM o2o.t_spider_m_dianping_shop WHERE name like '%{}%' AND categoryId='181')".format(brand)
        results = db.query(sql)
        for result in results:
            # print result.comment_text
            sentences = re.split("，|？|！|。|\s|~|、|,|&hellip;|\.|～|…| ", str(result.comment_text))
            # print sentences
            # raw_input('enter')
            if len(sentences) == 1 and len(sentences[0]) > 90:
                # print sentences[0]
                continue
            for sentence in sentences:
                phrase = ''
                word_is_one = []
                word_flag = []
                words = pseg.cut(sentence)
                for word in words:
                    if word.flag in allow_speech_tags or word.word == '没' or word.word == '不':
                        phrase = phrase + word.word
                        word_is_one.append(word)
                        word_flag.append(word.flag)
                # print word_flag
                if (len(word_is_one) == 1 and word_is_one[0].flag in ['v', 'n']) or 'n' not in word_flag:
                    continue
                elif phrase == '' or phrase == '用户没有填写评论' or phrase == '.':
                    continue
                else:
                    kw_list.append(phrase)
        if len(kw_list) == 0:
            continue
        word_count = Counter(kw_list)
        swd = sorted(word_count.items(), key=itemgetter(1), reverse=True)
        swd = swd[0:30]
        for k in swd:
            key_dict = {}
            key_dict['brand'] = brand
            key_dict['seg_time'] = datetime.datetime.now().strftime('%Y-%m') + '-01'
            key_dict['keyword'] = k[0]
            key_dict['count_num'] = k[1]
            key_dict['source_'] = '点评'
            key_dict['other1'] = ''
            key_dict['other2'] = ''
            key_dict['other3'] = ''
            key_dict['dt'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            db.insert('t_hh_yanke_keywords', **key_dict)


meituan()
dianping()