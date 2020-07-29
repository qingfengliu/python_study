# coding=utf8

import web

db = web.database(dbn='mysql', db='o2o', user='writer', pw='xxxx', port=3306, host='xxxx')


def update_shop_id():
    sql = "select distinct deal_id from t_hh_dianping_tuangou_deal_info WHERE dt > '2017-08-31' AND shop_id = 0 ;"
    data = db.query(sql)
    for d in data:
        deal_id = d.get('deal_id')
        sql_shop_id = 'select shop_id from t_hh_dianping_tuangou_deal_info where deal_id="%s" and dt="2017-09-08" and shop_id!=0' % deal_id
        data_shop_id = db.query(sql_shop_id)
        ll = ['2017-09-01',
              '2017-09-02',
              '2017-09-03',
              '2017-09-04',
              '2017-09-05',
              '2017-09-06',
              '2017-09-07',
              '2017-09-08']
        for l in ll:
            for d_shop_id in data_shop_id:
                shop_id = d_shop_id.get('shop_id')
                try:
                    sql_update = 'update t_hh_dianping_tuangou_deal_info set shop_id = "%s" where deal_id="%s" and dt="%s"' % (
                    shop_id, deal_id,l)
                    db.query(sql_update)
                    break
                except Exception, e:
                    print e

def exec_update():
    pass

if __name__ == '__main__':
    update_shop_id()
