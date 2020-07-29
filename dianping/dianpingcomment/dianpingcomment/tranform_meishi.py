# -*- encoding:utf-8 -*-
import sys
import json
import re

reload(sys)
sys.setdefaultencoding('utf-8')


def get_json_hierarchy(_json_obj, arch_ele_list):
    for e in arch_ele_list:
        if e not in _json_obj:
            return None
        _json_obj = _json_obj[e]
    return _json_obj


def format_list(data):
    result = []
    if data:
        for item in data:
            tmp = ''
            if item:
                if type(item) == unicode:
                    tmp = item.encode('utf-8')
                    tmp = tmp.replace('\u0001', '')
                    tmp = tmp.replace('\n', ' ')
                    tmp = tmp.replace('\t', ' ')
                    tmp = tmp.replace('\r', ' ')
                    tmp = tmp.strip()
                elif type(item) == int:
                    tmp = str(item)
                elif type(item) == str:
                    tmp = item.encode('utf-8').replace("\u0001", '')
                    tmp = tmp.replace('\n', ' ')
                    tmp = tmp.replace('\t', ' ')
                    tmp = tmp.replace('\r', ' ')
                    tmp = tmp.decode('utf-8').strip()
                else:
                    tmp = item
            result.append(tmp)
    return result


for line in sys.stdin:
    line = json.loads(line)
    content = line
    result = []
    result.append(get_json_hierarchy(content, ['shop_id']))
    result.append(get_json_hierarchy(content, ['comment_id']))
    result.append(get_json_hierarchy(content, ['user_id']))
    result.append(get_json_hierarchy(content, ['user_name']))
    result.append(get_json_hierarchy(content, ['user_contrib_val']))
    result.append(get_json_hierarchy(content, ['total_score']))
    result.append(get_json_hierarchy(content, ['score1_name']))
    result.append(get_json_hierarchy(content, ['score1']))
    result.append(get_json_hierarchy(content, ['score2_name']))
    result.append(get_json_hierarchy(content, ['score2']))
    result.append(get_json_hierarchy(content, ['score3_name']))
    result.append(get_json_hierarchy(content, ['score3']))
    result.append(get_json_hierarchy(content, ['comment_text']))
    result.append(get_json_hierarchy(content, ['comment_dt']))
    result.append(get_json_hierarchy(content, ['comment_type']))
    result.append(get_json_hierarchy(content, ['dt']))

    print "\t".join(format_list(result))
