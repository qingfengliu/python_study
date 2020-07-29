# coding=utf8
import redis


def del_hash():
    redis_ = redis.Redis(host='10.15.1.11', port=6379)
    redis_.delete('dianping:pet_hospital_hash')
del_hash()