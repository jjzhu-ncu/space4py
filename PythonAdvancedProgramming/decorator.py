__author__ = 'jjzhu'
# -*- coding:utf-8 -*-
import time
import hashlib
import pickle
from itertools import chain
cache = {}


def is_obsolete(entry, duration):
    return time.time() - entry['time'] > duration


def compute_key(function, args, kw):
    # dumps(obj, protocol=None)
    # dumps方法将obj序列化，并将结果以字符串的形式返回
    print(type(function))
    key = pickle.dumps((function, args, kw))
    # 计算hash值
    return hashlib.sha1(key).hexdigest()


def momoize(duration=10):
    def _memoize(function):
        def __memoize(*args, **kw):
            key = compute_key(function, args, kw)
            if key in cache and not is_obsolete(cache[key], duration):
                print(u'we got a winner')
                return cache[key]['value']
            result = function(*args, **kw)
            cache[key] = {'value': result,
                          'time': time.time()}
            return result
        return __memoize()
    return _memoize


# @momoize()
def very_very_very_complex_stuff(a, b):
    return a+b


class WhatFor(object):
    @classmethod
    def it(cls):
        print('work with %s' % cls)

    @staticmethod
    def uncommon():
        print('I could be a global function')


def my_decorator(arg1, arg2):
    def _my_decorator(function):
        def __my_decorator(*args, **kw):
            res = function(*args, **kw)
            return res
        return __my_decorator
    return _my_decorator


if __name__ == '__main__':
    # this_is = WhatFor()
    # this_is.it()
    # this_is.uncommon()
    # very_very_very_complex_stuff(4, 4)
    print(type(compute_key))
    print(type(very_very_very_complex_stuff))
    # print(very_very_very_complex_stuff.__name__)
    time.sleep(5)
    # very_very_very_complex_stuff(4, 4)
