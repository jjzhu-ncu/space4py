__author__ = 'jjzhu'


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
    this_is = WhatFor()
    this_is.it()
    this_is.uncommon()
