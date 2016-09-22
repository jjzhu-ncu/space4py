# -*- coding: utf-8 -*-
__author__ = 'jjzhu'


def str(s):
    print 'global str()'


def foo():
    def str(s):
        print 'closure str()'
    str('')


def bar():
    str('')

foo()
bar()
