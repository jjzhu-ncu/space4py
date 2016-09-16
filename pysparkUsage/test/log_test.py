__author__ = 'jjzhu'
__doc__ = u'logging 模块使用'
# -*- coding:utf-8 -*-
import logging
import logging.config


def screen_print():
    # 默认情况下，logging的日志级别是warning
    # 日志级别为CRITICAL>ERROR>WARNING>INFO>DEBUG>NOTSET
    # 所以低于设置的日志级别是不会被打印的

    logging.debug('this is a debug message')  # 不会打印
    logging.info('this is an info message')  # 不会被打印
    logging.warning('this is a warning message')
    logging.error('this is a error message')


def config_log():
    logging.basicConfig(
        level=logging.DEBUG,  # 设置日志级别
        # 设置日志输出格式
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        # 设置时间格式
        datefmt='%Y-%m-%d %H:%M:%S',
        # 设置日志输出文件名
        filename='my.log',
        # 设置文件读写方式
        filemode='w')
    logging.debug('this is a debug message')
    logging.info('this is an info message')
    logging.warning('this is a warning message')
    logging.error('this is a error message')


def screen_and_file():
    logging.basicConfig(
        level=logging.DEBUG,  # 设置日志级别
        # 设置日志输出格式
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        # 设置时间格式
        datefmt='%Y-%m-%d %H:%M:%S',
        # 设置日志输出文件名
        filename='my.log',
        # 设置文件读写方式
        filemode='w')
    # 设置StreamHandler
    console = logging.StreamHandler()
    # 设置输出日志级别
    console.setLevel(logging.DEBUG)
    # 设置相应的格式
    formatter = logging.Formatter(
        fmt='%(asctime)s  %(name)-12s: %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(fmt=formatter)
    logging.getLogger('').addHandler(console)
    logging.debug('this is a debug message')
    logging.info('this is an info message')
    logging.warning('this is a warning message')
    try:
        10/0  # this code will raise ZeroDivisionError exception
    except ZeroDivisionError as e:
        import traceback
        logging.error(traceback.format_exc())
        logging.exception(e)


def rotating_file_handler():
    from logging.handlers import RotatingFileHandler

    rotating_handler = RotatingFileHandler(
        'my.log',  # 设置输出日志文件名
        maxBytes=10*1024*1024,  # 指定每个日志文件的大小（字节），这里是10M
        backupCount=10  # 指定备份的日志文件数
    )
    rotating_handler.setLevel(logging.DEBUG)  # 设置级别
    formatter = logging.Formatter(
        fmt='%(asctime)s  %(name)-12s: %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    rotating_handler.setFormatter(formatter)
    # 可以通过addHandler添加其他的处理方式
    logging.getLogger('').addHandler(rotating_handler)
    logging.debug('this is a debug message')
    logging.info('this is an info message')
    logging.warning('this is a warning message')
    try:
        10/0  # this code will raise ZeroDivisionError exception
    except ZeroDivisionError as e:
        import traceback
        logging.error(traceback.format_exc())
        logging.exception(e)


def logger_conf():
    import platform
    import os
    if platform.system() is 'Windows':
                logging.config.fileConfig(os.path.abspath('../')+'\\conf\\logging.conf')
    elif platform.system() is 'Linux':
                logging.config.fileConfig(os.path.abspath('../')+'/conf/logging.conf')
    logger = logging.getLogger('simpleLogger')
    logger.debug('this is a debug message')
    logger.info('this is an info message')
    logger.warning('this is a warning message')

    try:
        10/0  # this code will raise ZeroDivisionError exception
    except ZeroDivisionError as e:
        # import traceback
        # logger.error(traceback.format_exc())
        logger.exception(e)
if __name__ == '__main__':
    for _ in range(10):
        screen_and_file()
