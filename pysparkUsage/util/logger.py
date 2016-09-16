__author__ = 'jjzhu'
import logging
import logging.config
from logging.handlers import RotatingFileHandler
import os
import platform
# -*- coding:utf-8 -*-


class Singleton(object):
    logger = None

    def __new__(cls, *args, **kwargs):
        rotating_handler = RotatingFileHandler(
            '../log/my.log',  # 设置输出日志文件名
            maxBytes=100*1024*1024,  # 指定每个日志文件的大小（字节），这里是10M
            backupCount=10  # 指定备份的日志文件数
        )
        rotating_handler.setLevel(logging.DEBUG)  # 设置级别
        formatter = logging.Formatter(
            fmt='%(asctime)s  %(name)-12s: %(levelname)-8s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        rotating_handler.setFormatter(formatter)

        # 设置StreamHandler
        console = logging.StreamHandler()
        # 设置输出日志级别
        console.setLevel(logging.DEBUG)
        console.setFormatter(fmt=formatter)

        # 可以通过addHandler添加其他的处理方式
        logger = logging.getLogger('')

        logger.addHandler(rotating_handler)
        logger.addHandler(console)

        logger.setLevel(logging.DEBUG)
        return logger
    #
    # @staticmethod
    # def get_instance():
    #
    #     rotating_handler = RotatingFileHandler(
    #         '../log/my.log',  # 设置输出日志文件名
    #         maxBytes=10*1024*1024,  # 指定每个日志文件的大小（字节），这里是10M
    #         backupCount=10  # 指定备份的日志文件数
    #     )
    #     rotating_handler.setLevel(logging.DEBUG)  # 设置级别
    #     formatter = logging.Formatter(
    #         fmt='%(asctime)s  %(name)-12s: %(levelname)-8s %(message)s',
    #         datefmt='%Y-%m-%d %H:%M:%S'
    #     )
    #     rotating_handler.setFormatter(formatter)
    #
    #     # 设置StreamHandler
    #     console = logging.StreamHandler()
    #     # 设置输出日志级别
    #     console.setLevel(logging.DEBUG)
    #     console.setFormatter(fmt=formatter)
    #
    #     # 可以通过addHandler添加其他的处理方式
    #     logger = logging.getLogger('')
    #
    #     logger.addHandler(rotating_handler)
    #     logger.addHandler(console)
    #
    #     logger.setLevel(logging.DEBUG)
    #     return logger


class Logger(Singleton):
    pass


if __name__ == '__main__':
    logger1 = Logger()
    logger2 = Logger()
    logger1.info('this is logger1')
    logger1.error('errorrrrrrrrrrrrrrrrr')
    # logger2.get_logger().info('this is logger2')
    # print(id(logger1) == id(logger2))
