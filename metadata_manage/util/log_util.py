# -*- coding:utf-8 -*-
#log操作相关的工具类

import logging
from logging.handlers import *
from config_util import ConfigUtil
import os

class Logger(object):

    def __init__(self,name,env='test'):
        log_format = "%(asctime)s %(filename)s[line:%(lineno)s] %(levelname)s %(message)s"
        formatter = logging.Formatter(log_format)
        config = ConfigUtil(env)
        log_dir = config.get_property_value("log.dir")
        log_level = self.logger_level(config.get_property_value("log.level"))
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir,"bi_monitor.log")
        log_file_handler = TimedRotatingFileHandler(filename=log_file,when="midnight",backupCount=10)
        log_file_handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logger.addHandler(log_file_handler)
        self.logger = logger

    def get_logger(self):
        return self.logger

    def logger_level(self,level='info'):
        if level == 'debug':
            return logging.DEBUG
        elif level == 'info':
            return logging.INFO
        elif level == 'warn':
            return logging.WARN
        elif level == 'error':
            return logging.ERROR
        else:
            return logging.INFO


if __name__ == '__main__':
    logger = Logger(name="test").get_logger()
    logger.info("test")
