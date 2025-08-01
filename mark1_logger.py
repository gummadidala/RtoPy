# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 12:02:51 2020

@author: Alan.Toppen
"""
import logging
from logging.config import dictConfig

# Create new log level for SUCCESS
SUCCESS_LEVEL_NUM = 1
logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")
def successv(self, message, *args, **kws):
    if self.isEnabledFor(SUCCESS_LEVEL_NUM):
        # Yes, logger takes its '*args' as 'args'.
        self._log(SUCCESS_LEVEL_NUM, message, args, **kws)
logging.Logger.success = successv


def mark1_logger(filename):
    logger_format = '|'.join([
        '%(asctime)s',
        '%(levelname)s',
        '%(pathname)s',
        '%(module)s',
        'line %(lineno)d',
        '%(message)s'])

    logging_config = dict(
        version = 1,
        formatters = {
            'formatter1': {
                'format': logger_format,
                'datefmt': '%F %X'
            }
        },
        handlers = {
            'stream_handler': {
                'class': 'logging.StreamHandler',
                'formatter': 'formatter1',
                'level': logging.NOTSET
            },
            'file_handler': {
                'class': 'logging.FileHandler',
                'filename': filename,
                'formatter': 'formatter1',
                'level': logging.NOTSET
            }
        },
        root = {
            'handlers': [
                'stream_handler',
                'file_handler'
            ],
            'level': logging.NOTSET,
        },
    )
    dictConfig(logging_config)

    return logging.getLogger()

#m1l = mark1_logger('test.log')
#m1l.debug('often makes a very good meal of %s', 'visiting tourists')

