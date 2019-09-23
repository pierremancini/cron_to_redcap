#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import logging.config


def set_root_logger(path_to_log, script_name):
    """
        Set root logger and configure its formatters and handlers.
    """

    config_logging = {
        "root": {
            "level": "DEBUG",
            "handlers": ["console", "file_handler"]
        },
        "version": 1,
        "formatters": {
            "simple": {
                "format": "%(name)s :: %(asctime)s :: %(levelname)s :: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "simple",
                "stream": "ext://sys.stdout"
            },
            "file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "filename": path_to_log + os.path.splitext(script_name)[0] + '.log',
                "formatter": "simple",
                "maxBytes": 10485760,
                "backupCount": 20,
                "encoding": "utf8"
            }
        }
    }

    logging.config.dictConfig(config_logging)

    logger = logging.getLogger()

    return logger
