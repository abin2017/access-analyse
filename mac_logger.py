import logging
import os
import sys
from datetime import datetime

import config


class LogManager:
    # LOG_FORMAT = '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(funcName)s() - %(message)s'
    LOG_FORMAT = '%(asctime)s [%(levelname)s] - %(message)s'

    @classmethod
    def get_logger(cls):
        if not hasattr(cls, '_logger'):
            cls.configure_logger()
        return cls._logger

    @staticmethod
    def configure_logger():
        level = config.get_value(config.CONF_LOG_LEVEL)
        i_level = logging.DEBUG
        log_file = './log.txt'

        try:
            if level == "warn":
                i_level = logging.WARN
            elif level == "error":
                i_level = logging.ERROR
            elif level == "info":
                i_level = logging.INFO

            log_dir = config.get_value(config.CONF_LOG_DIR)
            if log_dir:
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                current_time = datetime.now()
                time_str = current_time.strftime("%Y-%m-%d_%H-%M-%S")
                log_file = '{}/{}.txt'.format(log_dir, time_str)
        except Exception as e:
            print(e)

        if os.path.exists(log_file):
            os.remove(log_file)

        logging.basicConfig(filename=log_file, format=LogManager.LOG_FORMAT, level=i_level)

        # 创建一个 StreamHandler，用于将日志同时输出到标准输出
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter(LogManager.LOG_FORMAT))

        logger = logging.getLogger()
        logger.addHandler(stream_handler)

        LogManager._logger = logger

    @staticmethod
    def info(message):
        LogManager.get_logger().info(message)

    @staticmethod
    def warn(message):
        LogManager.get_logger().warning(message)

    @staticmethod
    def error(message):
        LogManager.get_logger().error(message)

    @staticmethod
    def debug(message):
        LogManager.get_logger().debug(message)
