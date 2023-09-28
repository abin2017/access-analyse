import logging
import sys
import config


class LogManager:
    LOG_FILE = "log.txt"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

    @classmethod
    def get_logger(cls):
        if not hasattr(cls, '_logger'):
            cls.configure_logger()
        return cls._logger

    @staticmethod
    def configure_logger():
        level = config.get_value(config.CONF_LOG_LEVEL)
        i_level = logging.DEBUG

        if level == "warn":
            i_level = logging.WARN
        elif level == "error":
            i_level = logging.ERROR
        elif level == "info":
            i_level = logging.INFO
        logging.basicConfig(filename=LogManager.LOG_FILE, format=LogManager.LOG_FORMAT, level=i_level)

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
