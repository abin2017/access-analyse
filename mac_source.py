import os
from datetime import datetime, timedelta
from config import *
from mac_logger import LogManager as logger

SRC_DATA_SUFFIX = "-access.log"


def make_new_date_str(date_str, days):
    if days == 0:
        return date_str
    # 将日期字符串转换为日期对象
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # 计算新的日期
    new_date = date_obj - timedelta(days=days)

    # 将新的日期转换为字符串
    new_date_str = new_date.strftime("%Y-%m-%d")

    return new_date_str


def verify_data_file_exist(end_date, during):
    days = 0
    while days < during:
        f = get_value(CONF_DATA_DIR) + '/' + make_new_date_str(end_date, days) + SRC_DATA_SUFFIX
        if not os.path.exists(f):
            err_str = '{} source data not exist'.format(f)
            logger.error()
            raise ValueError(err_str)
        days += 1


def open_data_file(s_date):
    source_dir = get_value(CONF_DATA_DIR)
    f_path = source_dir + '/' + s_date + SRC_DATA_SUFFIX
    file = open(f_path)
    return file


def close_data_file(handle):
    handle.close()


def read_data_line(handle):
    line = handle.readline()
    if not line:
        return None
    return line
