import json

CONF_DATA_DIR = "data_dir"
CONF_OUT_DIR = "out_dir"
CONF_END_DATA = "end_date"
CONF_INTERVAL = "interval"
CONF_TIMES = "times"
CONF_LOG_LEVEL = "log_level"
CONF_LOG_DIR = "log_dir"
CONF_MAX_TIME_INTER = "time_max_interval"
CONF_DB_REPORT_DIR = 'database_dir'

CONFIG_FILE = "config.json"
DEFAULT_VALUE = None

config = {}


def parse(file_path):
    global config
    with open(file_path) as f:
        config = json.load(f)


def get_value(key):
    if not config:
        parse("config.json")
    return config.get(key)
