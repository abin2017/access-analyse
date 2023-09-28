from mac_db import MacDatabase
from mac_logger import LogManager as logger
from mac_source import *
from config import *


def _make_db_out_path():
    current_time = datetime.now()
    time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
    out_path = ''
    try:
        out_path = get_value(CONF_OUT_DIR)
        out_path = out_path + '/' + time_str

        if not os.path.exists(out_path):
            os.makedirs(out_path)

    except Exception as e:
        logger.error(e)
    return out_path


def _parse_one_source_file(db: MacDatabase, f_path: str):
    h = open_data_file(f_path)
    line = read_data_line(h)

    while line is not None:
        # todo
        line = read_data_line(h)

    close_data_file(h)


def main():
    out_path = _make_db_out_path()

    try:
        times = get_value(CONF_TIMES)
        during = get_value(CONF_INTERVAL)
        end_date = get_value(CONF_END_DATA)
        i = 0

        while i < times:
            verify_data_file_exist(end_date, during)

            j = during - 1
            start_data = make_new_date_str(end_date, j)
            d = MacDatabase('{}/{}.db'.format(out_path, start_data))

            while j >= 0:
                start_data = make_new_date_str(end_date, j)
                _parse_one_source_file(d, start_data)
                j -= 1

            end_date = make_new_date_str(end_date, during)

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    # main()
    d = MacDatabase('test.db')
    d.connect()
    d.test()
    d.disconnect()
