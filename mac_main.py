from mac_db import MacDatabase, Constable, ConsColumn, ConsKeyCode, ConsRGNCode
from mac_logger import LogManager as logger
from mac_source import *
from config import *


def _find_between_strings(string, start_string, end_string):
    start_index = string.find(start_string)
    if start_index != -1:
        start_index += len(start_string)
        end_index = string.find(end_string, start_index)
        if end_index != -1:
            return string[start_index:end_index]
    return None


def _get_option_id(string):
    start_index = string.find('OPTION:')
    if start_index != -1:
        return int(string[7:])
    return 0


def _make_db_out_path():
    current_time = datetime.now()
    time_str = current_time.strftime("%Y-%m-%d_%H-%M-%S")
    out_path = ''
    try:
        out_path = get_value(CONF_OUT_DIR)
        out_path = out_path + '/' + time_str

        if not os.path.exists(out_path):
            os.makedirs(out_path)

    except Exception as e:
        logger.error(e)
    return out_path


def _process_ip_and_region(db: MacDatabase, login_id, ip, region, timestamp, data):
    table = []
    if data[1] != ip:
        table.append(Constable.WARN_IP)
    if data[2] != region:
        table.append(Constable.WARN_REGION)

    if len(data):
        new_timestamp = data[3]
        if new_timestamp > timestamp + 30 * 60:
            for t in table:
                db.insert_table2(t, {ConsColumn.LOGIN1: data[1], ConsColumn.LOGIN2: login_id,
                                     ConsColumn.WARN: ConsRGNCode.CHANGE_IN_30MIN})
        elif new_timestamp > timestamp + 20 * 60:
            for t in table:
                db.insert_table2(t, {ConsColumn.LOGIN1: data[1], ConsColumn.LOGIN2: login_id,
                                     ConsColumn.WARN: ConsRGNCode.CHANGE_IN_20MIN})
        elif new_timestamp > timestamp + 10 * 60:
            for t in table:
                db.insert_table2(t, {ConsColumn.LOGIN1: data[1], ConsColumn.LOGIN2: login_id,
                                     ConsColumn.WARN: ConsRGNCode.CHANGE_IN_10MIN})
        elif new_timestamp > timestamp + 5 * 60:
            for t in table:
                db.insert_table2(t, {ConsColumn.LOGIN1: data[1], ConsColumn.LOGIN2: login_id,
                                     ConsColumn.WARN: ConsRGNCode.CHANGE_IN_5MIN})


def _parse_one_source_file(db: MacDatabase, f_path: str, param: dict):
    h = open_data_file(f_path)
    line = read_data_line(h)

    while line is not None:
        result = line.split(",")

        time_string = '{} {}'.format(f_path, result[0])
        tm_format = "%Y-%m-%d %H:%M:%S"
        dt = datetime.strptime(time_string, tm_format)
        timestamp = int(dt.timestamp())
        key = _find_between_strings(result[1], 'receive new key ', ' with login')
        ip = result[2]
        region = result[-8]
        code = result[-5]
        mac = result[-4]
        option = _get_option_id(result[-2])
        keys = result[-1].split('-')
        key_out = None
        if len(keys) >= 2:
            key_out = keys[1].replace("\n", "")

        # insert
        mac_id = db.insert_mac(mac, option)
        ip_id = db.insert_ICR(Constable.IP, ip)
        region_id = db.insert_ICR(Constable.REGION, region)
        code_id = db.insert_ICR(Constable.CODE, code)
        login_id = db.insert_login(mac_id, code_id, ip_id, region_id, timestamp, key, key_out)

        if key is None:
            db.insert_table2(Constable.WARN_KEY, {ConsColumn.LOGIN1: login_id, ConsColumn.LOGIN2: login_id,
                                                  ConsColumn.WARN: ConsKeyCode.ERROR_NEW_KEY_EMPTY})
        if key_out is None:
            db.insert_table2(Constable.WARN_KEY, {ConsColumn.LOGIN1: login_id, ConsColumn.LOGIN2: login_id,
                                                  ConsColumn.WARN: ConsKeyCode.WARN_REQ_KEY_EMPTY})

        if param['timestamp'] != 0:
            res = db.query_table2(Constable.LOGIN,
                                  [ConsColumn.ID, ConsColumn.IP, ConsColumn.REGION, ConsColumn.TIME, ConsColumn.KEY_OUT,
                                   ConsColumn.KEY_IN],
                                  {ConsColumn.MAC: mac_id}, 'ORDER BY {} DESC LIMIT 2'.format(ConsColumn.ID))
            if len(res) == 2 and len(res[1]):
                old_data = res[1]
                _process_ip_and_region(db, login_id, ip_id, region_id, timestamp, old_data)

                if key_out:
                    if key_out == old_data[4]:
                        db.insert_table2(Constable.WARN_KEY,
                                         {ConsColumn.LOGIN1: old_data[0], ConsColumn.LOGIN2: login_id,
                                          ConsColumn.WARN: ConsKeyCode.WARN_SAME_LAST})
                    elif key_out != old_data[5]:
                        db.insert_table2(Constable.WARN_KEY,
                                         {ConsColumn.LOGIN1: old_data[0], ConsColumn.LOGIN2: login_id,
                                          ConsColumn.WARN: ConsKeyCode.ERROR_DIFFERENT})

            if timestamp > param['timestamp'] + param['max_interval']:
                db.insert_table2(Constable.WARN_TIME, {ConsColumn.LOGIN1: param['last_login'],
                                                       ConsColumn.LOGIN2: login_id,
                                                       ConsColumn.START: param['timestamp'],
                                                       ConsColumn.END: timestamp,
                                                       ConsColumn.INTERVAL: timestamp - param['timestamp']})
        if code != 'auto_code_':
            res = db.query_table2(Constable.LOGIN,
                                  [ConsColumn.ID, ConsColumn.MAC],
                                  {ConsColumn.CODE: code_id}, 'AND {}!={} LIMIT 1'.format(ConsColumn.MAC, mac_id))
            if len(res) and len(res[0]):
                db.insert_warn_code(code_id, res[0][1], mac_id)

        if option > 2:
            db.insert_warn_option(mac_id)

        param['timestamp'] = timestamp
        param['last_login'] = login_id
        line = read_data_line(h)

    close_data_file(h)


def main():
    logger.debug('START')
    out_path = _make_db_out_path()

    try:
        times = get_value(CONF_TIMES)
        during = get_value(CONF_INTERVAL)
        end_date = get_value(CONF_END_DATA)
        i = 0

        while i < times:
            verify_data_file_exist(end_date, during)
            param = {'timestamp': 0, 'max_interval': get_value(CONF_MAX_TIME_INTER), 'last_login': 0}
            j = during - 1
            start_data = make_new_date_str(end_date, j)
            d = MacDatabase('{}/{}.db'.format(out_path, start_data))
            d.connect()
            while j >= 0:
                start_data = make_new_date_str(end_date, j)
                logger.debug('Parse -- ' + start_data)
                _parse_one_source_file(d, start_data, param)
                j -= 1
            d.disconnect()
            end_date = make_new_date_str(end_date, during)

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    main()
    '''
    d = MacDatabase('test.db')
    d.connect()
    d.test()
    d.disconnect()
    '''
