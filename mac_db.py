import os
import sqlite3
import collections
from mac_logger import LogManager as logger


class ConsKeyCode:
    WARN_SAME_LAST = 1
    WARN_REQ_KEY_EMPTY = 2
    ERROR_DIFFERENT = 3
    ERROR_NEW_KEY_EMPTY = 4


class ConsRGNCode:
    CHANGE_IN_5MIN = 1
    CHANGE_IN_10MIN = 2
    CHANGE_IN_20MIN = 3
    CHANGE_IN_30MIN = 4


class Constable:
    LOGIN = 'login'
    MAC = 'mac'
    IP = 'ip'
    CODE = 'code'
    REGION = 'region'
    WARN_CODE = 'warn_code'
    WARN_TIME = 'warn_time'
    WARN_IP = 'warn_ip'
    WARN_REGION = 'warn_region'
    WARN_KEY = 'warn_key'
    WARN_OPTION = 'warn_option'


class ConsColumn:
    ID = 'id'
    MAC = Constable.MAC
    CODE = Constable.CODE
    IP = Constable.IP
    REGION = Constable.REGION
    TIME = 'time'
    KEY_IN = 'key_in'
    KEY_OUT = 'key_out'
    OPTION_ID = 'option_id'

    WARN = 'warn'
    LOGIN1 = 'login1'
    LOGIN2 = 'login2'
    START = 'start'
    END = 'end'
    INTERVAL = 'interval'
    MAC1 = 'mac1'
    MAC2 = 'mac2'


class MacDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = None
        self.cursor = None
        self.optimize_mac_query = True
        self.optimize_ip_query = True
        self.optimize_region_query = True
        self.optimize_code_query = True

        logger.debug(db_file)
        if os.path.exists(db_file):
            os.remove(db_file)
        if self.optimize_mac_query:
            self.o_mac = collections.defaultdict(int)
        if self.optimize_ip_query:
            self.o_ip = collections.defaultdict(int)
        if self.optimize_region_query:
            self.o_region = collections.defaultdict(int)
        if self.optimize_code_query:
            self.o_code = collections.defaultdict(int)

    def connect(self):
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

        # create tables

        # create login
        sql = '''CREATE TABLE {} (
            {}      INTEGER PRIMARY KEY AUTOINCREMENT
                            UNIQUE
                            NOT NULL,
            {}      INTEGER NOT NULL,
            {}      INTEGER NOT NULL,
            {}      INTEGER NOT NULL,
            {}      INTEGER NOT NULL,
            {}      INTEGER NOT NULL,
            {}      TEXT,
            {}      TEXT
        )'''.format(Constable.LOGIN, ConsColumn.ID, ConsColumn.MAC, ConsColumn.CODE, ConsColumn.IP, ConsColumn.REGION,
                    ConsColumn.TIME,
                    ConsColumn.KEY_IN, ConsColumn.KEY_OUT)
        self.cursor.execute(sql)
        self.connection.commit()

        # create mac
        sql = '''CREATE TABLE {} (
            {}      INTEGER PRIMARY KEY AUTOINCREMENT
                            UNIQUE
                            NOT NULL,
            {}      TEXT    NOT NULL,
            {}      INTEGER NOT NULL
        )'''.format(Constable.MAC, ConsColumn.ID, ConsColumn.MAC, ConsColumn.OPTION_ID)
        self.cursor.execute(sql)
        self.connection.commit()

        # create ip
        sql = '''CREATE TABLE {} (
            {} INTEGER PRIMARY KEY AUTOINCREMENT
                       UNIQUE
                       NOT NULL,
            {} TEXT    NOT NULL
        )'''.format(Constable.IP, ConsColumn.ID, ConsColumn.IP)
        self.cursor.execute(sql)
        self.connection.commit()

        # create code
        sql = '''CREATE TABLE {} (
            {} INTEGER PRIMARY KEY AUTOINCREMENT
                       UNIQUE
                       NOT NULL,
            {} TEXT    NOT NULL
        )'''.format(Constable.CODE, ConsColumn.ID, ConsColumn.CODE)
        self.cursor.execute(sql)
        self.connection.commit()

        # create region
        sql = '''CREATE TABLE {} (
            {} INTEGER PRIMARY KEY AUTOINCREMENT
                       UNIQUE
                       NOT NULL,
            {} TEXT    NOT NULL
        )'''.format(Constable.REGION, ConsColumn.ID, ConsColumn.REGION)
        self.cursor.execute(sql)
        self.connection.commit()

        # create warn_code
        sql = '''CREATE TABLE {} (
            {} INTEGER NOT NULL,
            {} INTEGER NOT NULL,
            {} INTEGER NOT NULL
        )'''.format(Constable.WARN_CODE, ConsColumn.CODE, ConsColumn.MAC1, ConsColumn.MAC2)
        self.cursor.execute(sql)
        self.connection.commit()

        # create warn_time
        sql = '''CREATE TABLE {} (
            {} INTEGER NOT NULL,
            {} INTEGER NOT NULL,
            {} INTEGER NOT NULL,
            {} INTEGER NOT NULL,
            {} INTEGER NOT NULL
        )'''.format(Constable.WARN_TIME, ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.START, ConsColumn.END, ConsColumn.INTERVAL)
        self.cursor.execute(sql)
        self.connection.commit()

        # create warn_ip
        sql = '''CREATE TABLE {} (
            {}  INTEGER NOT NULL,
            {}  INTEGER NOT NULL,
            {}  INTEGER NOT NULL
        )'''.format(Constable.WARN_IP, ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.WARN)
        self.cursor.execute(sql)
        self.connection.commit()

        # create warn_region
        sql = '''CREATE TABLE {} (
            {}  INTEGER NOT NULL,
            {}  INTEGER NOT NULL,
            {}  INTEGER NOT NULL
        )'''.format(Constable.WARN_REGION, ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.WARN)
        self.cursor.execute(sql)
        self.connection.commit()

        # create warn_key
        sql = '''CREATE TABLE {} (
            {}  INTEGER NOT NULL,
            {}  INTEGER NOT NULL,
            {}  INTEGER NOT NULL
        )'''.format(Constable.WARN_KEY, ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.WARN)
        self.cursor.execute(sql)
        self.connection.commit()

        # create warn_option
        sql = '''CREATE TABLE {} (
            {}  INTEGER NOT NULL
        )'''.format(Constable.WARN_OPTION, ConsColumn.MAC)
        self.cursor.execute(sql)
        self.connection.commit()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def query_table2(self, table: str, args: list, kwargs: dict, condition=None):
        sql_from = ' FROM {} '.format(table)
        sql_targets = 'SELECT '
        sql_where = ''

        if len(args) == 0:
            sql_targets += '*'
        else:
            # ','.join([i for i in t])
            sql_targets += ','.join(args)

        if len(kwargs):
            sql_where = 'WHERE'
            i = 0
            for k, v in kwargs.items():
                if i != 0:
                    if isinstance(v, int):
                        sql_where += ' AND ' + k + '=' + str(v)
                    else:
                        sql_where += ' AND ' + k + '="' + v + '"'
                else:
                    # sql_where += ' ' + k + '="' + v + '"'
                    sql_where += ' {}="{}"'.format(k, v)
                i += 1
        sql = sql_targets + sql_from + sql_where

        if condition is not None:
            sql += ' ' + condition

        # 执行SQL语句,传入条件参数
        self.cursor.execute(sql)
        # 'SELECT name, age FROM user WHERE id=?;'

        # 获取查询结果
        return self.cursor.fetchall()

    def query_table(self, table: str, *args, **kwargs):
        return self.query_table2(table, args, kwargs)

    def insert_table2(self, table: str, kwargs: dict):
        # sql = "INSERT INTO users(name, age) VALUES(?, ?)"
        values = []
        str_con = '('
        str_val = '('
        i = 0
        for k, v in kwargs.items():
            if i == 0:
                str_con += k
                str_val += '?'
            else:
                str_con += ', ' + k
                str_val += ', ?'
            values.append(v)
            i += 1

        sql = "INSERT INTO {}{}) VALUES{})".format(table, str_con, str_val)
        self.cursor.execute(sql, values)
        self.connection.commit()

        return self.cursor.lastrowid

    def insert_table(self, table: str, **kwargs):
        return self.insert_table2(table, kwargs)

    def update_table(self, table: str, condition: dict, update: dict):
        # sql = "UPDATE users SET age=? WHERE id=?"
        values = []
        str_con = ''
        str_val = ''
        i = 0
        for k, v in condition.items():
            if i == 0:
                # str_con += k + '="' + v + '"'
                str_con += '{}="{}"'.format(k, v)
            else:
                # str_con += ' AND ' + k + '="' + v + '"'
                str_con += ' AND {}="{}"'.format(k, v)
            i += 1

        i = 0
        for k, v in update.items():
            if i == 0:
                # str_val += k + '="' + v + '"'
                str_val += '{}="{}"'.format(k, v)
            else:
                # str_val += ', ' + k + '="' + v + '"'
                str_val += ', {}="{}"'.format(k, v)
            i += 1

        sql = "UPDATE {} SET {} WHERE {}".format(table, str_val, str_con)
        self.cursor.execute(sql, values)
        self.connection.commit()

    def insert_mac(self, mac, optionId):
        if self.optimize_mac_query:
            key = '{}O{}'.format(mac, optionId)
            idx = self.o_mac[key]
            if idx == 0:
                idx = self.insert_table2(Constable.MAC, {ConsColumn.MAC: mac, ConsColumn.OPTION_ID: optionId})
                self.o_mac[key] = idx
            return idx
        else:
            records = self.query_table2(Constable.MAC, [ConsColumn.ID],
                                        {ConsColumn.MAC: mac, ConsColumn.OPTION_ID: optionId})
            if len(records) == 0 or len(records[0]) == 0:
                return self.insert_table2(Constable.MAC, {ConsColumn.MAC: mac, ConsColumn.OPTION_ID: optionId})
            else:
                return records[0][0]

    def insert_ICR(self, table, data):
        column = ''
        o_dict = None
        if table == Constable.IP:
            column = ConsColumn.IP
            if self.optimize_ip_query:
                o_dict = self.o_ip
        elif table == Constable.CODE:
            column = ConsColumn.CODE
            if self.optimize_code_query:
                o_dict = self.o_code
        elif table == Constable.REGION:
            column = ConsColumn.REGION
            if self.optimize_region_query:
                o_dict = self.o_region
        else:
            raise ValueError('Not support table {}'.format(table))
        if o_dict is not None:
            idx = o_dict[data]
            if idx == 0:
                idx = self.insert_table2(table, {column: data})
                o_dict[data] = idx
            return idx
        else:
            records = self.query_table2(table, [ConsColumn.ID], {column: data})
            if len(records) == 0 or len(records[0]) == 0:
                return self.insert_table2(table, {column: data})
            else:
                return records[0][0]

    def insert_login(self, mac, code, ip, region, time, key_in=None, key_out=None):
        data = {ConsColumn.MAC: mac,
                ConsColumn.CODE: code,
                ConsColumn.IP: ip,
                ConsColumn.REGION: region,
                ConsColumn.TIME: time}
        if key_in is not None:
            data[ConsColumn.KEY_IN] = key_in
        if key_out is not None:
            data[ConsColumn.KEY_OUT] = key_out
        return self.insert_table2(Constable.LOGIN, data)

    def insert_warn_code(self, code, mac1, mac2):
        data = {ConsColumn.CODE: code,
                ConsColumn.MAC1: mac1,
                ConsColumn.MAC2: mac2}
        records = self.query_table2(Constable.WARN_CODE, [ConsColumn.CODE], data)
        if len(records) == 0 or len(records[0]) == 0:
            data1 = {ConsColumn.CODE: code,
                     ConsColumn.MAC1: mac2,
                     ConsColumn.MAC2: mac1}
            records = self.query_table2(Constable.WARN_CODE, [ConsColumn.CODE], data1)
            if len(records) == 0 or len(records[0]) == 0:
                self.insert_table2(Constable.WARN_CODE, data)

    def insert_warn_option(self, mac):
        records = self.query_table2(Constable.WARN_OPTION, [ConsColumn.MAC], {ConsColumn.MAC: mac})
        if len(records) == 0 or len(records[0]) == 0:
            return self.insert_table2(Constable.WARN_OPTION, {ConsColumn.MAC: mac})

    def test(self):
        print(self.insert_table(Constable.LOGIN, mac=1, code=2, ip=3, region=1, time=12345, key_in="xsdf",
                                key_out="sdf"))
        print(self.insert_table(Constable.LOGIN, mac=1, code=2, ip=3, region=1, time=12345, key_in="qwewr",
                                key_out="1234556"))
        print(self.insert_table(Constable.LOGIN, mac=2, code=2, ip=3, region=1, time=12345, key_in="qwewr",
                                key_out="1234556"))
        print(self.query_table(Constable.LOGIN, 'code', 'mac', id=2))
        self.update_table(Constable.LOGIN, {'id': 2}, {'code': 3})
        print(self.query_table(Constable.LOGIN, 'code', 'mac', id=2))
        print(self.insert_table(Constable.LOGIN, mac1=2, code=2, ip=3, region=1, time=12345, key_in="qwewr",
                                key_out="1234556"))
