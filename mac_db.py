import os
import sqlite3
from mac_logger import LogManager as logger


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
        logger.debug(db_file)
        if os.path.exists(db_file):
            os.remove(db_file)

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
            {}      TEXT    NOT NULL
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
            {} INTEGER NOT NULL
        )'''.format(Constable.WARN_TIME, ConsColumn.START, ConsColumn.END, ConsColumn.INTERVAL)
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

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def query_table(self, table: str, *args, **kwargs):
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

        # 执行SQL语句,传入条件参数
        self.cursor.execute(sql)
        # 'SELECT name, age FROM user WHERE id=?;'

        # 获取查询结果
        return self.cursor.fetchall()

    def insert_table(self, table: str, **kwargs):
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
