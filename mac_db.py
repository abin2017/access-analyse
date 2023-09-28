import sqlite3
from mac_logger import LogManager as logger


class MacDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = None
        self.cursor = None
        logger.debug(db_file)

    def connect(self):
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def create_table(self, table_name, columns):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_data(self, table_name, data):
        placeholders = ", ".join(["?" for _ in data])
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        self.cursor.execute(query, tuple(data))
        self.connection.commit()

    def query_data(self, table_name, condition=""):
        query = f"SELECT * FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def create_table_interface(self):
        table_name = input("Enter table name: ")
        columns = input("Enter columns definition: ")
        self.create_table(table_name, columns)
        print(f"Table '{table_name}' created successfully.")
