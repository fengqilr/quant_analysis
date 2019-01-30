db_pool = {}

from pymongo import MongoClient


class MongodbUtils(object):
    def __init__(self, table, collection, ip, port):
        self.table = table
        self.ip = ip
        self.port = port
        self.collection = collection
        if (ip, port) not in db_pool:
            db_pool[(ip, port)] = self.db_connection()
        self.db = db_pool[(ip, port)]
        self.db_table = self.db_table_connect()

    def __enter__(self):
        return self.db_table

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def db_connection(self):
        db = None
        try:
            db = MongoClient(self.ip, self.port)
        except Exception as e:
            print 'Can not connect mongodb'
            raise e
        return db

    def db_table_connect(self):
        table_db = self.db[self.table][self.collection]
        return table_db