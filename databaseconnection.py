# from sqlalchemy import create_engine
import psycopg2
import io


class DatabaseConnection:
    dbc = {"user": "tmpuser", "password": "mypassword", "host": "localhost", "port": "5432", "database": "stockprice"}

    def __init__(self):
        self._conn = psycopg2.connect(**self.dbc)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


with DatabaseConnection() as db:
    table_name = 'financial_statement_quarterly'
    postgreSQL_select_Query = "select * from %s" % (table_name)
    db.execute(postgreSQL_select_Query)
    db.fetchall()
    print("success")
