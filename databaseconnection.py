#from sqlalchemy import create_engine
import psycopg2
import io

# table_name = 'financial_statement_quarterly'
# try:
#   connection = psycopg2.connect(user="postgres",
#                                 password="11!Tosel",
#                                 host="localhost",
#                                 port="5432",
#                                 database="stockprice")
#
#   cursor = connection.cursor()
#   postgreSQL_select_Query = "select * from %s" %(table_name)
#   cursor.execute(postgreSQL_select_Query)
#   records = cursor.fetchall()
#   #df = pd.DataFrame(data=records,columns=['Date', 'High', 'Low', 'Open', 'Close','Volume','Adj_Close'])
# except (Exception, psycopg2.Error) as error :
#   print ("Error while fetching data from PostgreSQL", error)
# finally:
#   #closing database connection.
#   if(connection):
#     cursor.close()
#     connection.close()
#     print("PostgreSQL connection is closed")


class DatabaseConnection:
    dbc = {"user":"tmpuser", "password":"mypassword", "host":"localhost", "port":"5432", "database":"stockprice"}

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
#    db.execute('INSERT INTO comments (username, comment_body, date_posted) VALUES (?, ?, current_date)', ('tom', 'this is a comment'))
#    comments = db.query('SELECT * FROM comments')
#    print(comments)