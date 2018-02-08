import psycopg2
import psycopg2.extras
from Creds import db_username,db_password, host_name
import sys


class OcelliDB:
    connection = ""
    cursor = ""
    user = ""
    pwd = ""

    def __init__(self):
        self.user = db_username
        self.pwd = db_password

    # can use this to override the user / pwd if necessary
    def set_creds(self, user, pwd):
        self.user = user
        self.pwd = pwd
        self.host = host_name

    def connect(self):
        #conn_string = "dbname='ocelli' user='%s' host='52.45.194.240' password='%s'" % (self.user, self.pwd)
        conn_string = "dbname = 'ocelli' user= '%s' host = '10.1.10.131' password = '%s'" % (self.user, self.pwd)
        self.connection = psycopg2.connect(conn_string)
        # use DictCursor so we can access columns by their name
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def query2(self, query, *args):
        self.connect()
        rows = []
        try:
            #print self.cursor.mogrify(query, args)
            self.cursor.execute(query, args)
            rows = self.cursor.fetchall()

        except psycopg2.Error as e:
            print e

        self.connection.commit()
        self.connection.close()
        return rows

    def ins_upd2(self, query, *args):
        self.connect()
        try:
            #print self.cursor.mogrify(query, args)
            self.cursor.execute(query, args)
            self.connection.commit()
            self.connection.close()
            return 0
        except Exception as e:
            self.connection.close()
            pass

        self.connection.close()