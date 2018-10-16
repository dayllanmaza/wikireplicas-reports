import pymysql

conn = None

def get_conn():
    global conn
    if conn is None:
        conn = pymysql.connect(read_default_file='replica.my.cnf', charset='utf8')
    return conn
