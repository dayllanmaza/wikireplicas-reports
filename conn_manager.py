import pymysql

conn = None

def get_conn():
    global conn
    if conn is None:
        print('Connecting to replica cluster..')
        conn = pymysql.connect(read_default_file='replica.my.cnf')
    return conn

    