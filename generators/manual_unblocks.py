from pymysql.cursors import DictCursor
import conn_manager
import utils

# 7 days timeframe
SQL_7_DAYS_TIME_FRAME = """
    AND DATE_FORMAT(log_timestamp, '%Y%m%d')
    BETWEEN DATE_FORMAT(CURRENT_DATE - INTERVAL 7 DAY, '%Y%m%d')
        AND DATE_FORMAT(CURRENT_DATE, '%Y%m%d')
"""

def generate_data():
    print("Starting manual unblocks...")

    # get db names
    db_names = utils.get_db_names()
    conn = conn_manager.get_conn()
    data = []

    #get data for each wiki
    for dbname in db_names:
        print("Quering " + dbname)

        try:
            wiki = dbname
            conn.select_db(dbname)
            total_unblocks = get_total_unblocks()
            unblock_reasons = get_common_unblock_reasons()
            perc_unblock_author = get_perc_unblock_author()

            data.append((wiki, total_unblocks, perc_unblock_author, unblock_reasons ))
        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))


    # create csv
    headers = ('Wiki', 'Total unblocks', 'Same author unblock %', 'Common unblock reasons')
    utils.write_to_csv('unblocks', headers, data)

    print('Fin...')


def get_common_unblock_reasons():
    sql = """
    SELECT
        log_comment,
        COUNT(*) as total
    FROM logging_logindex
    WHERE log_action = 'unblock' AND log_type = 'block'
    %s
    GROUP BY log_comment
    ORDER BY total DESC
    """ % SQL_7_DAYS_TIME_FRAME

    conn = conn_manager.get_conn()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()

    ret = None
    if results:
        ret = ' | '.join([ "(%d) %s" % (val[1], str(val[0], 'utf8')) for val in results])

    return ret


def get_total_unblocks():
    sql = """
        SELECT
           COUNT(*) AS total_unblock
        FROM logging_userindex
        WHERE log_type = 'block' AND log_action = 'unblock'
        %s
    """ % SQL_7_DAYS_TIME_FRAME

    results = ()
    conn = conn_manager.get_conn()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchone()

    return results[0] if results else 0

def get_perc_unblock_author():
    unblocks = _get_unblocks()

    same_admin_unblock = 0
    for row in unblocks:
        if row['log_user'] == _get_blocking_admin(row['log_title'], row['log_timestamp']):
            same_admin_unblock += 1

    perc = 0.0
    total_unblocks = len(unblocks)
    if total_unblocks:
        perc = round(same_admin_unblock* 100 / total_unblocks, 2)

    return perc

def _get_blocking_admin(blocked_user, log_timestamp):
    sql = """
        SELECT
            log_user
        FROM logging_logindex
        WHERE log_type = 'block' AND log_action = 'block'
        AND log_title = %s AND log_timestamp < %s
        ORDER BY log_timestamp DESC
        LIMIT 1
    """

    results = []
    conn = conn_manager.get_conn()
    with conn.cursor() as cursor:
        cursor.execute(sql, (blocked_user, log_timestamp))
        results = cursor.fetchone()

    return results[0] if results else None


def _get_unblocks():
    sql = """
        SELECT
            log_title, log_user, log_timestamp
        FROM logging_logindex
        WHERE log_type = 'block' AND log_action = 'unblock'
        %s
    """ % SQL_7_DAYS_TIME_FRAME

    results = []
    conn = conn_manager.get_conn()
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()

    return results

