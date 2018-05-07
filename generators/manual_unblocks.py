from pymysql.cursors import DictCursor
import conn_manager
import utils
from . import fetch_one, fetch_all, get_sql_time_frame


def generate_data():
    print("Starting manual unblocks...")

    # get db names
    db_names = utils.get_db_names()
    conn = conn_manager.get_conn()
    data = []

    #get data for each wiki
    for dbname in db_names:
        try:
            conn.select_db(dbname)

            wiki = dbname
            total_unblocks = get_total_unblocks()
            unblock_reasons = get_common_unblock_reasons()
            perc_unblock_author = get_perc_unblock_author()

            data.append((wiki, total_unblocks, perc_unblock_author, unblock_reasons ))
        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))


    # create csv
    headers = ('Wiki', 'Total unblocks', 'Same author unblock %', 'Common unblock reasons')
    utils.write_to_csv('unblocks', headers, data)


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
    """ % get_sql_time_frame('log_timestamp')

    results = fetch_all(sql)

    reasons = None
    if results:
        reasons = ' | '.join([ "(%d) %s" % (val[1], str(val[0], 'utf-8')) for val in results])

    return reasons


def get_total_unblocks():
    sql = """
        SELECT
           COUNT(*) AS total_unblock
        FROM logging_userindex
        WHERE log_type = 'block' AND log_action = 'unblock'
        %s
    """ % get_sql_time_frame('log_timestamp')

    results = fetch_one(sql)
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
        perc = round(same_admin_unblock * 100 / total_unblocks, 2)

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

    results = fetch_one(sql, params=(blocked_user, log_timestamp))

    return results[0] if results else None


def _get_unblocks():
    sql = """
        SELECT
            log_title, log_user, log_timestamp
        FROM logging_logindex
        WHERE log_type = 'block' AND log_action = 'unblock'
        %s
    """ % get_sql_time_frame('log_timestamp')

    results = fetch_all(sql, cursor=DictCursor)

    return results

