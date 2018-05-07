from pymysql.cursors import DictCursor
import conn_manager
import utils
from . import fetch_one, fetch_all, get_sql_time_frame


def generate_data():
    print("Starting block edits...")
    data =[]

    # get db names
    db_names = utils.get_db_names()
    conn = conn_manager.get_conn()

    #get data for each wiki
    for dbname in db_names:
        try:
            conn.select_db(dbname)

            wiki = dbname
            total_reblocks = get_total_reblocks()
            perc_reblock_author = get_perc_reblock_author()


            data.append((wiki, total_reblocks, perc_reblock_author))
        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))

    # create csv
    headers = ('Wiki', 'Total reblocks', 'Same author reblock %')
    utils.write_to_csv('block_modifications', headers, data)

    print('Fin...')


def get_perc_reblock_author():
    reblocks = _get_reblocks()

    same_admin_reblock = 0
    for row in reblocks:
        if row['log_user'] == _get_blocking_admin(row['log_title'], row['log_timestamp']):
            same_admin_reblock += 1

    perc = 0.0
    total_reblocks = len(reblocks)
    if total_reblocks:
        perc = round(same_admin_reblock * 100 / total_reblocks, 2)

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

def get_total_reblocks():
    sql = """
        SELECT
            COUNT(*)
        FROM logging_logindex
        WHERE log_type = 'block' AND log_action = 'reblock'
        %s
    """ % get_sql_time_frame('log_timestamp')

    results = fetch_one(sql)

    return results[0] if results else 0


def _get_reblocks():
    sql = """
        SELECT
            log_title, log_user, log_timestamp
        FROM logging_logindex
        WHERE log_type = 'block' AND log_action = 'reblock'
        %s
    """ % get_sql_time_frame('log_timestamp')

    return fetch_all(sql, cursor=DictCursor)
