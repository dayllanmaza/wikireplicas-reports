import sys
from pymysql.cursors import DictCursor
from collections import OrderedDict
import phpserialize
import conn_manager
import utils
from . import fetch_one, fetch_all, get_sql_time_frame


def generate_data():
    print("Starting blocks per wiki...")

    # get db names
    db_names = utils.get_db_names()
    conn = conn_manager.get_conn()
    data = []

    #get data for each wiki
    for dbname in db_names:
        try:
            conn.select_db(dbname)

            wiki = dbname
            avg_blocks_per_hour = get_avg_blocks_per_hour()
            total_active_autoblocks = get_total_active_blocks(True)
            total_active_blocks = get_total_active_blocks()
            block_length_distribution = get_block_length_distribution()
            block_reasons = get_common_block_reasons()
            perc_blocks_prevent_talk_page = get_blocks_prevent_talk_page()

            data.append((
                wiki,
                avg_blocks_per_hour,
                total_active_blocks,
                total_active_autoblocks,
                perc_blocks_prevent_talk_page,
                block_length_distribution,
                block_reasons
            ))
        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))

    # create csv
    headers = (
        'Wiki',
        'Avg blocks per hour',
        'Total active blocks',
        'Total active autoblocks',
        '% blocks prevent edit talk page',
        'Block length distribution'
        'Common unblock reasons'
    )
    utils.write_to_csv('blocks_per_wiki', headers, data)


def get_block_length_distribution():
    sql = """
        SELECT
           log_params
        FROM logging_userindex
        WHERE log_type = 'block' AND log_action = 'block'
        %s
    """ % get_sql_time_frame('log_timestamp')

    results = fetch_all(sql)

    durations = {}
    valid_durations = ('minute', 'hour', 'day', 'week', 'month', 'year', 'never', 'infinite', 'indefinite')
    for log_params in results:
        try:
            params = phpserialize.loads(log_params[0], decode_strings=True)
            key = params['5::duration']
        except Exception as err:
            print('Error deserializing php:', err)
            continue

        if not any(keyword in key for keyword in valid_durations):
            print('Invalid Duration, replacing with other: ', key)
            key = 'other'

        if key in durations:
            durations[key] += 1
        else:
            durations[key] = 1

    dist_str = ''
    if durations:
        dist = [(key, durations[key]) for key in sorted(durations, key=durations.get, reverse=True)]
        dist_str = ' | '.join(['{0} ({1})'.format(*d) for d in dist ])

    return dist_str


def get_avg_blocks_per_hour():
    sql = """
        SELECT
           COUNT(*) AS total
        FROM logging_userindex
        WHERE log_type = 'block' AND log_action = 'block'
        %s
    """ % get_sql_time_frame('log_timestamp')

    results = fetch_one(sql)

    avg_blocks_per_hour = 0
    if results:
        avg_blocks_per_hour = round(results[0] / (24 * 7))

    return avg_blocks_per_hour


def get_blocks_prevent_talk_page():
    sql = """
    SELECT
        COUNT(*) AS total_blocks,
        SUM(IF (log_params like '%nousertalk%', 1 ,0))
    FROM logging_logindex
        WHERE log_type = 'block' AND log_action = 'block'
        {0}
    """.format(get_sql_time_frame('log_timestamp'))

    results = fetch_one(sql)
    perc = 0
    if results and results[0] > 0:
        perc =  round(results[1] * 100 / results[0], 2)

    return perc


def get_total_active_blocks(onlyAutoblocks = False):
    sql = """
    SELECT
        COUNT(*)
    FROM ipblocks
    WHERE 1=1
    %s
    """ % get_sql_time_frame('ipb_timestamp')

    if onlyAutoblocks:
        sql += ' AND ipb_auto = 1'

    results = fetch_one(sql)
    return results[0] if results else 0


def get_common_block_reasons():
    sql = """
    SELECT
        log_comment,
        COUNT(*) as total
    FROM logging_logindex
    WHERE log_action = 'block' AND log_type = 'block'
    %s
    GROUP BY log_comment
    ORDER BY total DESC
    """ % get_sql_time_frame('log_timestamp')

    conn = conn_manager.get_conn()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()

    reasons = None
    if results:
        # TODO: remove comment id <!-- 1234 -->
        reasons = ' | '.join([ "({0}) {1}".format(val[1], str(val[0], 'utf-8')) if val[0] is not None else '' for val in results[:100]])

    return reasons
