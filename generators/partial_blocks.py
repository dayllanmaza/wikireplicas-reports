from pymysql.cursors import DictCursor
import conn_manager
import utils
import json
import sys
import phpserialize
from pprint import pprint
from . import fetch_one, fetch_all, get_sql_time_frame


def generate_data():
    print("Starting partial blocks...")
    data =[]

    # get db names
    db_names = ['itwiki_p']
    conn = conn_manager.get_conn()

    #get data for each wiki
    for dbname in db_names:
        try:
            conn.select_db(dbname)

            wiki = dbname
            blocks = get_partial_blocks()

            data.extend(blocks)
        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))

    # create csv
    headers = ('blocker', 'blockee', 'date', 'reason', 'params')
    utils.write_to_csv('partial_blocks', headers, data)

    print('Fin...')


def get_partial_blocks():
    sql = """
        SELECT
            log_params,
            log_user_text as blocker,
            log_title as blockee,
            DATE_FORMAT( log_timestamp, '%Y-%m-%d') as date,
            comment_text as reason
        FROM logging_logindex l
        LEFT JOIN comment on  comment_id = log_comment_id
        WHERE log_type = 'block'
        AND log_params like '%"sitewide";b:0;%'
       {0}
    """.format(get_sql_time_frame('log_timestamp', '30 DAY'))

    results = fetch_all(sql, cursor=DictCursor)

    data = []
    for block in results:
        log_params = phpserialize.loads(block['log_params'], decode_strings=True)
        data.append([
            block['blocker'].decode('utf-8'),
            block['blockee'].decode('utf-8'),
            block['date'],
            block['reason'].decode('utf-8'),
            json.dumps(log_params)
        ])

    return data
