import conn_manager
import utils
from . import fetch_all

def generate_data():
    print("Starting top admins...")

    db_names = [
        'plwiki_p',
        'svwiki_p',
        'jawiki_p',
        'nlwiki_p',
        'zhwiki_p',
        'ptwiki_p',
        'nnwiki_p',
        'ukwiki_p',
        'hewiki_p',
        'cswiki_p',
        'tawiki_p',
        'itwiki_p',
        'enwiki_p',
        'dewiki_p',
        'frwiki_p',
        'eswiki_p',
        'ruwiki_p',
        'ptwiki_p',
        'kowiki_p',
        'arwiki_p',
        'fawiki_p',
        'commonswiki_p',
        'wikidatawiki_p',
        'metawiki_p'
    ]

    conn = conn_manager.get_conn()
    data = []

    # get data for each wiki
    for dbname in db_names:
        try:
            conn.select_db(dbname)

            sql = """
                SELECT
                    '%s' AS wiki,
                    CAST(log_user_text as CHAR) as user,
                    (
                        SELECT IF (COUNT(*) > 0, 'No', 'Yes')
                        FROM user_properties
                        WHERE up_property = 'disablemail'
                        AND up_value = 1 AND up_user = log_user
                    ) AS disablemail,
                    COUNT(*) AS total_blocks
                FROM logging
                LEFT JOIN user_groups ON ug_user = log_user
                    AND ug_group IN ('bot', 'flow-bot')
                WHERE log_action = 'block' AND log_type = 'block'
                AND log_timestamp >= 20180801000000 AND log_timestamp <= 20181031235959
                AND ug_group IS NULL
                GROUP BY log_user ORDER BY total_blocks DESC LIMIT 30
            """ % dbname[:-2]

            results = fetch_all(sql)

            data.extend(results)

        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))

    # create csv
    headers = ('wiki', 'username', 'allow email', '# blocks performed')
    utils.write_to_csv('top_admins', headers, data)
