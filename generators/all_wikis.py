import conn_manager
import utils
from . import fetch_one

def generate_data():
    print("Starting all wikis ...")

    # get db names
    db_names = utils.get_db_names()
    conn = conn_manager.get_conn()

    #get data for each wiki
    total_blocks = 0
    for dbname in db_names:
        try:
            conn.select_db(dbname)

            total_blocks += get_total_active_blocks()
        except Exception as err:
            print('Something wrong with %s, %s' % (dbname, err))

    utils.write_to_csv('all_wiki', ('Total active blocks', ), [(total_blocks,)])


def get_total_active_blocks():
    sql = """
    SELECT
        COUNT(*) AS total_blocks
    FROM ipblocks
    """

    results = fetch_one(sql)
    return results[0] if results else 0