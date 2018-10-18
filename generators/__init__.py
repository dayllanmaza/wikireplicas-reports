import conn_manager

def fetch_all(sql, params=None, cursor=None):
    results = []
    conn = conn_manager.get_conn()
    with conn.cursor(cursor) as cursor:
        cursor.execute(sql, params)
        results = cursor.fetchall()

    return results


def fetch_one(sql, params=None, cursor=None):
    results = None
    conn = conn_manager.get_conn()
    with conn.cursor(cursor) as cursor:
        cursor.execute(sql, params)
        results = cursor.fetchone()

    return results


def get_sql_time_frame(column_name):
    return """
        AND DATE_FORMAT( {0}, '%Y%m%d')
        BETWEEN DATE_FORMAT(CURRENT_DATE - INTERVAL 7 DAY, '%Y%m%d')
            AND DATE_FORMAT(CURRENT_DATE, '%Y%m%d')
    """.format(column_name)

__all__ = ['manual_unblocks', 'blocks_per_wiki', 'all_wikis', 'block_edits', 'ipboptions', 'top_admins', 'page_restrictions']
