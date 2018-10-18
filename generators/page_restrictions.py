import conn_manager
import utils
from . import fetch_all, get_sql_time_frame
import phpserialize
from collections import Counter
from pprint import pprint
import tablib

def generate_data():
    print("Starting page restrictions...")

    # db_names = [
    #     'itwiki_p',
    #     'eswiki_p'
    # ]

    db_names = [
        'itwiki_p',
        'eswiki_p',
        'frwiki_p',
        'dewiki_p',
        'enwiki_p'
    ]

    conn = conn_manager.get_conn()

    all_data = tablib.Databook()
    collated_data = tablib.Dataset(headers=['Wiki', 'Full', 'Semi', 'Extended'])

    def get_recent_changes(dbname):
        conn.select_db(dbname)

        sql = """
            SELECT rc_params
            FROM recentchanges
            WHERE rc_namespace = 0
            AND rc_log_type = 'protect'
            AND rc_log_action in ('protect', 'modify')
            {0}
        """.format(get_sql_time_frame('rc_timestamp'))

        results = fetch_all(sql)
        return results

    def get_total_restrictions(dbname):
        print("Starting %s total restrictions..." % dbname[:-2])

        conn.select_db(dbname)

        sql = """
            SELECT count(*), pr.pr_level
            FROM page_restrictions pr, page p
            WHERE pr.pr_page = p.page_id
            AND p.page_namespace = 0
            AND pr.pr_type = 'edit'
            AND pr.pr_level in ('sysop','autoconfirmed', 'extendedconfirmed')
            GROUP BY pr.pr_level
        """

        results = fetch_all(sql)

        data = {"full": 0, "semi": 0, "extended": 0}
        for result in results:
            level = result[1].decode("utf-8")
            count = result[0]
            if level == "autoconfirmed":
                data["semi"] = count
            if level == "sysop":
                data["full"] = count
            if level == "extendedconfirmed":
                data["extended"] = count

        collated_data.append([dbname[:-2], data["full"], data["semi"], data["extended"], "total"])

        print("Done with %s total restrictions..." % dbname[:-2])

    def get_restrictions_per_week(dbname, results):
        print("Starting %s per week restrictions..." % dbname[:-2])

        semi_count = 0
        full_count = 0
        extended_count = 0
        for result in results:
            params = phpserialize.loads(result[0], decode_strings=True)
            protect_type = params["details"][0]['type']
            protect_level = params["details"][0]['level']
            if protect_type != "edit":
                continue
            if protect_level == "autoconfirmed":
                semi_count += 1
            if protect_level == "sysop":
                full_count += 1
            if protect_level == "extendedconfirmed":
                extended_count += 1

        collated_data.append([dbname[:-2], full_count, semi_count, extended_count, "this week"])

        print("Done with %s per week restrictions..." % dbname[:-2])

    def get_expiry_distributions(dbname, results):
        print("Starting %s restriction expiry distributions..." % dbname[:-2])

        distribution_data = {}

        distribution_data["semi"] = Counter()
        distribution_data["full"] = Counter()
        distribution_data["extended"] = Counter()

        for result in results:
            params = phpserialize.loads(result[0], decode_strings=True)
            protect_type = params["details"][0]['type']
            protect_level = params["details"][0]['level']
            protect_expiry = params["details"][0]['expiry'][:8]

            if protect_type != "edit":
                continue
            if protect_level == "autoconfirmed":
                distribution_data["semi"][protect_expiry] += 1
            if protect_level == "sysop":
                distribution_data["full"][protect_expiry] += 1
            if protect_level == "extendedconfirmed":
                distribution_data["extended"][protect_expiry] += 1

        dist_data = tablib.Dataset(headers=["# of protections on %s" % dbname[:-2], "expiry", "protection type"])
        for key, value in distribution_data.items():
            for expiry, number in value.items():
                dist_data.append([number, '{0}-{1}-{2}'.format(expiry[:4], expiry[4:6], expiry[6:]), key])
        print("Distribution for %s" % dbname[:-2])
        print(dist_data)
        all_data.add_sheet(dist_data)
        print("Done with %s expiry distribution..." % dbname[:-2])


    for dbname in db_names:
        print("Starting %s restrictions..." % dbname[:-2])

        recent_changes = get_recent_changes(dbname)

        get_total_restrictions(dbname)
        get_restrictions_per_week(dbname, recent_changes)
        get_expiry_distributions(dbname, recent_changes)

        print("Done with %s restrictions..." % dbname[:-2])

    print("Number of Restrictions")
    print(collated_data)

    all_data.add_sheet(collated_data)
    with open('data/restrictions.xls', 'wb') as f:
        f.write(all_data.xls)

    print("Done with all page restrictions...")


