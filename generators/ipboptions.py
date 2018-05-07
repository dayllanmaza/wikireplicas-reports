import utils
import requests
import re


def generate_data():
    print("Starting ipblocks unblocks...")

    # get db names
    wikis = utils.get_wikis_url()
    failed_wikis = []
    data = []

    # get data for each wiki
    for wiki in wikis:
        try:
            data.append( fetch_revision(wiki) )
        except:
            failed_wikis.append(wiki)

    total = len(data)
    total_modified = sum([options is not None for options in data])

    stats = (
        total_modified,
        round(total_modified * 100 / total, 2),
        total
    )

    common_lenght_options = get_common_lenght_options(data)

    headers = ('Total modified', '% modified', 'Common length options')
    csv_data = [(total_modified, round(total_modified * 100 / total, 2), common_lenght_options)]

    utils.write_to_csv('ipboptions', headers, csv_data)



def fetch_revision(wiki):
    url = '{0}/w/api.php?action=query&prop=revisions&titles=MediaWiki:Ipboptions&rvprop=content&formatversion=2&format=json'.format(wiki)
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("can't reach wikimedia api")

    data = response.json()['query']['pages'][0]
    if 'revisions' in data:
        return data['revisions'][0]['content']

    return None


def get_common_lenght_options(data):
    lenght_options = re.findall('[^,:]+:([^,]+)',','.join(filter(None, data)))

    durations = {}
    for key in lenght_options:
        if key in durations:
            durations[key] += 1
        else:
            durations[key] = 1

    dist_str = ''
    if durations:
        dist = [(key, durations[key]) for key in sorted(durations, key=durations.get, reverse=True)]
        dist_str = ' | '.join(['{0} ({1})'.format(*d) for d in dist ])

    return dist_str


