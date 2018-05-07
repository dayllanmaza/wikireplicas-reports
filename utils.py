import sys
import os
import csv
import requests

db_names = []

def get_db_names():
    global db_names

    if not db_names:
        response = requests.get('https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json')

        if response.status_code != 200:
            raise Exception("can't reach wikimedia api")

        data = response.json()['sitematrix']
        for key, val in data.items():
            if key.isnumeric():
                sites = val['site']
            elif key == 'specials':
                sites = data[key]
            else:
                continue

            for site in sites:
                db_names.append(site['dbname'] + '_p')

    return db_names


def get_wikis_url():
    urls = []
    response = requests.get('https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json')

    if response.status_code != 200:
        raise Exception("can't reach wikimedia api")

    data = response.json()['sitematrix']
    for key, val in data.items():
        if key.isnumeric():
            sites = val['site']
        elif key == 'specials':
            sites = data[key]
        else:
            continue

        for site in sites:
            urls.append(site['url'])

    return urls


def write_to_csv(filename, headers, data):
    print('Generating CSV: ' + filename)
    data_folder = os.path.dirname(os.path.abspath(__file__)) + '/data/'
    with open(data_folder + filename + '.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, dialect='excel')

        writer.writerow(headers)
        writer.writerows(data)
