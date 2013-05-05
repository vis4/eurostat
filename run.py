import gzip
import sys
import csv
import config
import logging
from datetime import datetime

from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError
import dataset
import requests


def _get_float(s):
    try:
        return float(s)
    except:
        return None


def get_table(table_id):
    # print 'downloading', table_id
    url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?sort=-3&file=data%%2F%s.tsv.gz' % table_id
    gz_tsv = 'tsv/%s.tsv.gz' % table_id
    with open(gz_tsv, 'wb') as handle:
        request = requests.get(url, stream=True)
        for block in request.iter_content(1024):
            if not block:
                break
            handle.write(block)
    return gzip.open(gz_tsv, 'rb')


def import_tsv(db, table_id):
    try:
        tsv_file = get_table(table_id)
    except ConnectionError:
        return False
    sys.stdout.write(table_id+' ')
    c = 0
    try:
        db.query('DELETE FROM %s' % table_id)
        print 'd ',
    except:
        pass
    table = db[table_id]
    csv_in = csv.reader(tsv_file, delimiter='\t')
    head = map(lambda k: k.strip(), csv_in.next())
    dim = map(lambda k: k.strip(), head[0].split(','))
    dim[-1], h = dim[-1].split('\\')
    inserts = []
    for row in csv_in:
        # explode first column
        rdim = row[0].split(',')
        for i in range(1, len(row[1:])):
            data = dict()
            for d in range(len(dim)):
                data[dim[d]] = rdim[d]
            data[h] = head[i].strip()
            data['value'] = _get_float(row[i])
            data['value_s'] = row[i].strip()
            inserts.append(data)
        if len(inserts) > 25000:
            table.insert_many(inserts)
            inserts = []
            sys.stdout.write('.')
            c += 1
            if c % 4 == 0:
                sys.stdout.write(' ')

    if len(inserts) > 0:
        table.insert_many(inserts)
        sys.stdout.write('.')
    sys.stdout.write('\n')
    db['_tables'].upsert(dict(table_id=table_id, last_updated=datetime.now()), ['table_id'])
    return True


def get_index(db):
    sys.stdout.write('fetching table index')
    url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?dir=data&sort=-3&sort=2&start=all'
    r = requests.get(url)
    sys.stdout.write('.\n')
    soup = BeautifulSoup(r.text)
    table = soup.find('table')

    failed = []

    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) == 5 and tds[1].text.strip().replace('&nbsp;', '') != '':
            tid = tds[0].find('a').text
            if '.tsv.gz' in tid:
                tid = tid[:-7]
                if '_tables' in db.tables:
                    db_tbl = db['_tables'].find_one(table_id=tid)
                    if db_tbl:
                        last_updated_s = tds[3].text.strip()  # 02/05/2013 23:00:06
                        last_updated = datetime.strptime(last_updated_s, '%d/%m/%Y %H:%M:%S')
                        print db_tbl['last_updated'], last_updated
                        if last_updated < db_tbl['last_updated']:
                            print 'ignoring', tid
                            # ignore this table as we already imported the most recent version
                            continue
                if not import_tsv(db, tid):
                    failed.append(tid)
    for tid in failed:
        import_tsv(db, tid)


def get_db():
    sys.stdout.write('opening database connection')
    db = dataset.connect(config.DB)
    sys.stdout.write('.\n')
    return db


def main():
    db = get_db()
    if len(sys.argv) > 1:
        import_tsv(db, sys.argv[1])
    else:
        get_index(db)

if __name__ == '__main__':
    logging.disable(logging.WARNING)
    main()
