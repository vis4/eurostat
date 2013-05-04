import sys
from os.path import basename, splitext, dirname
from os import rename
from glob import glob
import csv
import dataset


def get_float(s):
    try:
        return float(s)
    except:
        return None


def import_tsv(src):
    tid = splitext(basename(src))[0]
    c = 0
    try:
        db.query('DELETE FROM %s' % tid)
        print '(drop) ',
    except:
        pass
    table = db[tid]
    csv_in = csv.reader(open(src), delimiter='\t')
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
            data['value'] = get_float(row[i])
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


if __name__ == '__main__':

    if len(sys.argv) == 1:
        print 'Usage:  python import_tsv.py  eurostat_tsv_files  database '
        exit()

    src = sys.argv[1]

    if len(sys.argv) > 2:
        db = sys.argv[2]
    else:
        db = 'sqlite:///eurostat.db'

    db = dataset.connect(db)
    for fn in glob(src):
        print splitext(basename(fn))[0] + ' ',
        import_tsv(fn)
        rename(fn, dirname(fn) + '/_imported/' + basename(fn))
        sys.stdout.write('d\n')
