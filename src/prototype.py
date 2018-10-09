#!/usr/bin/python3

from astropy.io import fits
import os
import pymongo
import sys
import re


DB_URI = 'mongodb://localhost:27017/'
DB_NAME = 'astrodb'
COLL_NAME = 'fits_test'

FITS_FILE, FITS_DIR = 'pdz_cosmos2015_v1.3.fits', '/media/james/TIBERIUS/astronomy_research'

DEBUG = False


def open_fits(fdir=FITS_DIR, fname=FITS_FILE):
    fname = fname.lstrip(os.path.sep)
    fdir = fdir.rstrip(os.path.sep)
    return fits.open(os.path.join(fdir, fname), memmp=True)


def get_collection(db_uri=DB_URI, db_name=DB_NAME, coll_name=COLL_NAME):
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    coll = db[coll_name]
    coll.drop()
    return coll


def generate_record(rec, cols):
    # record: {<col1>: {'format':<format>, 'value':<value>}, <col2>: {...}, ...}
    if DEBUG:
        types = {}

    rec = list(rec)

    record = {}
    for i in range(len(cols)):
        c = cols[i]
        if c['name'].lower() == 'id':
            record['_id'] = int(rec[i])
        else:
            record[i] = {}
            record[i]['format'] = c['format']

            data = str(rec[i])
            if re.search("\-?[0-9]+\.[0-9]+", data) is not None:
                data = float(data)
            elif re.search("\-?[0-9]+", data) is not None:
                data = int(data)
            record[i]['value'] = data

    if DEBUG:
        for c_name in record:
            try:
                data = record[c_name]['value']
            except TypeError:
                data = record[c_name]
            types[type(data)] = types[type(data)] + 1 if type(data) in types else 1
        print(types)

    return record


def get_fits_columns(cols):
    return [{'name': c.name, 'format': str(c.format)} for c in cols]


if __name__ == '__main__':
    record_list = []
    with open_fits(FITS_DIR, FITS_FILE) as hdu_table:
        increment = len(hdu_table[1].data) // 10000

        cols = get_fits_columns(hdu_table[1].columns)

        print('Generating records... ')
        sys.stdout.flush()
        i = 0
        for r in hdu_table[1].data:
            record = generate_record(r, cols)

            if DEBUG:
                print(record)

            record_list.append(record)

            if (i + 1) % increment == 0:
                print('{}/{}'.format(i, len(hdu_table[1].data)))
                sys.stdout.flush()
                exit()
            i += 1
        print('Done!')
        sys.stdout.flush()

    print('Requesting collection... ', end='')
    collection = get_collection(DB_URI, DB_NAME, COLL_NAME)
    print('Done!')

    print('Inserting records... ')
    sys.stdout.flush()
    inserted_ids = collection.insert_many(record_list)
    print('Done!')
    sys.stdout.flush()

    print('Inserted {} records'.format(len(inserted_ids)))

