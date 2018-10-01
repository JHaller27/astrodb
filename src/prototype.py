#!/usr/bin/python3

from astropy.io import fits
import os
import pymongo
import sys
import re


DB_URI = 'mongodb://localhost:27017/'
DB_NAME = 'astrodb'
COLL_NAME = 'fits_test'

DEBUG = False

def open_fits(fname, fdir='.'):
    fname = fname.lstrip(os.path.sep)
    fdir = fdir.rstrip(os.path.sep)
    return fits.open(os.path.join(fdir, fname), memmp=True)


def get_collection(db_uri, db_name, coll_name):
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    coll = db[coll_name]
    coll.drop()
    return coll


def generate_record(tbl, idx):
    # record: {<col1>: {'format':<format>, 'value':<value>}, <col2>: {...}, ...}
    if DEBUG:
        types = {}
    record = {}
    for c in tbl.columns:
        if c.name.lower() == 'id':
            record['_id'] = int(tbl.data[c.name][idx])
        else:
            record[c.name] = {}
            record[c.name]['format'] = c.format

            data = str(tbl.data[c.name][idx])
            if re.search("[0-9]+(\.[0-9]+)", data) is not None:
                data = float(data)
            elif re.search("[0-9]+", data) is not None:
                data = int(data)
            record[c.name]['value'] = data

    if DEBUG:
        for c_name in record:
            try:
                data = record[c_name]['value']
            except TypeError:
                data = record[c_name]
            types[type(data)] = types[type(data)] + 1 if type(data) in types else 1
        print(types)

    return record

if __name__ == '__main__':
    with open_fits('pdz_cosmos2015_v1.3.fits', '/media/james/TIBERIUS/astronomy_research') as hdu_table:
        print('Requesting collection... ', end='')
        collection = get_collection(DB_URI, DB_NAME, COLL_NAME)
        print('Done!')

        inserted_ids = []
        increment = len(hdu_table[1].data) // 100

        print('Generating records... ', end='')
        sys.stdout.flush()
        for i in range(1): #len(hdu_table[1].data)):
            record = generate_record(hdu_table[1], i)

            if DEBUG:
                print(record)

            inserted_ids.append(collection.insert_one(record))
            if i % increment == increment - 1:
                print('.', end='')
                sys.stdout.flush()
        print('Done!')
        sys.stdout.flush()

    print('Inserted {} records'.format(len(inserted_ids)))

