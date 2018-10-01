#!/usr/bin/python3

from astropy.io import fits
import os
import pymongo


DB_URI = 'mongodb://localhost:27017/'
DB_NAME = 'astrodb'
COLL_NAME = 'fits_test'


def open_fits(fname, fdir='.'):
    fname = fname.lstrip(os.path.sep)
    fdir = fdir.rstrip(os.path.sep)
    return fits.open(os.path.join(fdir, fname), memmp=True)


def get_collection(db_uri, db_name, coll_name):
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    coll = db[coll_name]
    return coll


def generate_record(tbl, idx):
    # record: {<col1>: {'format':<format>, 'value':<value>}, <col2>: {...}, ...}
    record = {}
    for c in tbl.columns:
        if c.name.lower() == 'id':
            record['_id'] = tbl.data[c.name][idx]
        else:
            record[c.name] = {}
            record[c.name]['format'] = c.format
            record[c.name]['value'] = tbl.data[c.name][idx]
    return record

if __name__ == '__main__':
    with open_fits('pdz_cosmos2015_v1.3.fits', '/mnt/d/astronomy_research') as hdu_table:
        print('Requesting collection... ', end='')
        collection = get_collection(DB_URI, DB_NAME, COLL_NAME)
        print('Done!')

        records = []

        print('Generating records', end='')
        increment = 100

        for i in range(len(hdu_table[1].data)):
            record = generate_record(hdu_table[1], i)
            records.append(record)
            if i % increment == increment - 1:
                print('.', end='')
        print('Done!')

        print('Inserting records... ', end='')
        inserted_ids = collection.insert_many(records)
        print('Done!')

    print('Inserted {} records'.format(len(inserted_ids)))

