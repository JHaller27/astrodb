#!venv/bin/python

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


def open_fits(fname: str=FITS_FILE) -> fits.HDUList:
    """
    Open a .fits file
    :param fname: path to a .fits file
    :return: An HDUList object
    """
    return fits.open(fname, memmp=True)


def get_collection(
        db_uri: str=DB_URI,
        db_name: str=DB_NAME,
        coll_name: str=COLL_NAME,
        drop: bool=True) -> pymongo.collection:
    """
    Get MongoDB collection matching the parameters
    :param db_uri: URI of MongoDB to connect to
    :param db_name: Name of MongoDB database
    :param coll_name: Name of MongoDB collection
    :param drop: Set to True to drop the collection,
                 otherwise set to False
    :return: MongoDB collection object
    """
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    coll = db[coll_name]
    if drop:
        coll.drop()
    return coll


def generate_record(rec: fits.FITS_rec, cols: list) -> dict:
    """
    Returns a single dict object based on FITS records and columns.
    :param rec: FITS record to convert
    :param cols: List of column definitions
    :return: Dict object representing new record
    """
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
            record[c['name']] = {}
            record[c['name']]['format'] = c['format']

            data = str(rec[i])
            if re.search("\-?[0-9]+\.[0-9]+", data) is not None:
                data = float(data)
            elif re.search("\-?[0-9]+", data) is not None:
                data = int(data)
            record[c['name']]['value'] = data

    if DEBUG:
        for c_name in record:
            try:
                data = record[c_name]['value']
            except TypeError:
                data = record[c_name]
            # noinspection PyUnboundLocalVariable
            types[type(data)] = types[type(data)] + 1 if type(data) in types else 1
        print(types)

    return record


def get_fits_columns(cols: fits.ColDefs) -> list:
    """
    Converts FITS columns to python "columns"
    :param cols: Column definitions from astropy.fits
    :return: List of dictionaries of the form
             {'name': <column-name>, 'format': <column-format>}
    """
    return [{'name': c.name, 'format': str(c.format)} for c in cols]


def insert_records(collection: pymongo.collection, record_list: list):
    """
    Inserts many records into a Mongo DB.
    :param collection: Mongo collection to insert into
    :param record_list: List of records (dicts) to insert
    :return: an InsertManyResult object
    """
    print('Inserting {} records... '.format(len(record_list)), end='')
    sys.stdout.flush()

    insert_result = collection.insert_many(record_list)

    return insert_result


def main():
    """
    Main driver for reading then inserting records
    """
    record_list = []
    record_count = 0
    with open_fits() as hdu_table:
        increment = 100

        cols = get_fits_columns(hdu_table[1].columns)

        print('Requesting collection... ', end='')
        collection = get_collection(DB_URI, DB_NAME, COLL_NAME)
        print('Done!')

        print('Generating records... ')
        sys.stdout.flush()
        i = 0
        for r in hdu_table[1].data:
            record = generate_record(r, cols)

            if DEBUG:
                print(record)

            record_list.append(record)
            record_count += 1

            if (i + 1) % increment == 0:
                insert_records(collection, record_list)
                record_list = []

                print('\t{} records left'.format(len(hdu_table[1].data) - record_count))

            i += 1

        insert_records(collection, record_list)

        print('Done!')
        sys.stdout.flush()

    print('Database successfully populated')


if __name__ == '__main__':
    main()
