#!venv/bin/python
from typing import Iterator

from astropy.io import fits
import pymongo
import re


DB_URI = 'mongodb://localhost:27017/'
DB_NAME = 'astrodb'
COLL_NAME = 'fits_test'

FITS_FILE = 'COSMOS2015_Laigle+_v1.1.fits.gz'

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
             format: {<col1>: {'format':<format>, 'value':<value>}, <col2>: {...}, ...}
    """
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
            if re.search("-?[0-9]+\.[0-9]+", data) is not None:
                data = float(data)
            elif re.search("-?[0-9]+", data) is not None:
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


def get_fits_columns(hdu_list: fits.HDUList) -> list:
    """
    Converts FITS columns to python "columns"
    :param hdu_list: HDUList object (containing columns)
    :return: Ordered list of dictionaries of the form
             {'name': <column-name>, 'format': <data-format-code>}
    """
    cols = hdu_list[1].columns
    return [{'name': c.name, 'format': str(c.format)} for c in cols]


def insert_records(collection: pymongo.collection, record_list: list):
    """
    Inserts many records into a Mongo DB.
    :param collection: Mongo collection to insert into
    :param record_list: List of records (dicts) to insert
    :return: an InsertManyResult object
    """
    print('Inserting {} records... '.format(len(record_list)), end='')

    insert_result = collection.insert_many(record_list)

    return insert_result


def hdu_records(hdu_list: fits.HDUList) -> Iterator:
    """
    Generator function to yield each record in hdu_list
    :param hdu_list: HDUList object (containing data)
    :return: Iterator for the record data in hdu_list
    """
    return iter(hdu_list[1].data)


def main():
    """
    Open a fits file, read all records, and write to database in chunks
    """
    record_list = []  # List of records (as dicts)
    record_count = 0  # Count of total records read thus far

    with open_fits() as hdu_table:
        chunk_size = 100  # Number of records to read/write to database

        columns = get_fits_columns(hdu_table)

        print('Requesting collection... ', end='')
        collection = get_collection(DB_URI, DB_NAME, COLL_NAME)
        print('Done!')

        print('Generating records... ')
        i = 0  # Total number of records read thus far
        for r in hdu_records(hdu_table):
            record = generate_record(r, columns)

            if DEBUG:
                print(record)

            record_list.append(record)
            record_count += 1

            # Write chunk of records to database
            if (i + 1) % chunk_size == 0:
                insert_records(collection, record_list)
                record_list = []

                print('\t{} records left'.format(len(hdu_table[1].data) - record_count))

            i += 1

        # Write remaining records to database
        insert_records(collection, record_list)

        print('Done!')

    print('Database successfully populated')


if __name__ == '__main__':
    main()
