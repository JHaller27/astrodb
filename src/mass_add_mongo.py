#!../venv/bin/python
from typing import Iterator

from astropy.io import fits
import argparse
import pymongo
import re


LOCAL_MONGO_URI = 'mongodb://localhost:27017/'

DEBUG = False


def open_fits(fname: str) -> fits.HDUList:
    """
    Open a .fits file
    :param fname: path to a .fits file
    :return: An HDUList object
    """
    return fits.open(fname, memmp=True)


def get_collection(
        coll_name: str,
        db_name: str,
        db_uri: str=LOCAL_MONGO_URI,
        drop: bool=True) -> pymongo.collection:
    """
    Get MongoDB collection matching the parameters
    :param coll_name: Name of MongoDB collection
    :param db_name: Name of MongoDB database
    :param db_uri: URI of MongoDB to connect to
                   Default: localhost mongodb
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


def hdu_records(hdu_list: fits.HDUList) -> Iterator:
    """
    Generator function to yield each record in hdu_list
    :param hdu_list: HDUList object (containing data)
    :return: Iterator for the record data in hdu_list
    """
    return iter(hdu_list[1].data)


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


def insert_records(collection: pymongo.collection, record_list: list) -> int:
    """
    Inserts many records into a Mongo DB.
    :param collection: Mongo collection to insert into
    :param record_list: List of records (dicts) to insert
    :return: Number of records successfully written
    """
    print('Inserting {} records... '.format(len(record_list)), end='')

    insert_result = collection.insert_many(record_list)

    return len(insert_result.inserted_ids)


def upload_hdu_list(hdu_list: fits.HDUList,
                    collection: pymongo.collection,
                    buffer_size: int) -> int:
    """
    Reads hdu_list, extracts data into dict "records", and
    uploads chunks of data to a mongo collection.
    :param hdu_list: HDUList object from which to read
    :param collection: Mongo collection to insert into
    :param buffer_size: (optional) Size of record buffer.
                       When buffer is full, will flush to
                       collection.
                       <=0: upload all records only after
                       all have been read.
                       1: (default) upload every record
                       after it is converted.
                       X: upload after every X record is
                       converted.
    :return: Number of records successfully written
    """
    record_buffer = []    # List of records (as dicts)
    inserted_record_count = 0  # Total number of records inserted thus far

    columns = get_fits_columns(hdu_list)

    for r in hdu_records(hdu_list):
        record = generate_record(r, columns)

        record_buffer.append(record)

        # Write chunk of records to database
        if 0 < buffer_size <= len(record_buffer):
            inserted_record_count += insert_records(collection, record_buffer)
            record_buffer = []

    # Upload remaining records
    inserted_record_count += insert_records(collection, record_buffer)

    return inserted_record_count


# Main processing
# =========================================================

def main(fits_path: str,
         coll_name: str, db_name: str, db_uri: str=LOCAL_MONGO_URI):
    """
    Open a fits file, read all records, and write to database in chunks
    :param fits_path: path to a fits file
    :param coll_name: Name of MongoDB collection
    :param db_name: Name of MongoDB database
    :param db_uri: URI of MongoDB to connect to
                   Default: localhost mongodb
    """
    with open_fits(fname=fits_path) as hdu_table:
        print('Requesting collection... ', end='')
        collection = get_collection(coll_name, db_name, db_uri)
        print('Done!')

        print('Generating records... ')
        record_count = upload_hdu_list(hdu_table, collection, buffer_size=100)

        print('Done!')

    print('Database successfully populated with {} records'.format(record_count))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('path_to_fits')
    parser.add_argument('--uri', '-u', help='MongoDB URI',
                        default=LOCAL_MONGO_URI)
    parser.add_argument('--db', '-d', help='MongoDB database name')
    parser.add_argument('--coll', '-c', help='MongoDB collection name')
    parser.add_argument('--buffer', '-b', help='Size of buffer of ready-to-upload records')

    args = parser.parse_args()

    main(fits_path=args.path_to_fits,
         coll_name=args.coll, db_name=args.db, db_uri=args.uri)
