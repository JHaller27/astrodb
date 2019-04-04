#!../venv/bin/python

from astropy.io import fits
import argparse
import pymongo
import logging

LOCAL_MONGO_URI = 'mongodb://localhost:27017/'

MONGO_MAX_INT = 2**(8*8)  # Max supported int size


# Setup logging
# =========================================================

logging.getLogger().setLevel(logging.NOTSET)
log = logging.getLogger('astrodb')

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fh = logging.FileHandler('astrodb.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)


# Process functions
# =========================================================

def open_fits(fname: str) -> fits.HDUList:
    """
    Open a .fits file
    :param fname: path to a .fits file
    :return: An HDUList object
    """
    try:
        log.info("Opening '%s'" % fname)
        return fits.open(fname, memmp=True)
    except FileNotFoundError:
        log.error("File not found!")
        exit(1)
    except OSError:
        log.error("Something went wrong reading '%s'..." % fname)
        log.error("Are you sure this is a fits-format file?")
        exit(1)


def get_collection(
        coll_name: str,
        db_name: str,
        db_uri: str=LOCAL_MONGO_URI,
        drop: bool=False) -> pymongo.collection:
    """
    Get MongoDB collection matching the parameters
    :param coll_name: Name of MongoDB collection
    :param db_name: Name of MongoDB database
    :param db_uri: URI of MongoDB to connect to
                   Default: localhost mongodb
    :param drop: Set to True to drop the collection,
                 otherwise set to False
                 Default: False
    :return: MongoDB collection object
    """
    try:
        log.info('Requesting collection... ')

        client = pymongo.MongoClient(db_uri)

        db = client[db_name]
        coll = db[coll_name]
        if drop:
            coll.drop()

        log.info('Found!')
        return coll
    except pymongo.errors.ConfigurationError:
        log.error("Could not connect to URI '%s'" % db_uri)
        exit(1)


def hdu_records(hdu_list: fits.HDUList) -> list:
    """
    Generator function to yield each record in hdu_list
    :param hdu_list: HDUList object (containing data)
    :return: List of HDU records
    """
    return list(hdu_list[1].data)


def generate_record(rec: fits.FITS_rec, cols: list) -> dict:
    """
    Returns a single dict object based on FITS records and columns.
    :param rec: FITS record to convert
    :param cols: List of column definitions
    :return: Dict object representing new record
             format: {<col1>: {'format':<format>, 'value':<value>}, <col2>: {...}, ...}
    """
    rec = list(rec)

    record = {}
    for i in range(len(cols)):
        c = cols[i]
        if c['name'].lower() == 'id':
            record['_id'] = int(rec[i])
        else:
            # Try convert to integer
            try:
                data = int(rec[i])

                # MongoDB can only handle 8-byte ints
                if data >= MONGO_MAX_INT:
                    data = str(rec[i])
            except ValueError:
                # Try convert to float
                try:
                    data = float(rec[i])
                except ValueError:
                    # If all else fails, encode as string
                    data = str(rec[i])
            record[c['name']] = data

    return record


def get_fits_columns(hdu_list: fits.HDUList) -> list:
    """
    Converts FITS columns to python "columns"
    :param hdu_list: HDUList object (containing columns)
    :return: Ordered list of dictionaries of the form
             {'name': <column-name>, 'format': <data-format-code>}
    """
    cols = hdu_list[1].columns
    log.info("File has %d columns" % len(cols))
    return [{'name': c.name, 'format': str(c.format)} for c in cols]


def insert_records(collection: pymongo.collection, record_list: list) -> int:
    """
    Inserts many records into a Mongo DB.
    :param collection: Mongo collection to insert into
    :param record_list: List of records (dicts) to insert
    :return: Number of records successfully written
    """
    log.info('Inserting %d records... ' % len(record_list))
    try:
        insert_result = collection.insert_many(record_list)
        return len(insert_result.inserted_ids)
    except pymongo.errors.OperationFailure as of:
        log.error("Insertion of %d records failed..." % len(record_list))
        log.error("%s" % str(of))
        exit(1)


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

    log.info('Generating records... ')
    hdu_record_list = hdu_records(hdu_list)
    for r in hdu_record_list:
        record = generate_record(r, columns)

        append_record(record, record_buffer)

        # Write chunk of records to database
        if 0 < buffer_size <= len(record_buffer):
            tmp_count = insert_record_list(record_buffer, collection)
            inserted_record_count += tmp_count
            log.info("\tProgress {}/{} ({:.2f}%)".format(
                inserted_record_count, len(hdu_record_list),
                (inserted_record_count*100) / len(hdu_record_list)
            ))
            record_buffer = []

    # Upload remaining records
    inserted_record_count += insert_records(collection, record_buffer)
    log.info("All %d/%d records uploaded!" % (inserted_record_count, len(hdu_record_list)))

    return inserted_record_count


def append_record(record: dict, list_of_records: list) -> None:
    """
    Append a record to a list of records, merging new record with existing records in the list
    using astropy coordinate matching
    :param record: Record to insert
    :param list_of_records: List to insert record into
    """

    # If record should be merged, then merge record with matching record
    for existing_record in list_of_records:
        if should_merge_by_distance(record, existing_record, 0):
            record = merge_records(record, existing_record, "rec1", "rec2")
            list_of_records.remove(existing_record)
            break

    # Insert the new (possibly merged) record
    list_of_records.append(record)


def insert_record_list(list_of_records: list, collection: pymongo.collection) -> int:
    """
    Insert all records in a list into a pymongo collection, merging records with records in the
    collection using astropy coordinate matching
    :param list_of_records:
    :param collection:
    :return: count of records inserted
    """
    # TODO Coordinate matching
    inserted_count = insert_records(collection, list_of_records)
    return inserted_count


def should_merge_by_distance(rec1: dict, rec2: dict, threshold: int = -1) -> bool:
    """
    Compare two records by distance
    :param rec1: first record
    :param rec2: second record
    :param threshold: two records should be merged if their distance is
                      less than or equal to threshold
    :return: True if records are close enough to be merged, False otherwise
    """
    return False


def merge_records(rec1: dict, rec2: dict, suffix1: str, suffix2: str) -> dict:
    """
    Merge two records, handling duplicate keys by appending a suffix
    :param rec1: first record
    :param rec2: second record
    :param suffix1: suffix to add to keys from rec1
    :param suffix2: suffix to add to keys from rec2
    :return: merged record
    """
    return {}


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
        collection = get_collection(coll_name, db_name, db_uri, drop=True)

        record_count = upload_hdu_list(hdu_table, collection, buffer_size=100)

        log.info('Done!')

        log.info('Database successfully populated with {} records'.format(record_count))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('path_to_fits')
    parser.add_argument('-u', '--uri', help='MongoDB URI',
                        default=LOCAL_MONGO_URI)
    parser.add_argument('-d', '--db', help='MongoDB database name')
    parser.add_argument('-c', '--coll', help='MongoDB collection name')
    parser.add_argument('-b', '--buffer', help='Size of buffer of ready-to-upload records')

    args = parser.parse_args()

    main(fits_path=args.path_to_fits,
         coll_name=args.coll, db_name=args.db, db_uri=args.uri)
