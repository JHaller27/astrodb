#!../venv/bin/python

from astropy.io import fits
from astropy import units as u
from astropy.coordinates import *
import argparse
import pymongo
import logging

# Constants and globals
# =========================================================

LOCAL_MONGO_URI = 'mongodb://localhost:27017/'

MONGO_MAX_INT = 2 ** (8 * 8)  # Max supported int size

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
        db_uri: str = LOCAL_MONGO_URI,
        drop: bool = False) -> pymongo.collection:
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
    except Exception as e:
        log.error(e)
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
            # Try convert to float
            try:
                data = float(rec[i])

                # MongoDB can only handle 8-byte ints
                if data >= MONGO_MAX_INT:
                    data = str(rec[i])
            except ValueError:
                # Try convert to int
                try:
                    data = int(rec[i])
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
                    collection: pymongo.collection) -> int:
    """
    Reads hdu_list, extracts data into dict "records", and
    uploads chunks of data to a mongo collection.
    :param hdu_list: HDUList object from which to read
    :param collection: Mongo collection to insert into
    :return: Number of records successfully written
    """
    global args

    record_buffer = []  # List of records (as dicts)
    inserted_record_count = 0  # Total number of records inserted thus far

    columns = get_fits_columns(hdu_list)

    log.info('Generating records... ')
    hdu_record_list = hdu_records(hdu_list)
    for r in hdu_record_list:
        record = generate_record(r, columns)

        # Coordinate-matches within the buffer
        append_record(record, record_buffer)

        # Write chunk of records to database
        if 0 < args.buffer <= len(record_buffer):
            # Coordinate-matches against database
            tmp_count = insert_record_list(record_buffer, collection, args.sep)
            inserted_record_count += tmp_count
            log.info("\tProgress {}/{} ({:.2f}%)".format(
                inserted_record_count, len(hdu_record_list),
                (inserted_record_count * args.buffer) / len(hdu_record_list)
            ))
            record_buffer = []

    # Upload remaining records
    inserted_record_count += insert_record_list(record_buffer, collection, args.sep)
    log.info("All %d/%d records uploaded!" % (inserted_record_count, len(hdu_record_list)))

    return inserted_record_count


def append_record(record: dict, list_of_records: list) -> None:
    """
    Append a record to a list of records, merging new record with existing records in the list
    using astropy coordinate matching
    :param record: Record to insert
    :param list_of_records: List to insert record into
    """
    global args

    # If record should be merged, then merge record with matching record
    for existing_record in list_of_records:
        if should_merge_by_distance(record, existing_record, args.sep):
            record = merge_records(record, existing_record, "rec1", "rec2")
            list_of_records.remove(existing_record)
            break

    # Insert the new (possibly merged) record
    list_of_records.append(record)


def insert_record_list(list_of_records: list, collection: pymongo.collection,
                       threshold: float) -> int:
    """
    Insert all records in a list into a pymongo collection, merging records with records in the
    collection using astropy coordinate matching
    :param list_of_records:
    :param collection:
    :param threshold:
    :return: count of records inserted
    """
    for new_record in list_of_records:
        # Generate mongo query to find objects whose coordinates are within the
        #    bounds of the threshold (a square area)
        #    (limits the number of comparisons while using actual coordinate matching)
        # *_bounds = (<lower bound>, <upper bound>)
        ra_bounds = (new_record["RA"] - threshold, new_record["RA"] + threshold)
        dec_bounds = (new_record["DEC"] - threshold, new_record["DEC"] + threshold)
        query = {"RA": {"$gte": ra_bounds[0], "$lte": ra_bounds[1]},
                 "DEC": {"$gte": dec_bounds[0], "$lte": dec_bounds[1]}
                 }

        # Compare against existing records matching the query
        #   (since threshold is radius, not the length of a square)
        for existing_record in collection.find(query):
            if should_merge_by_distance(new_record, existing_record, threshold):
                merged_record = merge_records(new_record, existing_record, "_rec1", "_rec2")

                collection.remove(existing_record)

                list_of_records.remove(new_record)
                list_of_records.append(merged_record)

    inserted_count = len(list_of_records)  # insert_records(collection, list_of_records)
    return inserted_count


def should_merge_by_distance(rec1: dict, rec2: dict, threshold: float) -> bool:
    """
    Compare two records by distance
    :param rec1: first record
    :param rec2: second record
    :param threshold: two records should be merged if their distance is
                      less than the threshold
    :return: True if records are close enough to be merged, False otherwise
    """
    coords1 = SkyCoord(ra=rec1["RA"] * u.degree, dec=rec1["DEC"] * u.degree, distance=1, frame="icrs")
    coords2 = SkyCoord(ra=rec2["RA"] * u.degree, dec=rec2["DEC"] * u.degree, distance=1, frame="icrs")
    sep = coords1.separation(coords2)
    log.debug("\tSeparation = {:.2f} (<{:.2f}, {:.2f}>, <{:.2f}, {:.2f}>)".format(
        sep.to(u.arcsec),
        coords1.ra.to(u.arcsec), coords1.ra.to(u.arcsec),
        coords2.dec.to(u.arcsec), coords2.dec.to(u.arcsec))
    )
    return sep.is_within_bounds(-threshold * u.arcsec, threshold * u.arcsec)


def merge_records(rec1: dict, rec2: dict,
                  suffix1: str, suffix2: str,
                  ra_key: str = "RA", dec_key: str = "DEC") -> dict:
    """
    Merge two records, handling duplicate keys by appending a suffix
    :param rec1: first record
    :param rec2: second record
    :param suffix1: suffix to add to keys from rec1 (coordinates used as masters)
    :param suffix2: suffix to add to keys from rec2
    :param ra_key: master key to use as RA coordinate
    :param dec_key: master key to use as DEC coordinate
    :return: merged record
    """
    log.info("Merging records")

    keys1 = set(rec1.keys())
    keys2 = set(rec2.keys())
    dupes = keys1.intersection(keys2)
    uniques = keys1.union(keys2).difference(dupes)

    new_rec = {}
    for k in uniques:
        new_rec[k] = rec1[k] if k in rec1 else rec2[k]
    for k in dupes:
        k1 = "%s_%s" % (k, suffix1)
        k2 = "%s_%s" % (k, suffix2)

        new_rec[k1] = rec1[k]
        new_rec[k2] = rec2[k]

    if ra_key in dupes:
        new_rec[ra_key] = rec1[ra_key]
    if "DEC" in dupes:
        new_rec[dec_key] = rec1[dec_key]

    return new_rec


# Main processing
# =========================================================

def main():
    """
    Open a fits file, read all records, and write to database in chunks
    """
    global args

    with open_fits(fname=args.path_to_fits) as hdu_table:
        collection = get_collection(args.coll, args.db, args.uri, drop=True)

        record_count = upload_hdu_list(hdu_table, collection)

        log.info('Done!')

        log.info('Database successfully populated with {} records'.format(record_count))


parser = argparse.ArgumentParser()

parser.add_argument('path_to_fits')
parser.add_argument('-u', '--uri', help='MongoDB URI',
                    default=LOCAL_MONGO_URI)
parser.add_argument('-d', '--db', help='MongoDB database name')
parser.add_argument('-c', '--coll', help='MongoDB collection name')
parser.add_argument('-b', '--buffer', type=int, help='Size of buffer of ready-to-upload records')
parser.add_argument('-s', '--sep', type=float, default=0.0,
                    help='Separation threshold under which objects are considered the same\n(units: arcseconds)')

args = parser.parse_args()

if __name__ == '__main__':
    main()
