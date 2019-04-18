#!../venv/bin/python

from astropy.io import fits
from astropy.io import ascii
from astropy import units as u
from astropy.coordinates import *
from os import sep as os_path_sep
import argparse
import pymongo
import logging

# Constants and globals
# =========================================================

LOCAL_MONGO_URI = 'mongodb://localhost:27017/'

MONGO_MAX_INT = 2 ** (8 * 8)  # Max supported int size

DATA_KEY = "data"

SOURCE_KEY = "source"

COORDS_KEY = "coords"

total_record_count = 0

# Setup logging
# =========================================================

logging.getLogger().setLevel(logging.NOTSET)
log = logging.getLogger('astrodb')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('astrodb.log')
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

log.addHandler(console_handler)
log.addHandler(file_handler)


# Process functions
# =========================================================

def get_table_from_file(fname: str, format: str, delim=None) -> fits.BinTableHDU:
    """
    Open a .fits or ascii source file
    :param fname: path to source file
    :param format: file format
    :param delim: delimiter for ascii file reading
    :return: An HDUList object
    """
    try:
        log.info("Opening '%s'" % fname)
        if format == "fits":
            return fits.open(fname, memmp=True)[1]
        else:
            args = [fname]
            kwargs = {}
            if format != "guess":
                kwargs["format"] = format
            if delim is not None:
                kwargs["delimiter"] = delim
            data = ascii.read(*args, **kwargs)
            return fits.BinTableHDU(data)
    except FileNotFoundError:
        log.error("File not found!")
        exit(1)
    except OSError:
        log.error("Something went wrong reading '%s'..." % fname)
        log.error("Are you sure this is a %s-format file?" % format)
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


def hdu_records(hdu_list: fits.BinTableHDU) -> fits.FITS_rec:
    """
    Generator function to yield each record in hdu_list
    :param hdu_list: HDUList object (containing data)
    :return: List of HDU records
    """
    return hdu_list.data


def generate_record(rec: fits.FITS_record, cols: fits.ColDefs) -> dict:
    """
    Returns a single dict object based on FITS records and columns.
    :param rec: FITS record to convert
    :param cols: List of column definitions
    :return: Dict object representing new record
             format: {<col1>: {'format':<format>, 'value':<value>}, <col2>: {...}, ...}
    """
    global args

    record_data = {SOURCE_KEY: args.src}
    coords = {"ra": {"min": None, "max": None}, "dec": {"min": None, "max": None}}
    for c in cols:
        # record[c.name] = {"value": rec[c.name], "unit": c.unit}
        try:
            record_data[c.name] = rec[c.name].item()
        except AttributeError:
            record_data[c.name] = rec[c.name]

        if c.name.lower() == "ra":
            if coords["ra"]["min"] is None or rec[c.name] < coords["ra"]["min"]:
                coords["ra"]["min"] = rec[c.name]
            if coords["ra"]["max"] is None or rec[c.name] > coords["ra"]["max"]:
                coords["ra"]["max"] = rec[c.name]
        elif c.name.lower() == "dec":
            if coords["dec"]["min"] is None or rec[c.name] < coords["dec"]["min"]:
                coords["dec"]["min"] = rec[c.name]
            if coords["dec"]["max"] is None or rec[c.name] > coords["dec"]["max"]:
                coords["dec"]["max"] = rec[c.name]

    return {DATA_KEY: [record_data], COORDS_KEY: coords}


def get_fits_columns(hdu_list: fits.BinTableHDU) -> fits.ColDefs:
    """
    Converts FITS columns to python "columns"
    :param hdu_list: HDUList object (containing columns)
    :return: Ordered list of dictionaries of the form
             {'name': <column-name>, 'format': <data-format-code>}
    """
    return hdu_list.columns


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


def upload_hdu_list(hdu_bin_table: fits.BinTableHDU,
                    collection: pymongo.collection) -> int:
    """
    Reads hdu_list, extracts data into dict "records", and
    uploads chunks of data to a mongo collection.
    :param hdu_bin_table: BinTableHDU object from which to read
    :param collection: Mongo collection to insert into
    :return: Number of records successfully written
    """
    global args, total_record_count

    record_buffer = []  # List of records (as dicts)
    inserted_record_count = 0  # Total number of records inserted thus far

    columns = get_fits_columns(hdu_bin_table)

    log.info('Generating records... ')
    hdu_record_list = hdu_records(hdu_bin_table)
    total_record_count = len(hdu_record_list)
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
                inserted_record_count, total_record_count,
                (inserted_record_count * 100) / total_record_count
            ))
            record_buffer = []

    # Upload remaining records
    inserted_record_count += insert_record_list(record_buffer, collection, args.sep)
    log.info("All %d/%d records uploaded!" % (inserted_record_count, total_record_count))

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
            record = merge_records(record, existing_record)
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
    final_record_list = []
    for new_record in list_of_records:
        # Generate mongo query to find objects whose coordinates are within the
        #    bounds of the threshold (a square area)
        #    (limits the number of comparisons while using actual coordinate matching)
        ra_min = new_record[COORDS_KEY]["ra"]["min"] - threshold
        ra_max = new_record[COORDS_KEY]["ra"]["max"] + threshold
        dec_min = new_record[COORDS_KEY]["dec"]["min"] - threshold
        dec_max = new_record[COORDS_KEY]["dec"]["max"] + threshold

        query = {
            "%s.%s.%s" % (COORDS_KEY, "ra", "min"): {"$lte": ra_max},
            "%s.%s.%s" % (COORDS_KEY, "ra", "max"): {"$gte": ra_min},
            "%s.%s.%s" % (COORDS_KEY, "dec", "min"): {"$lte": dec_max},
            "%s.%s.%s" % (COORDS_KEY, "dec", "max"): {"$gte": dec_min}
        }

        # Compare against existing records matching the query
        #   (since threshold is radius, not the length of a square)
        for existing_record in collection.find(query):
            if should_merge_by_distance(new_record, existing_record, threshold):
                new_record = merge_records(new_record, existing_record)

                collection.delete_one(existing_record)

        final_record_list.append(new_record)

    inserted_count = insert_records(collection, final_record_list)
    return inserted_count


def should_merge_by_distance(rec1: dict, rec2: dict, threshold: float) -> bool:
    """
    Compare two records by distance between the centers of each record's objects
    :param rec1: first record
    :param rec2: second record
    :param threshold: two records should be merged if their distance is
                      less than the threshold
    :return: True if records are close enough to be merged, False otherwise
    """
    med_ra1 = (rec1[COORDS_KEY]["ra"]["min"] + rec1[COORDS_KEY]["ra"]["max"]) / 2
    med_ra2 = (rec2[COORDS_KEY]["ra"]["min"] + rec2[COORDS_KEY]["ra"]["max"]) / 2
    med_dec1 = (rec1[COORDS_KEY]["dec"]["min"] + rec1[COORDS_KEY]["dec"]["max"]) / 2
    med_dec2 = (rec2[COORDS_KEY]["dec"]["min"] + rec2[COORDS_KEY]["dec"]["max"]) / 2

    coords1 = SkyCoord(ra=med_ra1 * u.degree, dec=med_dec1 * u.degree, distance=1, frame="icrs")
    coords2 = SkyCoord(ra=med_ra2 * u.degree, dec=med_dec2 * u.degree, distance=1, frame="icrs")

    sep = coords1.separation(coords2)
    log.debug("\tSeparation = {:.2f} (<{:.2f}, {:.2f}>, <{:.2f}, {:.2f}>)".format(
        sep.to(u.arcsec),
        coords1.ra.to(u.arcsec), coords1.ra.to(u.arcsec),
        coords2.dec.to(u.arcsec), coords2.dec.to(u.arcsec))
    )
    return sep.is_within_bounds(-threshold * u.arcsec, threshold * u.arcsec)


def merge_records(rec1: dict, rec2: dict) -> dict:
    """
    Merge two records, handling duplicate keys by appending a suffix
    :param rec1: first record
    :param rec2: second record
    :return: merged record
    """
    global total_record_count

    log.info("Merging records")

    new_rec = {
        DATA_KEY: rec1[DATA_KEY] + rec2[DATA_KEY],
        COORDS_KEY: {
            "ra": {
                "min": min(rec1[COORDS_KEY]["ra"]["min"], rec2[COORDS_KEY]["ra"]["min"]),
                "max": min(rec1[COORDS_KEY]["ra"]["max"], rec2[COORDS_KEY]["ra"]["max"]),
            },
            "dec": {
                "min": min(rec1[COORDS_KEY]["dec"]["min"], rec2[COORDS_KEY]["dec"]["min"]),
                "max": min(rec1[COORDS_KEY]["dec"]["max"], rec2[COORDS_KEY]["dec"]["max"]),
            }
        }
    }

    total_record_count -= 1

    return new_rec


# Main processing
# =========================================================

def main():
    """
    Open a fits file, read all records, and write to database in chunks
    """
    global args

    hdu_bin_table = get_table_from_file(fname=args.source_path, format=args.format, delim=args.delim)
    collection = get_collection(args.coll, args.db, args.uri)

    record_count = upload_hdu_list(hdu_bin_table, collection)

    log.info('Done!')

    log.info('Database successfully populated with {} records'.format(record_count))


# Argument parsing
# =========================================================

def allow_escape_chars(s: str) -> str:
    return s.encode("utf-8").decode("unicode_escape")


formats = [
    "aastex",
    "basic",
    "cds",
    "commented_header",
    "csv",
    "daophot",
    "ecsv",
    "fits",
    "fixed_width",
    "fixed_width_no_header",
    "fixed_width_two_line",
    "html",
    "ipac",
    "latex",
    "no_header",
    "rdb",
    "rst",
    "sextractor",
    "tab",
    "guess"
]

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 epilog="Valid options for FMT: "+(", ".join(formats)))

parser.add_argument('source_path')
parser.add_argument('-u', '--uri', help='MongoDB URI',
                    default=LOCAL_MONGO_URI)
parser.add_argument('-d', '--db', help='MongoDB database name')
parser.add_argument('-c', '--coll', help='MongoDB collection name')
parser.add_argument('-b', '--buffer', metavar="BUF", type=int, default=1,
                    help='Size of buffer of records. Will upload to database when buffer is full. '
                         '(Useful if you notice a speed increase by buffering more or fewer records).')
parser.add_argument('-s', '--sep', type=float, default=0.0,
                    help='Separation threshold under which objects are considered the same\n(units: arcseconds)')
parser.add_argument('--src', help="Optionally override file source for inserted records", required=False)
parser.add_argument('-f', '--format', metavar="FMT", choices=formats, default="fits",
                    help="Format of source file (fits or an ascii format)")
parser.add_argument('--delim', type=allow_escape_chars, default=None,
                    help="Delimiter used when parsing ascii files. "
                         "If not specified, will use file-format default")

args = parser.parse_args()

if args.src is None:
    args.src = args.source_path.split(os_path_sep)[-1]

if args.format == "guess":
    args.format = None

if __name__ == '__main__':
    main()
