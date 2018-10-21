#!/usr/bin/python3

from astropy.io import fits
import os
import sqlite3
import sys


DB_NAME = 'astro.db'
TBL_NAME = 'fits_test'

FITS_FILE, FITS_DIR = 'pdz_cosmos2015_v1.3.fits', '/media/james/TIBERIUS/astronomy_research'

CONN_LOCATION = os.path.join(FITS_DIR, 'sqlite', DB_NAME)

DEBUG = False

FITS2SQLITE_DATATYPES = {
    # SQLite types: NULL, INTEGER, REAL, TEXT, BLOB
    'L': 'INTEGER',  # Boolean
    'X': 'INTEGER',  # Bit
    'B': 'INTEGER',  # Unigned byte
    'I': 'INTEGER',  # 16-bit int
    'J': 'INTEGER',  # 32-bit int
    'K': 'INTEGER',  # 64-bit int
    'A': 'TEXT',     # Character
    'E': 'REAL',     # Single float
    'D': 'REAL',     # Double float
    'C': 'TEXT',     # Single complex
    'M': 'TEXT',     # Double complex
    'Q': 'TEXT'     # Array descriptor
}


def open_fits(fdir=FITS_DIR, fname=FITS_FILE):
    fname = fname.lstrip(os.path.sep)
    fdir = fdir.rstrip(os.path.sep)
    return fits.open(os.path.join(fdir, fname), memmp=True)


def get_table(cols):
    conn = sqlite3.connect(CONN_LOCATION)
    print('Opened database at ' + CONN_LOCATION)

    c = conn.cursor()

    # Drop table
    try:
        c.execute('DROP TABLE %s' % TBL_NAME)
        print('Dropping pre-existing table %s' % TBL_NAME)
    except sqlite3.OperationalError:
        print('No table %s found' % TBL_NAME)

    # Create table
    create_table_start = 'CREATE TABLE %s' % TBL_NAME
    #     Using %s is dangerous if fits column name is sql injection attack. Not considered significant risk
    table_cols = ', '.join(['%s %s' % (c['name'], FITS2SQLITE_DATATYPES[c['format']]) for c in cols])

    try:
        c.execute('%s (%s)' % (create_table_start, table_cols))
        print('New database created')
    except sqlite3.OperationalError:
        print('Existing database found')

    return conn, c


def generate_record(rec, cols):
    # record: (<value1>, <value2>, ...)
    rec_list = []
    for c in cols:
        data = rec[c['name']]
        data_type = FITS2SQLITE_DATATYPES[c['format']]
        if data_type == 'INTEGER':
            data = int(data)
        elif data_type == 'REAL':
            data = float(data)
        else:
            data = str(data)
        rec_list.append(data)
    return tuple(rec_list)


def get_fits_columns(cols):
    return [{'name': c.name, 'format': str(c.format)} for c in cols]


def insert_records(cursor, record_list):
    print('Inserting {} records... '.format(len(record_list)), end='')
    sys.stdout.flush()

    cursor.executemany('INSERT INTO %s VALUES (%s)' % (TBL_NAME, ', '.join(['?' for i in range(len(record_list[0]))])),
                       record_list)


if __name__ == '__main__':
    record_list = []
    record_count = 0
    with open_fits(FITS_DIR, FITS_FILE) as hdu_table:
        increment = 100

        cols = get_fits_columns(hdu_table[1].columns)

        print('Requesting table... ', end='')
        conn, cursor = get_table(cols)
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
                insert_records(cursor, record_list)
                conn.commit()
                record_list = []

                print('\t{} records left'.format(len(hdu_table[1].data) - record_count))
                conn.close()
                exit()

            i += 1

        insert_records(cursor, record_list)

        print('Done!')
        sys.stdout.flush()

    print('Database successfully populated')
