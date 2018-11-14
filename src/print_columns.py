#!../venv/bin/python
# Author: James Haller

import sys
from astropy.io import fits


FORMAT_NAMES = {
    'L': 'bool',
    'X': 'bit',
    'B': 'ubyte',
    'I': 'int',
    'J': 'long',
    'K': 'long long',
    'A': 'char',
    'E': 'float',
    'D': 'double',
    'C': 'complex',
    'M': 'double complex',
    'P': 'arr',
    'Q': 'double arr'
}


def fmt2str(fmt):
    return fmt if fmt not in FORMAT_NAMES else FORMAT_NAMES[fmt]


def print_columns(filename):
    try:
        with fits.open(filename, memmap=True) as hdu_list:
            columns = hdu_list[1].columns

        # Determine column widths
        col_widths = {'name': len('name'), 'format': len('format')}
        for col in columns:
            col_widths['name'] = max(col_widths['name'], len(col.name))
            col_widths['format'] = max(col_widths['format'], len(fmt2str(col.format)))

        # Print table
        fmt = '{:>{}} | {:<{}}'

        #   Table header / separator
        print(fmt.format(
            'name', col_widths['name'],
            'format', col_widths['format']
        ))
        print(fmt.format(
            '-' * col_widths['name'], col_widths['name'],
            '-' * col_widths['format'], col_widths['format']
        ))

        #   Table body
        for col in columns:
            print(fmt.format(
                col.name, col_widths['name'],
                fmt2str(col.format), col_widths['format']
            ))
    except FileNotFoundError as fnfe:
        print(fnfe, file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        print_columns(sys.argv[1])
    else:
        print('Usage: {} FILENAME'.format(sys.argv[0]), file=sys.stderr)
    exit()
