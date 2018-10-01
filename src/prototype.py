#!/usr/bin/python3

from astropy.io import fits
import os


def open_fits(fname, fdir='.'):
    fname = fname.lstrip(os.path.sep)
    fdir = fdir.rstrip(os.path.sep)
    return fits.open(os.path.join(fdir, fname), memmp=True)


def get_cols(tbl):
    return tbl.columns


if __name__ == '__main__':
    with open_fits('pdz_cosmos2015_v1.3.fits', '/mnt/d/astronomy_research') as hdu_table:
        print(get_cols(hdu_table[1]))

