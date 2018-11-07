#!/usr/bin/python3

from astropy.io import fits

import os
import sys

fdir = sys.argv[1] if len(sys.argv) >= 2 else '/mnt/d/astronomy_research'
fname = 'pdz_cosmos2015_v1.3.fits'

hdu_list = fits.open(os.path.join(fdir, fname), memmp=True)

if __name__ == '__main__':
    print(hdu_list[1].columns)
else:
    print('hdu_list ready!')
