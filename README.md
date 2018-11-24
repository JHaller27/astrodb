# AstroDB

## Rough Product Description

**Requirements**
* Include all available multi wavelength catalogs for the COSMOS and CANDELS (includes a portion of COSMOS, GOODS-N, GOODS-S, EGS, and UDS) fields
* Include optical, near-infrared, radio, Xray, etc.
  * includes photometric measurements as well as derived quantities (such as mass, photometric redshift, star formation rate, etc.)
* Include all available spectroscopic redshifts (and quality flags)
* Cross-match between catalogs to find ‘best’ counterparts for each source
* Allow user to find all sources within some specified radius
* Enable new datasets to be easily added over time
* Documentation

**Enhancements (optional)**    
* Potentially include postage stamp images
* Potentially include actual spectra, spectral line fits
* SED fits (image file)
* Morphological classifications
* Basic website as interface?

## General Knowledge

Test data: `COSMOS2015_Laigle+.fits.gz` from `ftp://ftp.iap.fr/pub/from_users/hjmcc/COSMOS2015`,
and some CANDELS data (fits files stored in a shared Google Drive folder).

AstroPy tutorials can be found [here](http://www.astropy.org/astropy-tutorials/FITS-tables.html).

Also of interest: [AstroPy coordinate tutorial](http://docs.astropy.org/en/stable/coordinates/).

Using [Google Sheet](https://docs.google.com/spreadsheets/d/1EYDZTCAMssnQXcbRf49nZOhDgYF5AcsNECnIOVaHyZ8/edit?usp=sharing)
to visualize data.

## Setup
Set up using a virtual environment...

  `astrodb$ python3.7 -m venv venv; ln -s venv/bin/activate activate`

While working with astrodb, make sure to first run the following command to activate the virtual environment...

  `astrodb$ . ./activate`

To exit the venv, use `astrodb$ deactivate`
(this happens automatically when the terminal is closed).

Install prerequisite packages with...

  `pip install -r requirements.txt`

### Binary Table Data Format

| FITS format code        | Description                    | 8-bit bytes |
|:------------------------|:-------------------------------|:------------|
| L                       | logical (Boolean)              | 1           |
| X                       | bit                            | *           |
| B                       | Unsigned byte                  | 1           |
| I                       | 16-bit integer                 | 2           |
| J                       | 32-bit integer                 | 4           |
| K                       | 64-bit integer                 | 4           |
| A                       | character                      | 1           |
| E                       | single precision floating point| 4           |
| D                       | double precision floating point| 8           |
| C                       | single precision complex       | 8           |
| M                       | double precision complex       | 16          |
| P                       | array descriptor               | 8           |
| Q                       | array descriptor               | 16          |

