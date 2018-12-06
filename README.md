# AstroDB

AstroDB is a set of Python tools to interact with a database of astronomical data.
AstroDB uses the [astropy](http://www.astropy.org) Python module to read in data of astronomical objects from fits files, and store the object data in MongoDB.
Once the database is populated with data, more data can be added either as an additional and independent object, or as more information on existing data (using [coordinate matching](http://docs.astropy.org/en/stable/coordinates/) to determine related object data).


## Table of Contents

* Installation
* Usage

* Rough Product Description
* General Knowledge

* Contributing
* Credits
* License


## Installation

### AstroDB

There are two main ways to setup AstroDB: using a virtual environment or using a global environment.
In either case, the first step is to clone this repository or download the source code.

You can place the files in any directory you want. These instructions will assume the directory is named `astrodb`.

All command line instructions in this file follow the form `<path>$ <command>`. `<path>` often will begin with `astrodb`
indicating the astrodb directory; make sure you are in this directory (or in the specified subdirectory).
If `<path>` is missing, then the location in which the command does not matter.
The `command` part is the text that should be entered into the console.

### Virtual Environment

_Recommended_

**Why use a virtual environment?**

A Python virtual environment ("venv") ensures the project's dependencies and requirements are managed independently.
If a required package's version changes in a way that is required for another project but that breaks AstroDB, a venv will ensure that AstroDB is unaffected by these sorts of issues.

**Virutal environment setup**

From the `astrodb` directory, run...

  `astrodb$ python3.7 -m venv venv && ln -s venv/bin/activate activate && . activate`

Install prerequisite packages with...

  `astrodb$ pip install -r requirements.txt`


### Global Environment

In a global environment, all modules installed via pip will be available to ALL python projects.
When modules are thus updated, ALL python projects relying on those modules will use the new version.
If you are new to python, this approach is easier to understand.

**Global environment setup**

Install prerequisite packages with...

  `$ pip3 install -r requirements.txt`


## Usage

### With a Virtual Environment setup

Whenever running a tool in AstroDB via command line, make sure to first run the following command to activate the virtual environment...

  `astrodb$ . activate`

To exit the venv, use...

  `astrodb$ deactivate`

(this also happens automatically when the terminal is closed).

To run a tool, use...

  `astrodb/src$ ./<astrodb-tool>.py` or `astrodb/src$ python <astrodb-tool>.py`

### With a Global Environement setup

To run a tool, use...

  `astrodb/src$ python3.7 <astrodb-tool>.py`


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

