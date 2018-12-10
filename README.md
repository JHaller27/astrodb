# AstroDB

AstroDB is a set of Python tools to interact with a database of astronomical data.
AstroDB uses the [astropy](http://www.astropy.org) Python module to read in data of astronomical objects from fits files, and store the object data in MongoDB.
Once the database is populated with data, more data can be added either as an additional and independent object, or as more information on existing data (using [coordinate matching](http://docs.astropy.org/en/stable/coordinates/) to determine related object data).


## Table of Contents

* [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [Credits](#credits)
* [License](#license)

## Installation

For more detailed information, see [wiki/Installation](wiki/Installation).

**Quick Start**

1. Clone or download the source code in this repository.
1. Setup a virtual environment using Python3.7
1. Install requirements (in `requirements.txt`) with pip

## Usage

For more detailed information, see [wiki/Usage](/wiki/Usage).

**Useful Commands**

* `astrodb$ . activate`
  * Every AstroDB session should begin with this command (if using a venv)
* `astrodb$ . deactivate`
  * This should happen automatically when you close the terminal, but it's useful to know if you want to continue coding in Python after running an AstroDB script.
* `astrodb/src$ ./<astrodb-tool>.py` or `astrodb/src$ python <astrodb-tool>.py` to run a script if using a venv.
* `astrodb/src$ python3.7 <astrodb-tool>.py` to run a script if not using a venv.


## Contributing

If you're thinking about contributing to this project, chances are that I am no longer mainting this project.
It is recommended that you fork this repository on GitHub and maintain your own version.

Note: If installing or using third-party python modules not already included in `requirements.txt`, export all dependencies to requirements file with `pip freeze > requirements.txt`. Do this ONLY if using a venv.

For more legal information on contributing see [license](#license).


## Credits

Credit for the concept and requirements goes to Dr. Jeyhan Kartaltepe (Rochester Institute of Technology).

Credit for development goes to me (James Haller, Rochester Institute of Technology).


## License

See separate [LICENSE](/LICENSE) file.

