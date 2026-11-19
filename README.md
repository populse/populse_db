# populse_db
[//]: [![](https://travis-ci.org/populse/populse_db.svg?branch=master)](https://travis-ci.org/populse/populse_db)
[![Build status](https://img.shields.io/appveyor/build/populse/populse-db-6gndf/master.svg)](https://ci.appveyor.com/project/populse/populse-db-6gndf/branch/master)
[![codecov](https://codecov.io/gh/populse/populse_db/branch/master/graph/badge.svg?token=sTgorGiZ7w)](https://codecov.io/gh/populse/populse_db)
[![](https://img.shields.io/badge/license-CeCILL--B-blue.svg)](https://github.com/populse/populse_db/blob/master/LICENSE)
[![](https://img.shields.io/pypi/v/populse_db.svg)](https://pypi.python.org/pypi/populse_db/)
[![](https://img.shields.io/pypi/status/populse_db.svg)](https://pypi.python.org/pypi/populse_db/)
[![](https://img.shields.io/badge/python-3.10%203.11%203.12-yellow.svg)](#)
[![](https://img.shields.io/badge/platform-Linux%2C%20OSX%2C%20Windows-orange.svg)](#)

Database API for Populse

# Documentation

[The documentation is available on populse_db's website](https://populse.github.io/populse_db)

# Installation

From [PyPI](https://pypi.org/project/populse-db/):

    - A compatible version of Python and Pip must be installed

    - To install pip:
        sudo apt-get install python3-pip

    - To install the module:
        python3 -m pip install populse-db

        - Or, using a specific Python version:
            python3.x -m pip install populse-db

From source:

    - A compatible version of Python must be installed:
        sudo apt-get install git

    - Clone from GitHub:
        git clone https://github.com/populse/populse_db.git /tmp/populse_db
        cd /tmp/populse_db

    - Install the module:
        python3 -m pip install .

        - Or, using a specific Python version:
	    python3.x -m pip install .

    - Delete the tempory data:
        cd /tmp
        rm -rf /tmp/populse_db

# Usage

	import populse_db
	from populse_db.database import Database
	import populse_db.database

# Tests

Unit tests written thanks to the python module unittest

Continuous integration made with Travis (Linux, OSX), and AppVeyor (Windows)

Code coverage calculated by the python module codecov

The module is ensured to work with Python >= 3.10

The module is ensured to work on the platforms Linux, OSX and Windows

The script of tests is python/populse_db/test.py, so the following command launches the tests:

	python test.py (if python/populse_db/ directory has been added to $PATH, or if $PWD in the terminal)

	python python/populse_db/test.py (from populse_db root folder)

	python -m populse_db.test

# Requirements

* lark-parser
* python-dateutil
* sphinx
* psycopg2
* SQLite version > 3.38.0

# Other packages used

  * ast
  * copy
  * datetime
  * hashlib
  * operator
  * os
  * re
  * six
  * tempfile
  * types
  * unittest

  # License

  The whole populse project is open source

  Populse_db is precisely released under the CeCILL-B software license

  You can find all the details on the license [here](http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html), or refer to the license file [here](https://github.com/populse/populse_db/blob/master/LICENSE)
