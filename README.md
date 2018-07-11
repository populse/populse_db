# populse_db [![](https://travis-ci.org/populse/populse_db.svg?branch=master)](https://travis-ci.org/populse/populse_db) [![](https://codecov.io/github/populse/populse_db/coverage.svg?branch=master)](https://codecov.io/github/populse/populse_db) [![](https://img.shields.io/badge/license-CeCILL--B-blue.svg)](https://github.com/populse/populse_db/blob/master/LICENSE.en) [![](https://img.shields.io/pypi/v/populse_db.svg)](https://pypi.python.org/pypi/populse_db/) [![](https://img.shields.io/badge/python-3.3%2C%203.4%2C%203.5%2C%203.6-yellow.svg)](https://pypi.python.org/pypi/populse_db/) [![](https://img.shields.io/badge/platform-Linux%2C%20OSX-orange.svg)](#)

SQLAlchemy based database API for Populse

# Documentation

The documentation is available on populse_db's website here: [https://populse.github.io/populse_db](https://populse.github.io/populse_db)
	
# Installation

From source:

    sudo apt-get install git
    git clone https://github.com/populse/populse_db.git /tmp/populse_db
    cd /tmp/populse_db
    sudo python setup.py install
    cd /tmp
    sudo rm -r /tmp/populse_db

python/ directory must be added to $PYTHONPATH 

# Usage

	import populse_db                          ->  OK
	from populse_db.database import Database   ->  Wrong usage (Cyclic problems)
	import populse_db.database                 ->  Wrong usage (Cyclic problems)
	
# Tests

Unit tests written thanks to the python module unittest

Continuous integration made with Travis

Code coverage calculated by the python module codecov

The module is ensured to work with Python >= 3.3

The module is ensured to work on the platforms Linux and OSX (It is supposed to work on Windows, hasn't been tested yet)

# Launch the tests

Add the directory python/populse_db in $PATH, or open this directory inside a terminal
	
	python python/populse_db/test.py
	
# Requirements

* sqlalchemy
* lark-parser
* python-dateutil

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
  
  You can find all the details on the license [here](http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html), or refer to the license file [here](https://github.com/populse/populse_db/blob/master/LICENSE.en)
