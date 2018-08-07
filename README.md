# populse_db                                                                                                                         [![](https://travis-ci.org/populse/populse_db.svg?branch=master)](https://travis-ci.org/populse/populse_db)                                         [![Build status](https://ci.appveyor.com/api/projects/status/exyixo4o0osns6tn?svg=true)](https://ci.appveyor.com/project/ouvrierl/populse-db-6gndf)                                                                                                                       [![](https://codecov.io/github/populse/populse_db/coverage.svg?branch=master)](https://codecov.io/github/populse/populse_db) [![](https://img.shields.io/badge/license-CeCILL--B-blue.svg)](https://github.com/populse/populse_db/blob/master/LICENSE.en) [![](https://img.shields.io/pypi/v/populse_db.svg)](https://pypi.python.org/pypi/populse_db/)                                           [![](https://img.shields.io/badge/python-2.7%2C%203.3%2C%203.4%2C%203.5%2C%203.6%2C%203.7-yellow.svg)](#)                                                                      [![](https://img.shields.io/badge/platform-Linux%2C%20OSX%2C%20Windows-orange.svg)](#)

SQLAlchemy based database API for Populse

# Documentation

The documentation is available on populse_db's website here: [https://populse.github.io/populse_db](https://populse.github.io/populse_db)
	
# Installation

From source:

    # A compatible version of Python must be installed 
    sudo apt-get install git
    git clone https://github.com/populse/populse_db.git /tmp/populse_db
    cd /tmp/populse_db
    sudo python setup.py install # Beware that it is the good Python version
    cd /tmp
    sudo rm -r /tmp/populse_db

# Usage

	import populse_db                          
	from populse_db.database import Database   
	import populse_db.database                 
	
# Tests

Unit tests written thanks to the python module unittest

Continuous integration made with Travis (Linux, OSX), and AppVeyor (Windows)

Code coverage calculated by the python module codecov

The module is ensured to work with Python 2.7 and Python >= 3.3

The module is ensured to work on the platforms Linux, OSX and Windows

The script of tests is python/populse_db/test.py, so the following command launches the tests:
	
	python test.py (if python/populse_db/ directory has been added to $PATH, or if $PWD in the terminal)
        
	python python/populse_db/test.py (from populse_db root folder)
	
# Requirements

* sqlalchemy
* lark-parser
* python-dateutil
* sphinx
* psycopg2

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
