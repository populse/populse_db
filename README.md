# populse_db [![Build Status](https://travis-ci.org/populse/populse_db.svg?branch=master)](https://travis-ci.org/populse/populse_db) [![codecov.io](https://codecov.io/github/populse/populse_db/coverage.svg?branch=master)](https://codecov.io/github/populse/populse_db) [![Licence](https://img.shields.io/github/license/populse/populse_db.svg)](https://github.com/populse/populse_db/blob/master/LICENSE.md) [![PyPi version](https://img.shields.io/pypi/v/populse_db.svg)](https://pypi.python.org/pypi/populse_db/) [![Python versions](https://img.shields.io/pypi/pyversions/populse_db.svg)](https://pypi.python.org/pypi/populse_db/)

SQLAlchemy based database API for Populse

# Website

The documentation is available on populse_db's website here: [https://populse.github.io/populse_db](https://populse.github.io/populse_db)

# Tools

The API can support every database type, it takes an engine as entry

The database is managed thanks to the ORM SQLAlchemy

# Relational schema
![alt text](docs/pictures/schema.png "Relational schema")

Type {string, integer, float, date, datetime, time, json, list_string, list_integer, list_float, list_date, list_datetime, list_time, list_json}
	
# Installation

python directory must be added to $PYTHONPATH 

# Import

	import populse_db
	
# Tests

Unit tests written thanks to the python module unittest

The module is ensure to work with Python >= 3.3

# Launch the tests

Add the directory python/populse_db in $PATH, or open this directory inside a terminal
	
	python python/populse_db/test.py
	
# Dependencies

* sqlalchemy
* lark-parser
* python-dateutil

Other packages used:
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
