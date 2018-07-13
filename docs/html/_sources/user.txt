.. toctree::

   index

User documentation
==================

Installation
------------

* From source:

.. code-block:: python
   
   sudo apt-get install git
   git clone https://github.com/populse/populse_db.git /tmp/populse_db
   cd /tmp/populse_db
   sudo python setup.py install
   cd /tmp
   sudo rm -r /tmp/populse_db

* python/ directory must be added to $PYTHONPATH

Usage
-----

Import examples:

.. code-block:: python
   
   import populse_db
   from populse_db.database import Database
   import populse_db.database

Tests
-----

* Unit tests have been written with the package unittest
* Continuous integration has been deployed with Travis
* The code coverage is generated with the package codecov
* The script of tests is python/populse_db/test.py, so the following command launches the tests:

.. code-block:: python
   
   python test.py (if python/populse_db/ directory has been added to $PATH, or if $PWD in the terminal)
   python python/populse_db/test.py (from populse_db root directory)
