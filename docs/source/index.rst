.. populse_db documentation master file, created by
   sphinx-quickstart on Thu Jul  5 16:12:12 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::

   documentation

Generalities
============

Overview
--------

* SQLAlchemy based database API for Populse
* It can support every database type, it takes an engine as entry
* Populse_db is ensured to work with Python >= 3.3
* Populse_db is ensured to work on the platforms Linux and OSX (It is supposed to work on Windows, hasn't been tested yet)

Requirements
------------

The modules required for populse_db are the following ones:

* sqlalchemy
* lark-parser
* python-dateutil

Other packages used
-------------------

The other packages used by populse_db are the following ones:

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

Documentation
-------------

* This documentation website has been generated with Sphinx
* The source code of this website is in docs/ directory (The website is actually in docs/html/ directory, but docs/index.html is redirecting to the website)

* Generate the website (from populse_db root directory):

.. code-block:: python
   
   cd docs/
   make html
   cp -R build/doctrees/ ./doctrees/
   cp -R build/html/ ./html/
   rm -d -r build/html/
   rm -d -r build/doctrees/
   cd ..

License
-------

The whole populse project is open source.

Populse_db is precisely released under the CeCILL-B software license.

You can find all the details on the license `here
<http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html>`_, or refer to the license file `here
<https://github.com/populse/populse_db/blob/master/LICENSE.en>`_.

