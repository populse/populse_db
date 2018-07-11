.. populse_db documentation master file, created by
   sphinx-quickstart on Thu Jul  5 16:12:12 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.




Populse_db's documentation
======================================

Overview
-------------

* SQLAlchemy based database API for Populse
* It can support every database type, it takes an engine as entry
* Populse_db is ensured to work with Python >= 3.3
* Populse_db is ensured to work on the platforms linux and osx

Installation
-------------

* From source:

.. code-block:: python
   
   sudo apt-get install git
   git clone https://github.com/populse/populse_db.git /tmp/populse_db
   cd /tmp/populse_db
   sudo python setup.py install
   cd /tmp
   sudo rm -r /tmp/populse_db

* python/ directory must be added to $PYTHONPATH

Documentation
-------------

* This documentation website has been generated with Sphinx
* The source code of this website is in docs/ folder (The website is actually in docs/html/ folder, but docs/index.html is redirecting to the website)

* Generate the website (from populse_db root folder):

.. code-block:: python
   
   cd docs/
   make html
   cp -R build/doctrees/ ./doctrees/
   cp -R build/html/ ./html/
   rm -d -r build/html/
   rm -d -r build/doctrees/
   cd ..

.. toctree::
   :maxdepth: 2

Indices and tables
-------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

License
-------------

.. _cecillB: http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
.. _license: https://github.com/populse/populse_db/blob/master/LICENSE.en

The whole populse project is open source.

Populse_db is precisely released under the CeCILL-B software license.

You can find all the details on the license `here
<http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html>`_, or refer to the license file `here
<https://github.com/populse/populse_db/blob/master/LICENSE.en>`_

