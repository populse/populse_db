.. toctree::

Tutorial
========

This tutorial illustrate the main populse_db features with code examples.
It is the good place to start learnig populse_db.

Installation
============
Populse_db can be installed, using `the standard Python package manager 
<https://pip.pypa.io/>`_, either from source code of from release hosted
on `Python Package Index <https://pypi.org/>`_. 

Install globally
----------------
If you already have Python version 3.9 or greater, you can simply use::

   pip install populse_db

You will then be be able to run the tutorial codes with the following command::

    python -m populse_db.tutorial_01.py

Install in virtual environement
-------------------------------

If you have the right Python version and want to install populse_db
in its own isolated directory, you can use a `virtual environment 
<https://docs.python.org/3/tutorial/venv.html>`_::

    python -m venv $HOME/populse_db_venv
    $HOME/populse_db_venv/bin/pip install populse_db

You will then be be able to run the tutorial codes with the following command::
    
    $HOME/populse_db/bin/python -m populse_db.tutorial_01.py

Install using another Python
----------------------------

If your system does not have the appropriate Python version, you can use a 
container technology and a `virtual environment 
<https://docs.python.org/3/tutorial/venv.html>`_ to have an install entirely
independant of your system. For this, we recommend to use Singularity. Once
Singularity is installed on your system you can install the environment and
populse_db with the following commands::

    # get latest Python
    singularity pull --disable-cache $HOME/python.sif docker://python 

    # Create virtualenv
    $HOME/python.sif python -m venv $HOME/populse_db_venv

    # Install populse_db
    $HOME/python.sif $HOME/populse_db_venv/bin/pip install populse_db

You will then be be able to run the tutorial codes with the following command::
    
    $HOME/python.sif $HOME/populse_db_venv/bin/python -m populse_db.tutorial_01.py

Install options
---------------

By default, only SQL backend is supported. If one wants to use Postgresql
database, use the following install option::

   pip install populse_db[postgres]

The documentation is embedded in the releases. However, if one needs to rebuild
the documentation from sources, use::

   pip install populse_db[doc]

Connection to database
======================

..include:: ../../python/populse_db/tutorial/tutorial_01.py
    :literal:

Data storage
============


Document fields definition
==========================


Query documents
===============

Transactions
============
