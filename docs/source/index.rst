.. populse_db documentation master file, created by
   sphinx-quickstart on Thu Jul  5 16:12:12 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::


+----------------------------+--------------------------------------------------------------+--------------------------------------------------+
|`Index <./index.html>`_     |`Documentation <./documentation.html>`_                       |`GitHub <https://github.com/populse/populse_db>`_ |
+----------------------------+--------------------------------------------------------------+--------------------------------------------------+


Introduction
============

populse_db is an open source Python module that sits in front of a database 
(`SQLite <https://sqlite.org>`_ or `Postgres <https://www.postgresql.org>`_)
and allows the storage and query of JSON documents. It brings together
features from SQL and NOSQL databases and can be used without any database
server thanks to its SQLite backend.

Features
--------

* **No server required**: populse_db can be used with a database stored in a
  simple file or even in memory. This feature is simply a consequence of
  the possibility to use a SQLite database backend.
* **No schema required**: populse_db can be used without thinking about
  database schema. But it is also possible to define some pieces of schema
  or to use an existing one to further optimize query performances.
* **Date and time support**: populse_db support items of type `datetime.time`,
  `datetime.date` and `datetime.datetime` in JSON objects. These items are
  automatically encoded (resp. decoded) to (resp. from) strings using ISO
  format. 
* **Query list elements**: populse_db has a simple query language that
  allows to select objects by looking for items in list fields.
* **Transactions support**: to ensure the consistency of the database, 
  populse_db allows all modifications to be made within a transaction.
  Thus, in case of unexpected problem, the database will keep its initial
  state before the transaction. 
* **Partitioning of JSON objects**: populse_db allows to read or modify
  individually each stored object. But it also allows to define a
  partitioning of the objects offering a direct access to different parts of
  the same object.

Dependencies
------------

Install tools take care of all the required dependencies for you but here is a
summary of what populse_db needs:

* Python >= 3.9. populse_db uses some features introduced in Python 3.9 (such
  as the possibility to subscript list type as in ``list[str]``). Therefore it
  can't work with Python 3.8 or earlier releases.
* `dateutil <https://dateutil.readthedocs.io/>`_
* `Lark-parser <https://github.com/lark-parser/lark/>`_ >= 0.7.0.
* *[optional]* `Psycopg2 <https://www.psycopg.org/>`_ allows the connection to
  a Postgresql database.
* *[optional]* `Sphinx <https://www.sphinx-doc.org/>`_ allows to build the
  documentation from source code.

Get started
============

Installation
-------------
Populse_db can be installed by standard Python tool. For instance::

   pip install populse_db

By default, only SQL backend is supported. If one wants to use Postgresql
database, use the following command::

   pip install populse_db[postgres]

The documentation is embedded in the project. However, if one needs to rebuild
the documentation, use::

   pip install populse_db[doc]


Basic usage
-----------

Populse_db is organized in *collections* and *documents*. A document is a JSON
object represented by a dictionary in Python. A collection is a named container
in which documents can be stored. Internally, collections correponds to
database tables and documents are stored in table rows (one document per row).
But  populse_db can hide this internal database stuff. The only thing user
must do isto declare its collections if it uses an empty database.

::

   from datetime import date
   from populse_db import Database
   from pprint import pprint

   # Connect to a new in memory database using SQLite
   with Database(':memory:') as db:
      # Since database is empty, declare collections that
      # will be used to store documents.
      db.add_collection('subject')
      db.add_collection('acquisition')

      # Store documents in the database
      db['subject']['rbndt001'] = {
         'name': 'Eléa',
         'sex': 'f',
         'birth_date': date(1968, 3, 3),
      }
      db['subject']['rbndt002'] = {
         'name': 'Païkan',
         'sex': 'm',
         'birth_date': date(1963, 12, 7),
      }
      db['acquisition']['rbndt001_t1'] = {
         'subject': 'rbndt001',
         'type': ['image', 'mri', 'T1'],
         'format': 'DICOM',
         'files': [
               '/somewhere/t1/acq0001.dcm',
               '/somewhere/t1/acq0002.dcm',
               '/somewhere/t1/acq0003.dcm',
               '/somewhere/t1/acq0004.dcm',
         ],
         'date': date(2022, 3, 28),
      }
      db['acquisition']['rbndt001_t2'] = {
         'subject': 'rbndt001',
         'type': ['image', 'mri', 'T2'],
         'format': 'DICOM',
         'files': [
               '/somewhere/t2/acq0001.dcm',
               '/somewhere/t2/acq0002.dcm',
               '/somewhere/t2/acq0003.dcm',
               '/somewhere/t2/acq0004.dcm',
         ],
         'date': date(2022, 3, 28),
      }
      db['acquisition']['rbndt002_t1'] = {
         'subject': 'rbndt002',
         'type': ['image', 'mri', 'T1'],
         'format': 'NIFTI',
         'files': [
               '/elsewhere/sub-rbndt001.nii',
         ],
         'date': date(2022, 3, 29),
      }

      # Retrieve a single document from collection 'subject'
      pprint(db['subject']['rbndt001'])

      # Retrieve all documents from collection 'subject' respecting the
      # following conditions:
      #   - the "subject" field equals to "rbndt001"
      #   - the "type" field is a list containing tha value "T1"
      for doc in db['acquisition'].filter('subject=="rbndt001" and "T1" in type'):
         pprint(doc)

The example above illustrate how to use populse_db to store and retrieve JSON
objects without wondering about the underlying SQL database engine. This script
produces the following result::

   {'birth_date': datetime.date(1968, 3, 3),
   'name': 'Eléa',
   'primary_key': 'rbndt001',
   'sex': 'f'}
   {'date': datetime.date(2022, 3, 28),
   'files': ['/somewhere/t1/acq0001.dcm',
            '/somewhere/t1/acq0002.dcm',
            '/somewhere/t1/acq0003.dcm',
            '/somewhere/t1/acq0004.dcm'],
   'format': 'DICOM',
   'primary_key': 'rbndt001_t1',
   'subject': 'rbndt001',
   'type': ['image', 'mri', 'T1']}





Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
