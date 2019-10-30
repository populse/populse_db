##########################################################################
# Populse_db - Copyright (C) IRMaGe/CEA, 2018
# Distributed under the terms of the CeCILL-B license, as published by
# the CEA-CNRS-INRIA. Refer to the LICENSE file or to
# http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
# for details.
##########################################################################

import ast
import copy
import json
import hashlib
import os
import re
import types
from datetime import date, time, datetime

import dateutil.parser
import six

#from sqlalchemy import (create_engine, Column, MetaData, Table, sql,
                        #String, Integer, Float, Boolean, Date, DateTime,
                        #Time, Enum, event)
#from sqlalchemy.ext.automap import automap_base
#from sqlalchemy.orm import sessionmaker, scoped_session, mapper
#from sqlalchemy.schema import CreateTable, DropTable
#from sqlalchemy.exc import ArgumentError

import populse_db

# Field types
FIELD_TYPE_STRING = "string"
FIELD_TYPE_INTEGER = "int"
FIELD_TYPE_FLOAT = "float"
FIELD_TYPE_BOOLEAN = "boolean"
FIELD_TYPE_DATE = "date"
FIELD_TYPE_DATETIME = "datetime"
FIELD_TYPE_TIME = "time"
FIELD_TYPE_JSON = "json"
FIELD_TYPE_LIST_STRING = "list_string"
FIELD_TYPE_LIST_INTEGER = "list_int"
FIELD_TYPE_LIST_FLOAT = "list_float"
FIELD_TYPE_LIST_BOOLEAN = "list_boolean"
FIELD_TYPE_LIST_DATE = "list_date"
FIELD_TYPE_LIST_DATETIME = "list_datetime"
FIELD_TYPE_LIST_TIME = "list_time"
FIELD_TYPE_LIST_JSON = "list_json"

LIST_TYPES = [FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER, FIELD_TYPE_LIST_FLOAT,
              FIELD_TYPE_LIST_BOOLEAN, FIELD_TYPE_LIST_DATE, FIELD_TYPE_LIST_DATETIME, FIELD_TYPE_LIST_TIME,
              FIELD_TYPE_LIST_JSON]
SIMPLE_TYPES = [FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT,
                FIELD_TYPE_BOOLEAN, FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME, FIELD_TYPE_JSON]
ALL_TYPES = [FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER, FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN,
             FIELD_TYPE_LIST_DATE, FIELD_TYPE_LIST_DATETIME,
             FIELD_TYPE_LIST_TIME, FIELD_TYPE_LIST_JSON, FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT,
             FIELD_TYPE_BOOLEAN, FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME, FIELD_TYPE_JSON]

TYPE_TO_COLUMN = {}
TYPE_TO_COLUMN[FIELD_TYPE_INTEGER] = Integer
TYPE_TO_COLUMN[FIELD_TYPE_LIST_INTEGER] = Integer
TYPE_TO_COLUMN[FIELD_TYPE_FLOAT] = Float
TYPE_TO_COLUMN[FIELD_TYPE_LIST_FLOAT] = Float
TYPE_TO_COLUMN[FIELD_TYPE_BOOLEAN] = Boolean
TYPE_TO_COLUMN[FIELD_TYPE_LIST_BOOLEAN] = Boolean
TYPE_TO_COLUMN[FIELD_TYPE_DATE] = Date
TYPE_TO_COLUMN[FIELD_TYPE_LIST_DATE] = Date
TYPE_TO_COLUMN[FIELD_TYPE_DATETIME] = DateTime
TYPE_TO_COLUMN[FIELD_TYPE_LIST_DATETIME] = DateTime
TYPE_TO_COLUMN[FIELD_TYPE_TIME] = Time
TYPE_TO_COLUMN[FIELD_TYPE_LIST_TIME] = Time
TYPE_TO_COLUMN[FIELD_TYPE_STRING] = String
TYPE_TO_COLUMN[FIELD_TYPE_LIST_STRING] = String
TYPE_TO_COLUMN[FIELD_TYPE_JSON] = String
TYPE_TO_COLUMN[FIELD_TYPE_LIST_JSON] = String

# Table names
FIELD_TABLE = "field"
COLLECTION_TABLE = "collection"


class Database:
    """
    Database API

    attributes:
        - string_engine: String engine of the database
        - caches: Bool to know if the caches must be used
        - list_tables: Bool to know if list tables must be used
        - query_type: Default query implementation for applying the filters
        - engine: SQLAlchemy database engine

    methods:
        - __enter__: Creates or gets a DatabaseSession instance
        - __exit__: Releases the latest created DatabaseSession
        - clear: Clears the database

    """

    def __init__(self, string_engine, caches=False, list_tables=True,
                 query_type='mixed'):
        """Initialization of the database

        :param string_engine: Database engine

                              The engine is constructed this way: dialect[+driver]://user:password@host/dbname[?key=value..]

                              The dialect can be mysql, oracle, postgresql, mssql, or sqlite

                              The driver is the name of a DBAPI, such as psycopg2, pyodbc, or cx_oracle

                              For sqlite databases, the file can be not existing yet, it will be created in this case

                              Examples:
                                        - "mysql://scott:tiger@hostname/dbname"
                                        - "postgresql://scott:tiger@localhost/test"
                                        - "sqlite:///foo.db"
                                        - "oracle+cx_oracle://scott:tiger@tnsname"
                                        - "mssql+pyodbc://scott:tiger@mydsn"

                              See sqlalchemy documentation for more precisions about the engine: http://docs.sqlalchemy.org/en/latest/core/engines.html

        :param caches: Bool to know if the caches (the rows of the database are stored in dictionaries) must be used (Put True if you count on having a lot of data) => False by default

        :param list_tables: Bool to know if tables must be created to store list values (Put True to have a pure SQL version of IN operator in filters) => True by default

        :param query_type: Type of query to use for the filters ('sql', 'python', 'mixed', or 'guess') => 'mixed' by default

        :raise ValueError: - If string_engine is invalid
                           - If caches is invalid
                           - If list_tables is invalid
                           - If query_type is invalid
                           - If the schema is not coherent with the API (the database is not a populse_db database)
        """

        self.string_engine = string_engine
        if not isinstance(string_engine, six.string_types):
            raise ValueError(
                "Wrong string_engine, it must be of type {0}, but string_engine of type {1} given".format(str, type(string_engine)))
        if not isinstance(caches, bool):
            raise ValueError(
                "Wrong caches, it must be of type {0}, but caches of type {1} given".format(bool, type(caches)))
        self.caches = caches
        if not isinstance(list_tables, bool):
            raise ValueError("Wrong list_tables, it must be of type {0}, but list_tables of type {1} given".format(bool,
                                                                                                                   type(
                                                                                                                       list_tables)))
        self.list_tables = list_tables
        query_list = [populse_db.filter.QUERY_MIXED, populse_db.filter.QUERY_GUESS, populse_db.filter.QUERY_PYTHON,
                      populse_db.filter.QUERY_SQL]
        if query_type not in query_list:
            raise ValueError("Wrong query_type, it must be in {0}, but {1} given".format(query_list, query_type))
        self.query_type = query_type

        # SQLite database: It is created if it does not exist
        if string_engine.startswith('sqlite:///'):
            self.sqlite_location = re.sub("sqlite.*:///", "", string_engine)
            self.init_sqlite()
        
        self.engine = None


    def __enter__(self):
        '''
        Return a DatabaseSession instance for using the database. This is
        supposed to be called using a "with" statement:
        
        with database as session:
           session.add_document(...)
           
        Therefore __exit__ must be called to get rid of the session.
        When called recursively, the underlying database session returned
        is the same. The commit/rollback of the session is done only by the
        outermost __enter__/__exit__ pair (i.e. by the outermost with
        statement).
        '''
        if self.engine is None:
            self.engine = SQLiteEngine(self.sqlite_location)
            self.engine_count = 1
        else:
            self.engine_count += 1
    
        return DatabaseSession(self)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        Release a DatabaseSession previously created by __enter__.
        If no recursive call of __enter__ was done, the session
        is commited if no error is reported (e.g. exc_type is None)
        otherwise it is rolled back. Nothing is done 
        '''
        self.engine_count -= 1
        if self.engine_count == 0:
            # If there is no recursive call, commit or rollback
            # the session according to the presence of an exception
            if exc_type is None:
                try:
                    self.engine.commit()
                except:
                    self.engine.rollback()
                    raise
            else:
                self.engine.rollback()
                #six.reraise(exc_type, exc_val, exc_tb)
            self.sqlite_delete_engine(self)
            
    def clear(self):
        """
        Removes all documents and collections in the database
        """
        with self as cursor:
            cursor.delete_tables()


class DatabaseSession:
    """
    DatabaseSession API

    attributes:
        - database: Database instance
        - session: Session related to the database
        - table_classes: List of all table classes, generated automatically
        - base: Database base
        - metadata: Database metadata

    methods:
        - add_collection: Adds a collection
        - remove_collection: Removes a collection
        - get_collection: Gives the collection row
        - get_collections: Gives all collection rows
        - get_collections_names: Gives all collection names
        - add_field: Adds a field to a collection
        - add_fields: Adds a list of fields to a collection
        - remove_field: Removes a field from a collection
        - get_field: Gives all fields rows given a collection
        - get_fields_names: Gives all fields names given a collection
        - name_to_valid_column_name: Gives the valid table/column name corresponding
          to the name
        - get_value: Gives the value of <collection, document, field>
        - set_value: Sets the value of <collection, document, field>
        - set_values: Sets several values of <collection, document, field>
        - remove_value: Removes the value of <collection, document, field>
        - add_value: Adds a value to <collection, document, field>
        - get_document: Gives the document row given a document name and a collection
        - get_documents: Gives all document rows given a collection
        - get_documents_names: Gives all document names given a collection
        - add_document: Adds a document to a collection
        - remove_document: Removes a document from a collection
        - save_modifications: Saves the pending modifications
        - unsave_modifications: Unsaves the pending modifications
        - has_unsaved_modifications: To know if there are unsaved
          modifications
        - filter_documents: Gives the list of documents matching the filter
    """

    # Some types (e.g. time, date and datetime) cannot be
    # serialized/deserialized into string with repr/ast.literal_eval.
    # This is a problem for storing the corresponding list_columns in
    # database. For the list types with this problem, we record in the
    # following dictionaries the functions that must be used to serialize
    # (in _list_item_to_string) and deserialize (in _string_to_list_item)
    # the list items.
    _list_item_to_string = {
        FIELD_TYPE_LIST_DATE: lambda x: x.isoformat(),
        FIELD_TYPE_LIST_DATETIME: lambda x: x.isoformat(),
        FIELD_TYPE_LIST_TIME: lambda x: x.isoformat()
    }

    _string_to_list_item = {
        FIELD_TYPE_LIST_DATE: lambda x: dateutil.parser.parse(x).date(),
        FIELD_TYPE_LIST_DATETIME: lambda x: dateutil.parser.parse(x),
        FIELD_TYPE_LIST_TIME: lambda x: dateutil.parser.parse(x).time(),
    }

    def __init__(self, database):
        """
        Creates a session API of the Database instance

        :param database: Database instance to take into account
        """

        self.database = database

    # Shortcuts to access Database attributes
    @property
    def engine(self):
        return self.database.engine

    @property
    def list_tables(self):
        return self.database.list_tables

    @property
    def query_type(self):
        return self.database.query_type



    """ COLLECTIONS """

    def add_collection(self, name, primary_key="index"):
        """
        Adds a collection

        :param name: New collection name (str, must not be existing)

        :param primary_key: New collection primary_key column (str) => "index" by default

        :raise ValueError: - If the collection is already existing
                           - If the collection name is invalid
                           - If the primary_key is invalid
        """

        # Checks
        if not isinstance(name, str):
            raise ValueError(
                "The collection name must be of type {0}, but collection name of type {1} given".format(str,
        if not isinstance(primary_key, str):
            raise ValueError(
                "The collection primary_key must be of type {0}, but collection primary_key of type {1} given".format(
                    str, type(primary_key)))
        collection_row = self.get_collection(name)
        if collection_row is not None:
            raise ValueError("A collection/table with the name {0} already exists".format(name))

        pk_name = self.name_to_valid_column_name(primary_key)
        table_name = self.name_to_valid_column_name(name)
        with self.engine.cursor() as c:
            c.add_collection(name, primary_key, table_name, pk_name)


    def remove_collection(self, name):
        """
        Removes a collection

        :param name: Collection to remove (str, must be existing)

        :raise ValueError: If the collection does not exist
        """

        # Checks
        collection_row = self.get_collection(name)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(name))

        # Removing the collection row
        self.session.query(self.table_classes[COLLECTION_TABLE]).filter(
            self.table_classes[COLLECTION_TABLE].collection_name == name).delete()
        self.session.query(self.table_classes[FIELD_TABLE]).filter(
            self.table_classes[FIELD_TABLE].collection_name == name).delete()

        # Removing the collection document table + metadata associated
        collection_query = DropTable(self.table_classes[self.name_to_valid_column_name(name)].__table__)
        self.session.execute(collection_query)
        self.metadata.remove(self.table_classes[self.name_to_valid_column_name(name)].__table__)

        # Removing the class associated
        self.table_classes.pop(name, None)

        if self.__caches:
            self.__documents.pop(name, None)
            self.__fields.pop(name, None)
            self.__collections.pop(name, None)

        self.session.flush()

        # Base updated to remove the document table of the collection
        self.__update_table_classes()

    def get_collection(self, name):
        """
        Returns the collection row of the collection

        :param name: Collection name (str, must be existing)

        :return: The collection row if it exists, None otherwise
        """

        if self.__caches:
            try:
                return self.__collections[name]
            except KeyError:
                return None
        else:
            if not isinstance(name, six.string_types):
                return None
            collection_row = self.session.query(self.table_classes[COLLECTION_TABLE]).filter(
                self.table_classes[COLLECTION_TABLE].collection_name == name).first()
            return collection_row

    def get_collections_names(self):
        """
        Gives the list of all collection names

        :return: List of all collection names
        """

        collections = self.session.query(self.table_classes[COLLECTION_TABLE].collection_name).all()
        collections_list = [collection.collection_name for collection in collections]
        return collections_list

    def get_collections(self):
        """
        Gives the list of all collection rows

        :return: List of all collection rows
        """

        return self.session.query(self.table_classes[COLLECTION_TABLE]).all()

    """ FIELDS """

    def add_fields(self, fields):
        """
        Adds the list of fields

        :param fields: List of fields: [collection, name, type, description]
        """

        collections = []

        if not isinstance(fields, list):
            raise ValueError(
                "The fields must be of type {0}, but fields of type {1} given".format(list, type(fields)))

        for field in fields:

            # Adding each field
            if not isinstance(field, list) or len(field) != 4:
                raise ValueError("Invalid field, it must be a list of four elements: [collection, name, type, description]")
            self.add_field(field[0], field[1], field[2], field[3], False)
            if field[0] not in collections:
                collections.append(field[0])

        # Updating the table classes
        self.session.flush()

        # Classes reloaded in order to add the new column attribute
        self.__update_table_classes()

        if self.__caches:
            for collection in collections:
                self.__refresh_cache_documents(collection)

    def add_field(self, collection, name, field_type, description=None,
                  index=False, flush=True):
        """
        Adds a field to the database

        :param collection: Field collection (str, must be existing)

        :param name: Field name (str, must not be existing)

        :param field_type: Field type, in ('string', 'int', 'float', 'boolean', 'date', 'datetime',
                     'time', 'json', 'list_string', 'list_int', 'list_float', 'list_boolean', 'list_date',
                     'list_datetime', 'list_time', 'list_json')

        :param description: Field description (str or None) => None by default

        :param index: Bool to know if indexing must be done => False by default

        :param flush: Bool to know if the table classes must be updated (put False if in the middle of filling fields) => True by default

        :raise ValueError: - If the collection does not exist
                           - If the field already exists
                           - If the field name is invalid
                           - If the field type is invalid
                           - If the field description is invalid
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_row = self.get_field(collection, name)
        if field_row is not None:
            raise ValueError("A field with the name {0} already exists in the collection {1}".format(name, collection))
        if not isinstance(name, str):
            raise ValueError(
                "The field name must be of type {0}, but field name of type {1} given".format(str, type(name)))
        if not field_type in ALL_TYPES:
            raise ValueError("The field type must be in {0}, but {1} given".format(ALL_TYPES, field_type))
        if not isinstance(description, str) and description is not None:
            raise ValueError(
                "The field description must be of type {0} or None, but field description of type {1} given".format(str,
                                                                                                                    type(
                                                                                                                        description)))

        # Adding the field in the field table
        field_row = self.table_classes[FIELD_TABLE](field_name=name, collection_name=collection, type=field_type,
                                                    description=description)

        if self.__caches:
            self.__fields[collection][name] = field_row

        self.session.add(field_row)

        # Fields creation
        if field_type in LIST_TYPES:
            if self.list_tables:
                table = 'list_%s_%s' % (self.name_to_valid_column_name(collection), self.name_to_valid_column_name(name))
                list_table = Table(table, self.metadata, Column('document_id', String, primary_key=True),
                                   Column('i', Integer, primary_key=True),
                                   Column('value', TYPE_TO_COLUMN[field_type[5:]]))
                list_query = CreateTable(list_table)
                self.session.execute(list_query)

                # Creating the class associated
                collection_dict = {'__tablename__': table, '__table__': list_table}
                collection_class = type(table, (self.base,), collection_dict)
                mapper(collection_class, list_table)
                self.table_classes[table] = collection_class
            # String columns if it list type, as the str representation of the lists will be stored
            field_type = String
        else:
            field_type = self.__field_type_to_column_type(field_type)

        column = Column(self.name_to_valid_column_name(name), field_type, index=index)
        column_str_type = column.type.compile(self.database.engine.dialect)
        column_name = column.compile(dialect=self.database.engine.dialect)

        # Column created in document table, and in initial table if initial values are used

        document_query = str('ALTER TABLE "%s" ADD COLUMN %s %s' %
                             (self.name_to_valid_column_name(collection), column_name, column_str_type))
        self.session.execute(document_query)
        self.table_classes[self.name_to_valid_column_name(collection)].__table__.append_column(column)
        
        # Redefinition of the table classes
        if flush:
            self.session.flush()

            # Classes reloaded in order to add the new column attribute
            self.__update_table_classes()

            if self.__caches:
                self.__refresh_cache_documents(collection)

        self.__unsaved_modifications = True

    def name_to_valid_column_name(self, name):
        """
        Transforms the name into a valid and unique table/column name, by hashing it with md5

        :param name: Name (str)

        :return: Valid and unique (hashed) table/column name
        """

        if self.__caches:
            try:
                return self.__names[name]
            except KeyError:
                valid_name = hashlib.md5(name.encode('utf-8')).hexdigest()
                self.__names[name] = valid_name
                return valid_name
        else:
            valid_name = hashlib.md5(name.encode('utf-8')).hexdigest()
            return valid_name

    def remove_field(self, collection, field):
        """
        Removes a field in the collection

        :param collection: Field collection (str, must be existing)

        :param field: Field name (str, must be existing), or list of fields (list of str, must all be existing)

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_rows = []
        if isinstance(field, list):
            for field_elem in field:
                field_row = self.get_field(collection, field_elem)
                if field_row is None:
                    raise ValueError(
                        "The field with the name {0} does not exist in the collection {1}".format(field_elem,
                                                                                                  collection))
                else:
                    field_rows.append(field_row)
        else:
            field_row = self.get_field(collection, field)
            if field_row is None:
                raise ValueError(
                    "The field with the name {0} does not exist in the collection {1}".format(field, collection))
            else:
                field_rows.append(field_row)

        field_names = []
        if isinstance(field, list):
            for field_elem in field:
                field_names.append(self.name_to_valid_column_name(field_elem))
        else:
            field_names.append(self.name_to_valid_column_name(field))

        # Field removed from collection document table
        old_document_table = Table(self.name_to_valid_column_name(collection), self.metadata)
        select = sql.select(
            [c for c in old_document_table.c if c.name not in str(field_names)])

        remaining_columns = [copy.copy(c) for c in old_document_table.columns
                             if c.name not in str(field_names)]

        # Creation of backup table, not containing the column
        document_backup_table = Table(self.name_to_valid_column_name(collection) + "_backup", self.metadata)

        for column in old_document_table.columns:
            if column.name not in str(field_names):
                document_backup_table.append_column(column.copy())

        self.session.execute(CreateTable(document_backup_table))

        insert = sql.insert(document_backup_table).from_select(
            [c.name for c in remaining_columns], select)
        self.session.execute(insert)

        # Removing the original table
        self.metadata.remove(old_document_table)
        self.session.execute(DropTable(old_document_table))

        # Recreating the table without the column
        new_document_table = Table(self.name_to_valid_column_name(collection), self.metadata)
        for column in document_backup_table.columns:
            new_document_table.append_column(column.copy())

        self.session.execute(CreateTable(new_document_table))

        select = sql.select(
            [c for c in document_backup_table.c if c.name not in str(field_names)])
        insert = sql.insert(new_document_table).from_select(
            [c.name for c in remaining_columns], select)
        self.session.execute(insert)

        # Removing the backup table
        self.metadata.remove(document_backup_table)
        self.session.execute(DropTable(document_backup_table))

        if self.list_tables:
            if isinstance(field, list):
                for field_elem in field:
                    if self.get_field(collection, field_elem).type in LIST_TYPES:
                        table = 'list_%s_%s' % (self.name_to_valid_column_name(collection), self.name_to_valid_column_name(field_elem))
                        collection_query = DropTable(self.table_classes[table].__table__)
                        self.session.execute(collection_query)
                        self.metadata.remove(self.table_classes[table].__table__)

            else:
                if self.get_field(collection, field).type in LIST_TYPES:
                    table = 'list_%s_%s' % (self.name_to_valid_column_name(collection), self.name_to_valid_column_name(field))
                    collection_query = DropTable(self.table_classes[table].__table__)
                    self.session.execute(collection_query)
                    self.metadata.remove(self.table_classes[table].__table__)

        # Removing field rows from field table
        for field_row in field_rows:
            self.session.delete(field_row)

        self.session.flush()

        # Classes reloaded in order to remove the columns attributes
        self.__update_table_classes()

        if self.__caches:
            self.__refresh_cache_documents(collection)
            if isinstance(field, list):
                for field_elem in field:
                    self.__fields[collection].pop(field_elem, None)
            else:
                self.__fields[collection].pop(field, None)

        self.__unsaved_modifications = True

    def get_field(self, collection, name):
        """
        Gives the field row, given a field name and a collection

        :param collection: Document collection (str, must be existing)

        :param name: Field name (str, must be existing)

        :return: The field row if the field exists, None otherwise
        """

        if self.__caches:
            try:
                return self.__fields[collection][name]
            except KeyError:
                return None
        else:
            if not isinstance(collection, six.string_types) or not isinstance(name, six.string_types):
                return None
            field_row = self.session.query(self.table_classes[FIELD_TABLE]).filter(
                self.table_classes[FIELD_TABLE].field_name == name).filter(
                self.table_classes[FIELD_TABLE].collection_name == collection).first()
            return field_row

    def get_fields_names(self, collection):
        """
        Gives the list of all fields, given a collection

        :param collection: Fields collection (str, must be existing)

        :return: List of all fields names of the collection if it exists, None otherwise
        """

        fields = self.session.query(self.table_classes[FIELD_TABLE].field_name).filter(
            self.table_classes[FIELD_TABLE].collection_name == collection).all()

        fields_names = [field.field_name for field in fields]

        return fields_names

    def get_fields(self, collection):
        """
        Gives the list of all fields rows, given a collection

        :param collection: Fields collection (str, must be existing)

        :return: List of all fields rows of the collection if it exists, None otherwise
        """

        fields = self.session.query(self.table_classes[FIELD_TABLE]).filter(
            self.table_classes[FIELD_TABLE].collection_name == collection).all()
        return fields

    """ VALUES """

    def get_value(self, collection, document, field):
        """
        Gives the current value of <collection, document, field>

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :return: The current value of <collection, document, field> if it exists, None otherwise
        """
        
        document = self.get_document(collection, document)
        if document is None:
            return None
        try:
            return getattr(document, field, None)
        except TypeError:
            # Raises if field is not a string
            return None

    def set_value(self, collection, document, field, new_value, flush=True):
        """
        Sets the value associated to <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :param new_value: New value

        :param flush: Bool to know if flush to do => True by default

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the value is invalid
                           - If trying to set the primary_key
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_row = self.get_field(collection, field)
        if field_row is None:
            raise ValueError(
                "The field with the name {0} does not exist in the collection {1}".format(field, collection))
        document_row = self.__get_document_row(collection, document)
        if document_row is None:
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document, collection))
        if not self.__check_type_value(new_value, field_row.type):
            raise ValueError("The value {0} is invalid for the type {1}".format(new_value, field_row.type))

        column_name = self.name_to_valid_column_name(field)
        new_column = self.__python_to_column(field_row.type, new_value)

        if field != collection_row.primary_key:
            setattr(document_row, column_name, new_column)
        else:
            raise ValueError("Impossible to set the primary_key value of a document")

        if self.list_tables and isinstance(new_value, list):
            primary_key = self.get_collection(collection).primary_key
            document_id = getattr(document_row, self.name_to_valid_column_name(primary_key))
            table_name = 'list_%s_%s' % (self.name_to_valid_column_name(collection), column_name)

            table = self.metadata.tables[table_name]
            sql = table.delete(table.c.document_id == document_id)
            self.session.execute(sql)

            sql = table.insert()
            sql_params = []
            cvalues = [self.__python_to_column(field_row.type[5:], i) for i in new_value]
            index = 0
            for i in cvalues:
                sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                index += 1
            if sql_params:
                self.session.execute(sql, params=sql_params)

        if flush:
            self.session.flush()

        self.__unsaved_modifications = True

    def set_values(self, collection, document, values, flush=True):
        """
        Sets the values of a <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :param values: Dict of values (key=field, value=value)

        :param flush: Bool to know if flush to do => True by default

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the values are invalid
                           - If trying to set the primary_key
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        document_row = self.__get_document_row(collection, document)
        if document_row is None:
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document, collection))
        if not isinstance(values, dict):
            raise ValueError(
                "The values must be of type {0}, but values of type {1} given".format(dict, type(values)))
        for field in values:
            field_row = self.get_field(collection, field)
            if field_row is None:
                raise ValueError(
                    "The field with the name {0} does not exist in the collection {1}".format(field, collection))
            if not self.__check_type_value(values[field], field_row.type):
                raise ValueError("The value {0} is invalid for the type {1}".format(values[field], field_row.type))

        database_values = {}
        for field in values:
            column_name = self.name_to_valid_column_name(field)
            field_row = self.get_field(collection, field)
            new_column = self.__python_to_column(field_row.type, values[field])
            database_values[column_name] = new_column
            if collection_row.primary_key == field:
                raise ValueError("Impossible to set the primary_key value of a document")

        # Updating all values
        for column in database_values:
            setattr(document_row, column, database_values[column])

        # Updating list tables values
        for field in values:
            field_row = self.get_field(collection, field)
            if self.list_tables and isinstance(values[field], list):
                column = self.name_to_valid_column_name(field)
                collection_name = self.name_to_valid_column_name(collection)
                table_name = 'list_%s_%s' % (collection_name, column)
                table = self.metadata.tables[table_name]
                sql = table.delete(table.c.document_id == document)
                self.session.execute(sql)

                sql = table.insert()
                sql_params = []
                cvalues = [self.__python_to_column(field_row.type[5:], i) for i in values[field]]
                index = 0
                for i in cvalues:
                    sql_params.append({'document_id': document, 'i': index, 'value': i})
                    index += 1
                if sql_params:
                    self.session.execute(sql, params=sql_params)

        # TODO set list tables values

        if flush:
            self.session.flush()

        self.__unsaved_modifications = True

    def remove_value(self, collection, document, field, flush=True):
        """
        Removes the value <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :param flush: Bool to know if flush to do (put False in the middle of removing values) => True by default

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_row = self.get_field(collection, field)
        if field_row is None:
            raise ValueError(
                "The field with the name {0} does not exist in the collection {1}".format(field, collection))
        document_row = self.__get_document_row(collection, document)
        if document_row is None:
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document, collection))

        sql_column_name = self.name_to_valid_column_name(field)
        collection_name = self.name_to_valid_column_name(collection)
        old_value = getattr(document_row, sql_column_name)
        setattr(document_row, sql_column_name, None)

        if self.list_tables and field_row.type.startswith('list_'):
            primary_key = self.get_collection(collection).primary_key
            document_id = getattr(document_row, self.name_to_valid_column_name(primary_key))
            table_name = 'list_%s_%s' % (collection_name, sql_column_name)
            table = self.metadata.tables[table_name]
            sql = table.delete(table.c.document_id == document_id)
            self.session.execute(sql)

        if flush:
            self.session.flush()
        self.__unsaved_modifications = True

    def add_value(self, collection, document, field, value, checks=True):
        """
        Adds a value for <collection, document, field>

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :param value: Value to add

        :param checks: Bool to know if flush to do and value check (Put False in the middle of adding values) => True by default

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the value is invalid
                           - If <collection, document, field> already has a value
        """

        collection_row = self.get_collection(collection)
        field_row = self.get_field(collection, field)
        document_row = self.__get_document_row(collection, document)

        if checks:
            if collection_row is None:
                raise ValueError("The collection {0} does not exist".format(collection))
            if field_row is None:
                raise ValueError(
                    "The field with the name {0} does not exist in the collection {1}".format(field, collection))
            if document_row is None:
                raise ValueError(
                    "The document with the name {0} does not exist in the collection {1}".format(document, collection))
            if not self.__check_type_value(value, field_row.type):
                raise ValueError("The value {0} is invalid for the type {1}".format(value, field_row.type))

        field_name = self.name_to_valid_column_name(field)
        database_value = getattr(
            document_row, field_name)
        collection_name = self.name_to_valid_column_name(collection)

        # We add the value only if it does not already exist
        if database_value is None:
            if value is not None:
                current_value = self.__python_to_column(
                    field_row.type, value)
                setattr(
                    document_row, field_name,
                    current_value)
                if self.list_tables and isinstance(value, list):
                    primary_key = self.get_collection(collection).primary_key
                    document_id = getattr(document_row, self.name_to_valid_column_name(primary_key))
                    table = 'list_%s_%s' % (collection_name, field_name)
                    sql = self.metadata.tables[table].insert()
                    sql_params = []
                    cvalues = [self.__python_to_column(field_row.type[5:], i) for i in value]
                    index = 0
                    for i in cvalues:
                        sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                        index += 1
                    if sql_params:
                        self.session.execute(sql, params=sql_params)

            if checks:
                self.session.flush()
            self.__unsaved_modifications = True

        else:
            raise ValueError(
                "The tuple <{0}, {1}> already has a value in the collection {2}".format(field, document, collection))

    """ DOCUMENTS """

    def __get_document_row(self, collection, document):
        """
        Gives the SQLAchemy row of a document, given a collection and a
        document identifier.

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :return: The document row if the document exists, None otherwise
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return None
        if self.__caches:
            return self.__documents[collection].get(document)
        else:
            primary_key = collection_row.primary_key
            column = getattr(self.table_classes[self.name_to_valid_column_name(collection)],
                             self.name_to_valid_column_name(primary_key))
            value = column.type.python_type(document)
            query = self.session.query(self.table_classes[self.name_to_valid_column_name(collection)]).filter(
                column == value)
            document_row = query.first()
            return document_row
    
    def get_document(self, collection, document):
        """
        Gives a Document instance given a collection and a document identifier

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :return: The document row if the document exists, None otherwise
        """

        document_row = self.__get_document_row(collection, document)
        if document_row is not None:
            document = Document(self, collection, document_row)
        else:
            document = None
        return document

    def get_documents_names(self, collection):
        """
        Gives the list of all document names, given a collection

        :param collection: Documents collection (str, must be existing)

        :return: List of all document names of the collection if it exists, None otherwise
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return []
        else:
            documents = self.session.query(getattr(self.table_classes[self.name_to_valid_column_name(collection)],
                                                   self.name_to_valid_column_name(collection_row.primary_key))).all()
            documents_list = [getattr(document, self.name_to_valid_column_name(collection_row.primary_key)) for document
                              in documents]
            return documents_list

    def get_documents(self, collection):
        """
        Gives the list of all document rows, given a collection

        :param collection: Documents collection (str, must be existing)

        :return: List of all document rows of the collection if it exists, None otherwise
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return []
        else:
            documents = self.session.query(self.table_classes[self.name_to_valid_column_name(collection)]).all()
            documents_list = [Document(self, collection, document) for document in documents]
            return documents_list

    def remove_document(self, collection, document):
        """
        Removes a document in the collection

        :param collection: Document collection (str, must be existing)

        :param document: Document name (str, must be existing)

        :raise ValueError: - If the collection does not exist
                           - If the document does not exist
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        document_row = self.get_document(collection, document)
        if document_row is None:
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document, collection))
        primary_key = collection_row.primary_key

        self.session.query(self.table_classes[self.name_to_valid_column_name(collection)]).filter(
            getattr(self.table_classes[self.name_to_valid_column_name(collection)],
                    self.name_to_valid_column_name(primary_key)) == document).delete()

        # Removing document from list tables
        if self.list_tables:
            for table in self.table_classes:
                if "list" in table:
                    self.session.query(self.table_classes[table]).filter(
                        self.table_classes[table].document_id == document).delete()

        if self.__caches:
            self.__documents[collection].pop(document, None)

        self.session.flush()
        self.__unsaved_modifications = True

    def add_document(self, collection, document, create_missing_fields=True, flush=True):
        """
        Adds a document to a collection

        :param collection: Document collection (str, must be existing)

        :param document: Dictionary of document values (dict), or document primary_key (str)

                            - The primary_key must not be existing

        :param create_missing_fields: Boolean to know if the missing fields must be created

            - If True, fields that are in the document but not in the collection are created if the type can be guessed from the value in the document
              (possible for all valid values except None and []).
            
        :param flush: Bool to know if flush to do, put False in the middle of filling the table => True by default

        :raise ValueError: - If the collection does not exist
                           - If the document already exists
                           - If document is invalid (invalid name or no primary_key)
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        primary_key = self.get_collection(collection).primary_key
        if not isinstance(document, dict) and not isinstance(document, str):
            raise ValueError(
                "The document must be of type {0} or {1}, but document of type {2} given".format(dict, str, document))
        if isinstance(document, dict) and primary_key not in document:
            raise ValueError(
                "The primary_key {0} of the collection {1} is missing from the document dictionary".format(primary_key,
                                                                                                           collection))
        if isinstance(document, dict):
            document_row = self.get_document(collection, document[primary_key])
        else:
            document_row = self.get_document(collection, document)
        if document_row is not None:
            raise ValueError(
                "A document with the name {0} already exists in the collection {1}".format(document, collection))

        if not isinstance(document, dict):
            document = {primary_key: document}

        document_id = document[primary_key]
        column_values = {self.name_to_valid_column_name(primary_key): document_id}
        lists = []
        for k, v in document.items():
            column_name = self.name_to_valid_column_name(k)
            self.ensure_field_for_value(collection, k, v, create=create_missing_fields)
            field_type = self.get_field(collection, k).type
            column_value = self.__python_to_column(field_type, v)
            column_values[column_name] = column_value
            if self.list_tables and isinstance(v, list):
                table = 'list_%s_%s' % (self.name_to_valid_column_name(collection), column_name)
                # sql = sql_text('INSERT INTO %s (document_id, i, value) VALUES (:document_id, :i, :value)' % table)
                sql = self.metadata.tables[table].insert()
                sql_params = []
                cvalues = [self.__python_to_column(field_type[5:], i) for i in v]
                index = 0
                for i in cvalues:
                    sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                    index += 1
                lists.append((sql, sql_params))

        if self.list_tables:
            for sql, sql_params in lists:
                if sql_params:
                    self.session.execute(sql, params=sql_params)

        document_row = self.table_classes[self.name_to_valid_column_name(collection)](**column_values)
        self.session.add(document_row)

        if self.__caches:
            self.__documents[collection][document_id] = document_row

        if flush:
            self.session.flush()

        self.__unsaved_modifications = True

    def ensure_field_for_value(self, collection, field , value, create=True):
        """
        Check that a field exists otherwise create with an appropriate type
        corresponding to a Python value.

        :param collection: Document collection (str, must be existing)

        :param field: field name to check

        :param value: value whose type is used to determine the field type
            
        :param create: if False, raises an error if the field does not exist
        """
        
        if self.get_field(collection, field) is None:
            if not create:
                raise ValueError('Collection {0} has no field {1}'
                                 .format(collection, field))
            try:
                field_type = self.__python_value_type(value)
            except KeyError:
                raise ValueError('Collection {0} has no field {1} and it '
                                 'cannot be created from a value of type {2}'
                                 .format(collection,
                                         field,
                                         type(value)))
            self.add_field(collection, field, field_type)


    """ MODIFICATIONS """

    def save_modifications(self):
        """
        Saves the modifications by committing the session
        """
        try:
            self.session.commit()
        except:
            self.session.rollback()
            raise

        self.__unsaved_modifications = False

    def unsave_modifications(self):
        """
        Unsaves the modifications by rolling back the session
        """

        self.session.rollback()
        self.__unsaved_modifications = False
        self.metadata = MetaData()
        self.metadata.reflect(self.database.engine)
        self.__update_table_classes()
        self.__fill_caches()

    def has_unsaved_modifications(self):
        """
        Knowing if the database has pending modifications that are unsaved

        :return: True if there are pending modifications to save,
                 False otherwise
        """

        return self.__unsaved_modifications

    """ FILTERS """

    def __filter_query(self, collection, filter, query_type=None):
        """
        Given a filter string, return a query that can be used with
        filter_documents() to select documents

        :param query_type: Type of query to build, in ('mixed', 'sql', 'python', 'guess') => None by default

                                - If None, the default query_type is used.

        :param filter:

        :param collection: Filter collection (str, must be existing)
        """

        if query_type is None:
            query_type = self.query_type
        filter_to_query_class = populse_db.filter._filter_to_query_classes[query_type]
        tree = populse_db.filter.filter_parser().parse(filter)
        query = filter_to_query_class(self, collection).transform(tree)
        return query

    def filter_documents(self, collection, filter_query):
        """
        Iterates over the collection documents selected by filter_query

        Each item yield is a row of the collection table returned by sqlalchemy

        filter_query can be the result of self.filter_query() or a string containing a filter
        (in this case self.fliter_query() is called to get the actual query)

        :param collection: Filter collection (str, must be existing)
        :param filter_query: Filter query (str)

                                - A filter row must be written this way: {<field>} <operator> "<value>"
                                - The operator must be in ('==', '!=', '<=', '>=', '<', '>', 'IN', 'ILIKE', 'LIKE')
                                - The filter rows can be linked with ' AND ' or ' OR '
                                - Example: "((({BandWidth} == "50000")) AND (({FileName} LIKE "%G1%")))"
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))

        if isinstance(filter_query, six.string_types):
            filter_query = self.__filter_query(collection, filter_query)
        if filter_query is None:
            select = self.metadata.tables[self.name_to_valid_column_name(collection)].select()
            python_filter = None
        elif isinstance(filter_query, types.FunctionType):
            select = self.metadata.tables[self.name_to_valid_column_name(collection)].select()
            python_filter = filter_query
        elif isinstance(filter_query, tuple):
            sql_condition, python_filter = filter_query
            select = select = self.metadata.tables[self.name_to_valid_column_name(collection)].select(
                sql_condition)
        else:
            select = self.metadata.tables[self.name_to_valid_column_name(collection)].select(
                filter_query)
            python_filter = None
        for row in self.session.execute(select):
            row = Document(self, collection, row)
            if python_filter is None or python_filter(row):
                yield row

    """ UTILS """

    _python_type_to_tag_type = {
        type(None): None,
        type(''): FIELD_TYPE_STRING,
        type(u''): FIELD_TYPE_STRING,
        int: FIELD_TYPE_INTEGER,
        float: FIELD_TYPE_FLOAT,
        time: FIELD_TYPE_TIME,
        datetime: FIELD_TYPE_DATETIME,
        date: FIELD_TYPE_DATE,
        bool: FIELD_TYPE_BOOLEAN,
        dict: FIELD_TYPE_JSON,
    }

    def __python_value_type(self, value):
        """
        Returns the field type corresponding to a Python value.
        This type can be used in add_field(s) method.
        For list values, only the first item is considered to get the type.
        Type cannot be determined for empty list.
        If value is None, the result is None.
        """
        if isinstance(value, list):
            if value:
                item_type = self.__python_value_type(value[0])
                return 'list_' + item_type
            else:
                # Raises a KeyError for empty list
                return self._python_type_to_tag_type[list]
        else:
            return self._python_type_to_tag_type[type(value)]
    
    @staticmethod
    def __field_type_to_column_type(field_type):
        """
        Gives the SQLAlchemy column type corresponding to the field type

        :param field_type: Column type

        :return: The sql column type given the field type
        """

        return TYPE_TO_COLUMN[field_type]

    @staticmethod
    def __check_type_value(value, valid_type):
        """
        Checks the type of the value

        :param value: Value

        :param valid_type: Type that the value is supposed to have

        :return: True if the value is valid, False otherwise
        """

        value_type = type(value)
        if valid_type is None:
            return False
        if value is None:
            return True
        if valid_type == FIELD_TYPE_INTEGER and value_type == int:
            return True
        if valid_type == FIELD_TYPE_FLOAT and value_type == int:
            return True
        if valid_type == FIELD_TYPE_FLOAT and value_type == float:
            return True
        if valid_type == FIELD_TYPE_BOOLEAN and value_type == bool:
            return True
        if valid_type == FIELD_TYPE_STRING and value_type == str:
            return True
        if valid_type == FIELD_TYPE_JSON and value_type == dict:
            return True
        if valid_type == FIELD_TYPE_DATETIME and value_type == datetime:
            return True
        if valid_type == FIELD_TYPE_TIME and value_type == time:
            return True
        if valid_type == FIELD_TYPE_DATE and value_type == date:
            return True
        if (valid_type in LIST_TYPES
                and value_type == list):
            for value_element in value:
                if not DatabaseSession.__check_type_value(value_element, valid_type.replace("list_", "")):
                    return False
            return True
        return False

    @staticmethod
    def __python_to_column(column_type, value):
        """
        Converts a python value into a suitable value to put in a
        database column.
        """
        if isinstance(value, list):
            return DatabaseSession.__list_to_column(column_type, value)
        elif isinstance(value, dict):
            return json.dumps(value)
        else:
            return value

    @staticmethod
    def __column_to_python(column_type, value):
        """
        Converts a value of a database column into the corresponding
        Python value.
        """
        if column_type.startswith('list_'):
            return DatabaseSession.__column_to_list(column_type, value)
        elif column_type == FIELD_TYPE_JSON:
            if value is None:
                return None
            return json.loads(value)
        else:
            return value

    @staticmethod
    def __list_to_column(column_type, value):
        """
        Converts a python list value into a suitable value to put in a
        database column.
        """
        converter = DatabaseSession._list_item_to_string.get(column_type)
        if converter is None:
            list_value = value
        else:
            list_value = [converter(i) for i in value]
        return repr(list_value)

    @staticmethod
    def __column_to_list(column_type, value):
        """
        Converts a value of a database column into the corresponding
        Python list value.
        """
        if value is None:
            return None
        list_value = ast.literal_eval(value)
        converter = DatabaseSession._string_to_list_item.get(column_type)
        if converter is None:
            return list_value
        return [converter(i) for i in list_value]

class Undefined:
    pass

class Document(dict):
    '''
    A Document is a Python dictionary containing a document field values.
    It is build from the result of an SQL query and allow to access to the
    fields via attribute syntax (e.g. doc.toto == doc['toto']).
    '''

    def __init__(self, database_session, collection, row):
        for field in database_session.get_fields_names(collection):
            column = database_session.name_to_valid_column_name(field)
            db_value = getattr(row, column)
            value = database_session._DatabaseSession__column_to_python(
                database_session.get_field(collection,field).type,
                db_value)
            self[field] = value

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
