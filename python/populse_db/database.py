import ast
import copy
import hashlib
import os
import re
import types
from datetime import date, time, datetime

import dateutil.parser
import six
from sqlalchemy import (create_engine, Column, MetaData, Table, sql,
                        String, Integer, Float, Boolean, Date, DateTime,
                        Time, Enum, event)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session, mapper
from sqlalchemy.schema import CreateTable, DropTable

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
        - string_engine: string engine of the database
        - caches: Boolean to know if the caches must be used
        - list_tables: Boolean to know if list tables must be used
                        True if tables are created to store values of
                        list fields and have a pure SQL version of IN
                        operator in filters
        - query_type: default query implementation for filter_query()
                      and filter_documents()
                      Can be 'sql', 'python', 'mixed', or 'guess'
        - engine: SQLAlchemy database engine

    methods:
        - create_empty_schemas: initializes empty database schema
        - __enter__: create or get a DatabaseSession instance
        - __exit__: release the latest created DatabaseSession
        - clear: clears the database

    """

    def __init__(self, string_engine, caches=False, list_tables=True,
                 query_type='mixed'):
        self.string_engine = string_engine
        if not isinstance(caches, bool):
            raise ValueError("Wrong caches, it must be of type {0}, but caches of type {1} given".format(bool, type(caches)))
        self.caches = caches
        if not isinstance(list_tables, bool):
            raise ValueError("Wrong list_tables, it must be of type {0}, but list_tables of type {1} given".format(bool, type(list_tables)))
        self.list_tables = list_tables
        query_list = [populse_db.filter.QUERY_MIXED, populse_db.filter.QUERY_GUESS, populse_db.filter.QUERY_PYTHON, populse_db.filter.QUERY_SQL]
        if query_type not in query_list:
            raise ValueError("Wrong query_type, it must be in {0}, but {1} given".format(query_list, query_type))
        self.query_type = query_type

        # SQLite database: we create it if it does not exist
        if string_engine.startswith('sqlite'):
            self.__db_file = re.sub("sqlite.*:///", "", string_engine)
            if not os.path.exists(self.__db_file):
                parent_dir = os.path.dirname(self.__db_file)
                if not os.path.exists(parent_dir):
                    os.makedirs(os.path.dirname(self.__db_file))

        self.create_empty_schema(self.string_engine)
        self.engine = create_engine(self.string_engine, connect_args={'check_same_thread': False})

        if string_engine.startswith('sqlite'):
            @event.listens_for(self.engine, "connect")
            def do_connect(dbapi_connection, connection_record):
                # disable pysqlite's emitting of the BEGIN statement entirely.
                # also stops it from emitting COMMIT before any DDL.
                dbapi_connection.isolation_level = None

            @event.listens_for(self.engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                """
                Manages the pragmas during the database opening
                :param dbapi_connection:
                :param connection_record:
                """
                dbapi_connection.execute('pragma case_sensitive_like=ON')
                dbapi_connection.execute('pragma foreign_keys=ON')

            @event.listens_for(self.engine, "begin")
            def do_begin(conn):
                # emit our own BEGIN
                conn.execute("BEGIN")

        self.__scoped_session = scoped_session(sessionmaker(
            bind=self.engine, autocommit=False, autoflush=False))

    @staticmethod
    def create_empty_schema(string_engine):
        """
        Creates the database file with an empty schema
        :param string_engine: Path of the new database file
        """

        engine = create_engine(string_engine)
        metadata = MetaData()
        metadata.reflect(bind=engine)
        if FIELD_TABLE in metadata.tables:
            return False
        else:
            Table(FIELD_TABLE, metadata,
                  Column("name", String, primary_key=True),
                  Column("collection", String, primary_key=True),
                  Column(
                      "type", Enum(FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT, FIELD_TYPE_BOOLEAN,
                                   FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME, FIELD_TYPE_JSON,
                                   FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER,
                                   FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN, FIELD_TYPE_LIST_DATE,
                                   FIELD_TYPE_LIST_DATETIME, FIELD_TYPE_LIST_TIME, FIELD_TYPE_LIST_JSON,
                                   name='field_type'),
                      nullable=False),
                  Column("description", String, nullable=True))

            Table(COLLECTION_TABLE, metadata,
                  Column("name", String, primary_key=True),
                  Column("primary_key", String, nullable=False))

            metadata.create_all(engine)
            return True

    def __enter__(self):
        '''
        Return a DatabaseSession instance for using the database. This is
        supossed to be called using a "with" statement:
        
        with database as session:
           session.add_document(...)
           
        Therefore __exit__ must be called to get rid of the session.
        When called recursively, the underlying database session returned
        is the same. The commit/rollback of the session is done only by the
        outermost __enter__/__exit__ pair (i.e. by the outermost with
        statement).
        '''
        # Create the session object
        new_session = self.__scoped_session()
        # Check if it is a brain new session object or if __enter__ already
        # added a DatabaseSession instance to it (meaning recursive call)
        db_session = getattr(new_session, '_populse_db_session', None)
        if db_session is None:
            # No recursive call. Create a new DatabaseSession
            # and attach it to the session. Doing this way allow
            # to be thread safe because scoped_session automatically
            # creates a new session per thread. Therefore we also
            # create a new DatabaseSession per thread.
            db_session = DatabaseSession(self, new_session)
            new_session._populse_db_session = db_session
            # Attache a counter to the session object to count
            # the recursion depth of __enter__ calls
            new_session._populse_db_counter = 1
        else:
            # __enter__ is called recursively. Simply increment
            # the recusive depth counter previously attached to
            # the session object
            new_session._populse_db_counter += 1
        return db_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        Release a DatabaseSession previously created by __enter__.
        If no recursive call of __enter__ was done, the session
        is commited if no error is reported (e.g. exc_type is None)
        otherwise it is rolled back. Nothing is done 
        '''
        # Get the current session. SqlAlchemy scoped_session returns
        # the same object (per thread) on each call until remove()
        # is called.
        current_session = self.__scoped_session()
        # Decrement recursive depth counter
        current_session._populse_db_counter -= 1
        if current_session._populse_db_counter == 0:
            # If there is no recursive call, commit or rollback
            # the session according to the presence of an exception
            if exc_type is None:
                current_session.commit()
            else:
                current_session.rollback()
            # Delete the database session
            del current_session._populse_db_session
            del current_session._populse_db_counter
            self.__scoped_session.remove()

    def clear(self):
        """
        Remove all documents and collections in the database
        """

        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        if FIELD_TABLE in metadata.tables:
            for table in reversed(metadata.sorted_tables):
                if table.name in (FIELD_TABLE, COLLECTION_TABLE):
                    self.engine.execute(table.delete())
                else:
                    self.engine.execute(DropTable(table))
            return True
        else:
            return False

class DatabaseSession:
    """
    DatabaseSession API

    attributes:
        - database: database
        - session: session related to the database
        - table_classes: list of table classes, generated automatically
        - base: database base
        - metadata: database metadata
        - unsaved_modifications: boolean to know if there are unsaved
          modifications in the database

    methods:
        - add_collection: adds a collection
        - remove_collection: removes a collection
        - get_collection: gives the collection row
        - get_collections: gives all collection rows
        - get_collections_names: gives all collection names
        - add_field: adds a field
        - add_fields: adds a list of fields
        - remove_field: removes a field
        - get_field: Gives all fields rows
        - get_fields_names: gives all fields names
        - field_type_to_column_type: gives the column type corresponding
          to a field type
        - field_name_to_column_name: gives the column name corresponding
          to the field name
        - get_value: gives the value of <document, field>
        - set_value: sets the value of <document, field>
        - set_values: sets several values of a collection document
        - remove_value: removes the value of <document, field>
        - check_type_value: checks the type of a value
        - new_value: adds a value to <document, field>
        - get_document: gives the document row given a document name
        - get_documents: gives all document rows
        - get_documents_names: gives all document names
        - add_document: adds a document
        - remove_document: removes a document
        - save_modifications: saves the pending modifications
        - unsave_modifications: unsaves the pending modifications
        - has_unsaved_modifications: to know if there are unsaved
          modifications
        - filter_documents: gives the list of documents matching the filter
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

    def __init__(self, database, session):
        """
        Creates a session API of the database instance
        :param database: Database to take into account
        :param session: Session related to the database
        """

        self.database = database
        self.session = session

        # Database opened
        self.metadata = MetaData()
        self.metadata.reflect(self.database.engine)

        self.unsaved_modifications = False

        self.__update_table_classes()

        # Database schema checked
        if (COLLECTION_TABLE not in self.table_classes.keys() or
                FIELD_TABLE not in self.table_classes.keys()):
            raise ValueError(
                'The database schema is not coherent with the API')

        if self.__caches:
            self.__fill_caches()

    # Shortcuts to access Database attributes
    @property
    def __caches(self):
        return self.database.caches

    @property
    def list_tables(self):
        return self.database.list_tables

    @property
    def query_type(self):
        return self.database.query_type

    def __update_table_classes(self):
        """
        Redefines the model after an update of the schema
        """
        self.table_classes = {}
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(engine=self.database.engine)
        for table in self.metadata.tables.keys():
            self.table_classes[table] = getattr(
                self.base.classes, table)

    """ CACHES """

    def __fill_caches(self):
        """
        Fills the caches at database opening if they are used
        """

        self.__documents = {}
        self.__fields = {}
        self.__names = {}
        self.__collections = {}

        # Collections
        collections_rows = self.session.query(self.table_classes[COLLECTION_TABLE]).all()
        for collection_row in collections_rows:
            self.__collections[collection_row.name] = collection_row

        # Fields
        for collection in self.__collections:
            fields_rows = self.session.query(self.table_classes[FIELD_TABLE]).filter(
                self.table_classes[FIELD_TABLE].collection == collection).all()
            self.__fields[collection] = {}
            for field_row in fields_rows:
                self.__fields[collection][field_row.name] = field_row

        # Documents
        for collection in self.__collections:
            documents_rows = self.session.query(self.table_classes[collection]).all()
            self.__documents[collection] = {}
            for document_row in documents_rows:
                self.__documents[collection][getattr(document_row, self.__collections[collection].primary_key)] = FieldRow(self, collection, document_row)

        # Names
        for collection in self.__collections:
            self.__names[collection] = {}
            for field in self.__fields[collection]:
                if field == self.__collections[collection].primary_key:
                    self.__names[collection][field] = field
                else:
                    self.__names[collection][field] = hashlib.md5(field.encode('utf-8')).hexdigest()

    def __refresh_cache_documents(self, collection):
        """
        Refreshes the document cache after field added/removed
        :param collection: collection to refresh
        """

        self.__documents[collection].clear()
        documents_rows = self.session.query(self.table_classes[collection]).all()
        self.__documents[collection] = {}
        for document_row in documents_rows:
            self.__documents[collection][getattr(document_row, self.__collections[collection].primary_key)] = FieldRow(
                self, collection, document_row)

    """ COLLECTIONS """

    def add_collection(self, name, primary_key="name"):
        """
        Adds a collection
        :param name: New collection name
        :param primary_key: New collection primary_key column => "name" by default
        """

        # Checks
        collection_row = self.get_collection(name)
        if collection_row is not None or name in self.table_classes:
            raise ValueError("A collection/table with the name {0} already exists".format(name))
        if not isinstance(name, str):
            raise ValueError("The collection name must be of type {0}, but collection name of type {1} given".format(str, type(name)))
        if not isinstance(primary_key, str):
            raise ValueError("The collection primary_key must be of type {0}, but collection primary_key of type {1} given".format(str, type(primary_key)))

        # Adding the collection row
        collection_row = self.table_classes[COLLECTION_TABLE](name=name, primary_key=primary_key)
        self.session.add(collection_row)

        # Creating the collection document table
        collection_table = Table(name, self.metadata, Column(primary_key, String, primary_key=True))
        collection_query = CreateTable(collection_table)
        self.session.execute(collection_query)

        # Creating the class associated
        collection_dict = {'__tablename__': name, '__table__': collection_table}
        collection_class = type(name, (self.base,), collection_dict)
        mapper(collection_class, collection_table)
        self.table_classes[name] = collection_class

        # Adding the primary_key of the collection as field
        primary_key_field = self.table_classes[FIELD_TABLE](name=primary_key, collection=name,
                                                            type=FIELD_TYPE_STRING,
                                                            description="Primary_key of the document collection {0}".format(name))
        self.session.add(primary_key_field)

        if self.__caches:
            self.__documents[name] = {}
            self.__fields[name] = {}
            self.__fields[name][primary_key] = primary_key_field
            self.__names[name] = {}
            self.__names[name][primary_key] = primary_key
            self.__collections[name] = collection_row

        self.session.flush()

    def remove_collection(self, name):
        """
        Removes a collection
        :param name: collection to remove
        """

        # Checks
        collection_row = self.get_collection(name)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(name))

        # Removing the collection row
        self.session.query(self.table_classes[COLLECTION_TABLE]).filter(
            self.table_classes[COLLECTION_TABLE].name == name).delete()
        self.session.query(self.table_classes[FIELD_TABLE]).filter(
            self.table_classes[FIELD_TABLE].collection == name).delete()

        # Removing the collection document table + metadata associated
        collection_query = DropTable(self.table_classes[name].__table__)
        self.session.execute(collection_query)
        self.metadata.remove(self.table_classes[name].__table__)

        # Removing the class associated
        self.table_classes.pop(name, None)

        if self.__caches:
            self.__documents.pop(name, None)
            self.__fields.pop(name, None)
            self.__names.pop(name, None)
            self.__collections.pop(name, None)

        self.session.flush()

        # Base updated to remove the document table of the collection
        self.__update_table_classes()

    def get_collection(self, name):
        """
        Returns the collection row of the collection
        :param name: collection name
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
                self.table_classes[COLLECTION_TABLE].name == name).first()
            return collection_row

    def get_collections_names(self):
        """
        Gives the list of collection names
        :return: list of document names of the collection
        """

        collections = self.session.query(self.table_classes[COLLECTION_TABLE].name).all()
        collections_list = [collection.name for collection in collections]
        return collections_list

    def get_collections(self):
        """
        Gives the list of collection rows
        :return: list of document rows of the collection
        """

        return self.session.query(self.table_classes[COLLECTION_TABLE]).all()

    """ FIELDS """

    def add_fields(self, fields):
        """
        Adds the list of fields
        :param fields: list of fields (collection, name, type, description)
        """

        collections = []

        for field in fields:

            # Adding each field
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
        Adds a field to the database, if it does not already exist
        :param collection: field collection (str)
        :param name: field name (str)
        :param field_type: field type (string, int, float, boolean, date, datetime,
                     time, list_string, list_int, list_float, list_boolean, list_date,
                     list_datetime, or list_time)
        :param description: field description (str or None) => None by default
        :param flush: bool to know if the table classes must be updated (put False if in the middle of filling fields) => True by default
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_row = self.get_field(collection, name)
        if field_row is not None:
            raise ValueError("A field with the name {0} already exists in the collection {1}".format(name, collection))
        if not isinstance(name, str):
            raise ValueError("The field name must be of type {0}, but field name of type {1} given".format(str, type(name)))
        if not field_type in ALL_TYPES:
            raise ValueError("The field type must be in {0}, but {1} given".format(ALL_TYPES, field_type))
        if not isinstance(description, str) and description is not None:
            raise ValueError(
                "The field description must be of type {0} or None, but field description of type {1} given".format(str, type(description)))

        # Adding the field in the field table
        field_row = self.table_classes[FIELD_TABLE](name=name, collection=collection, type=field_type,
                                                    description=description)

        if self.__caches:
            self.__fields[collection][name] = field_row
            self.__names[collection][name] = hashlib.md5(name.encode('utf-8')).hexdigest()

        self.session.add(field_row)

        # Fields creation
        if field_type in LIST_TYPES:
            if self.list_tables:
                table = 'list_%s_%s' % (collection, self.field_name_to_column_name(collection, name))
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
            field_type = self.field_type_to_column_type(field_type)

        column = Column(self.field_name_to_column_name(collection, name), field_type, index=index)
        column_str_type = column.type.compile(self.database.engine.dialect)
        column_name = column.compile(dialect=self.database.engine.dialect)

        # Column created in document table, and in initial table if initial values are used

        document_query = str('ALTER TABLE %s ADD COLUMN %s %s' %
                             (collection, column_name, column_str_type))
        self.session.execute(document_query)
        self.table_classes[collection].__table__.append_column(column)

        # Redefinition of the table classes
        if flush:
            self.session.flush()

            # Classes reloaded in order to add the new column attribute
            self.__update_table_classes()

            if self.__caches:
                self.__refresh_cache_documents(collection)

        self.unsaved_modifications = True

    @staticmethod
    def field_type_to_column_type(field_type):
        """
        Gives the sqlalchemy column type corresponding to the field type
        :param field_type: column type
        :return: The sql column type given the field type
        """

        return TYPE_TO_COLUMN[field_type]

    def field_name_to_column_name(self, collection, field):
        """
        Transforms the field name into a valid and unique column name, by hashing it
        :param collection: field collection (str)
        :param field: field name (str)
        :return: Valid and unique (hashed) column name
        """

        if self.__caches:
            return self.__names[collection][field]
        else:
            primary_key = self.get_collection(collection).primary_key
            if field == primary_key:
                return field
            else:
                field_name = hashlib.md5(field.encode('utf-8')).hexdigest()
                return field_name

    def remove_field(self, collection, field):
        """
        Removes a field in the collection
        :param collection: field collection
        :param field: field name (str), or list of fields (str)
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_rows = []
        if isinstance(field, list):
            for field_elem in field:
                field_row = self.get_field(collection, field_elem)
                if field_row is None:
                    raise ValueError("The field with the name {0} does not exist in the collection {1}".format(field_elem, collection))
                else:
                    field_rows.append(field_row)
        else:
            field_row = self.get_field(collection, field)
            if field_row is None:
                raise ValueError("The field with the name {0} does not exist in the collection {1}".format(field, collection))
            else:
                field_rows.append(field_row)

        field_names = []
        if isinstance(field, list):
            for field_elem in field:
                field_names.append(self.field_name_to_column_name(collection, field_elem))
        else:
            field_names.append(self.field_name_to_column_name(collection, field))

        # Field removed from document table
        old_document_table = Table(collection, self.metadata)
        select = sql.select(
            [c for c in old_document_table.c if c.name not in str(field_names)])

        remaining_columns = [copy.copy(c) for c in old_document_table.columns
                             if c.name not in str(field_names)]

        # Creation of backup table, not containing the column
        document_backup_table = Table(collection + "_backup", self.metadata)

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
        new_document_table = Table(collection, self.metadata)
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
                        table = 'list_%s_%s' % (collection, self.field_name_to_column_name(collection, field_elem))
                        collection_query = DropTable(self.table_classes[table].__table__)
                        self.session.execute(collection_query)
                        self.metadata.remove(self.table_classes[table].__table__)

            else:
                if self.get_field(collection, field).type in LIST_TYPES:
                    table = 'list_%s_%s' % (collection, self.field_name_to_column_name(collection, field))
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
                    self.__names[collection].pop(field_elem, None)
            else:
                self.__fields[collection].pop(field, None)
                self.__names[collection].pop(field, None)

        self.unsaved_modifications = True

    def get_field(self, collection, name):
        """
        Gives the column row given a column name and a collection
        :param collection: document collection
        :param name: column name
        :return: The column row if the column exists, None otherwise
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
                self.table_classes[FIELD_TABLE].name == name).filter(
                self.table_classes[FIELD_TABLE].collection == collection).first()
            return field_row

    def get_fields_names(self, collection):
        """
        Gives the list of fields, given a collection
        :param collection: fields collection
        :return: List of fields names of the collection
        """

        fields = self.session.query(self.table_classes[FIELD_TABLE].name).filter(
            self.table_classes[FIELD_TABLE].collection == collection).all()

        fields_names = [field.name for field in fields]

        return fields_names

    def get_fields(self, collection):
        """
        Gives the list of fields rows, given a collection
        :param collection: fields collection
        :return: List of fields rows of the colletion
        """

        fields = self.session.query(self.table_classes[FIELD_TABLE]).filter(
            self.table_classes[FIELD_TABLE].collection == collection).all()
        return fields

    """ VALUES """

    def get_value(self, collection, document, field):
        """
        Gives the current value of <document, field> in the collection
        :param collection: Document collection (str)
        :param document: Document name (str)
        :param field: Field name (str)
        :return: The current value of <document, field> in the collection if it exists, None otherwise
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return None
        field_row = self.get_field(collection, field)
        if field_row is None:
            return None
        document_row = self.get_document(collection, document)
        if document_row is None:
            return None

        return FieldRow(self, collection, document_row)[field]

    def set_value(self, collection, document, field, new_value, flush=True):
        """
        Sets the value associated to <document, field>
        :param collection: document collection (str)
        :param document: document name (str)
        :param field: Field name (str)
        :param new_value: new value
        :param flush: bool to know if flush to do
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_row = self.get_field(collection, field)
        if field_row is None:
            raise ValueError("The field with the name {0} does not exist in the collection {1}".format(field, collection))
        document_row = self.get_document(collection, document)
        if document_row is None:
            raise ValueError("The document with the name {0} does not exist in the collection {1}".format(document, collection))
        if not self.check_type_value(new_value, field_row.type):
            raise ValueError("The value {0} is invalid for the type {1}".format(new_value, field_row.type))

        column_name = self.field_name_to_column_name(collection, field)
        new_column = self.python_to_column(field_row.type, new_value)

        if field != collection_row.primary_key:
            setattr(document_row.row, column_name, new_column)
        else:
            raise ValueError("Impossible to set the primary_key value of a document")

        if self.list_tables and isinstance(new_value, list):
            primary_key = self.get_collection(collection).primary_key
            document_id = document_row[primary_key]
            table_name = 'list_%s_%s' % (collection, column_name)

            table = self.metadata.tables[table_name]
            sql = table.delete(table.c.document_id == document_id)
            self.session.execute(sql)

            sql = table.insert()
            sql_params = []
            cvalues = [self.python_to_column(field_row.type[5:], i) for i in new_value]
            index = 0
            for i in cvalues:
                sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                index += 1
            if sql_params:
                self.session.execute(sql, params=sql_params)

        if flush:
            self.session.flush()

        self.unsaved_modifications = True

    def set_values(self, collection, document, values, flush=True):
        """
        Sets the values of a collection document
        :param collection: document collection (str)
        :param document: document name (str)
        :param values: Dictionary of values (key=field, value=value)
        :param flush: bool to know if flush to do
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        document_row = self.get_document(collection, document)
        if document_row is None:
            raise ValueError("The document with the name {0} does not exist in the collection {1}".format(document, collection))
        for field in values:
            field_row = self.get_field(collection, field)
            if field_row is None:
                raise ValueError("The field with the name {0} does not exist in the collection {1}".format(field, collection))
            if not self.check_type_value(values[field], field_row.type):
                raise ValueError("The value {0} is invalid for the type {1}".format(values[field], field_row.type))

        database_values = {}
        for field in values:
            column_name = self.field_name_to_column_name(collection, field)
            field_row = self.get_field(collection, field)
            new_column = self.python_to_column(field_row.type, values[field])
            database_values[column_name] = new_column
            if collection_row.primary_key == field:
                raise ValueError("Impossible to set the primary_key value of a document")

        # Updating all values
        for column in database_values:
            setattr(document_row.row, column, database_values[column])

        # Updating list tables values
        for field in values:
            field_row = self.get_field(collection, field)
            database_value = self.python_to_column(field_row.type, values[field])
            if self.list_tables and isinstance(database_value, list):
                primary_key = self.get_collection(collection).primary_key
                document_id = getattr(document_row.row, primary_key)
                table_name = 'list_%s_%s' % (collection, column)

                table = self.metadata.tables[table_name]
                sql = table.delete(table.c.document_id == document_id)
                self.session.execute(sql)

                sql = table.insert()
                sql_params = []
                cvalues = [self.python_to_column(field_row.type[5:], i) for i in database_value]
                index = 0
                for i in cvalues:
                    sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                    index += 1
                if sql_params:
                    self.session.execute(sql, params=sql_params)

        # TODO set list tables values

        if flush:
            self.session.flush()

        self.unsaved_modifications = True

    def remove_value(self, collection, document, field, flush=True):
        """
        Removes the value associated to <document, field> in the collection
        :param collection: document collection (str)
        :param document: document name (str)
        :param field: field name (str)
        :param flush: boolean to know if flush to do (put False in the middle of removing values) => True by default
        """

        # Checks
        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        field_row = self.get_field(collection, field)
        if field_row is None:
            raise ValueError("The field with the name {0} does not exist in the collection {1}".format(field, collection))
        document_row = self.get_document(collection, document)
        if document_row is None:
            raise ValueError("The document with the name {0} does not exist in the collection {1}".format(document, collection))

        sql_column_name = self.field_name_to_column_name(collection, field)
        old_value = getattr(document_row.row, sql_column_name)
        setattr(document_row.row, sql_column_name, None)

        if self.list_tables and field_row.type.startswith('list_'):
            primary_key = self.get_collection(collection).primary_key
            document_id = document_row[primary_key]
            table_name = 'list_%s_%s' % (collection, sql_column_name)
            table = self.metadata.tables[table_name]
            sql = table.delete(table.c.document_id == document_id)
            self.session.execute(sql)

        if flush:
            self.session.flush()
        self.unsaved_modifications = True

    def check_type_value(self, value, valid_type):
        """
        Checks the type of the value
        :param value: value
        :param type: type that the value is supposed to have
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
                if not self.check_type_value(value_element, valid_type.replace("list_", "")):
                    return False
            return True
        return False

    def new_value(self, collection, document, field, value, checks=True):
        """
        Adds a value for <document, field>
        :param collection: document collection
        :param document: document name
        :param field: Field name
        :param value: value
        :param checks: boolean to know if flush to do and value check (Put False in the middle of adding values, during import)
        """

        collection_row = self.get_collection(collection)
        field_row = self.get_field(collection, field)
        document_row = self.get_document(collection, document)

        if checks:
            if collection_row is None:
                raise ValueError("The collection {0} does not exist".format(collection))
            if field_row is None:
                raise ValueError("The field with the name {0} does not exist in the collection {1}".format(field, collection))
            if document_row is None:
                raise ValueError("The document with the name {0} does not exist in the collection {1}".format(document, collection))
            if not self.check_type_value(value, field_row.type):
                raise ValueError("The value {0} is invalid for the type {1}".format(value, field_row.type))

        field_name = self.field_name_to_column_name(collection, field)
        database_value = getattr(
            document_row, field_name)

        # We add the value only if it does not already exist
        if database_value is None:
            if value is not None:
                current_value = self.python_to_column(
                    field_row.type, value)
                setattr(
                    document_row.row, field_name,
                    current_value)
                if self.list_tables and isinstance(value, list):
                    primary_key = self.get_collection(collection).primary_key
                    document_id = document_row[primary_key]
                    table = 'list_%s_%s' % (collection, field_name)
                    sql = self.metadata.tables[table].insert()
                    sql_params = []
                    cvalues = [self.python_to_column(field_row.type[5:], i) for i in value]
                    index = 0
                    for i in cvalues:
                        sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                        index += 1
                    if sql_params:
                        self.session.execute(sql, params=sql_params)

            if checks:
                self.session.flush()
            self.unsaved_modifications = True

        else:
            raise ValueError("The tuple <{0}, {1}> already has a value in the collection {2}".format(field, document, collection))

    """ DOCUMENTS """

    def get_document(self, collection, document):
        """
        Gives the document row of a document, given a collection
        :param collection: document collection
        :param document: document name
        :return The document row if the document exists, None otherwise
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return None
        if self.__caches:
            try:
                return self.__documents[collection][document]
            except KeyError:
                return None
        else:
            primary_key = collection_row.primary_key
            column = getattr(self.table_classes[collection], primary_key)
            value = column.type.python_type(document)
            query = self.session.query(self.table_classes[collection]).filter(
                column == value)
            document_row = query.first()
            if document_row is not None:
                document_row = FieldRow(self, collection, document_row)
            return document_row

    def get_documents_names(self, collection):
        """
        Gives the list of document names, given a collection
        :param collection: documents collection
        :return: list of document names of the collection
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return []
        else:
            documents = self.session.query(getattr(self.table_classes[collection], collection_row.primary_key)).all()
            documents_list = [getattr(document, collection_row.primary_key) for document in documents]
            return documents_list

    def get_documents(self, collection):
        """
        Gives the list of document rows, given a collection
        :param collection: documents collection
        :return: list of document rows of the collection
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            return []
        else:
            documents = self.session.query(self.table_classes[collection]).all()
            documents_list = [FieldRow(self, collection, document) for document in documents]
            return documents_list

    def remove_document(self, collection, document):
        """
        Removes a document in the collection
        :param collection: document collection (str)
        :param document: document name (str)
        """

        collection_row = self.get_collection(collection)
        if collection_row is None:
            raise ValueError("The collection {0} does not exist".format(collection))
        document_row = self.get_document(collection, document)
        if document_row is None:
            raise ValueError("The document with the name {0} does not exist in the collection {1}".format(document, collection))
        primary_key = collection_row.primary_key

        self.session.query(self.table_classes[collection]).filter(
            getattr(self.table_classes[collection], primary_key) == document).delete()

        # Removing document from list tables
        if self.list_tables:
            for table in self.table_classes:
                if "list" in table:
                    self.session.query(self.table_classes[table]).filter(
                        self.table_classes[table].document_id == document).delete()

        if self.__caches:
            self.__documents[collection].pop(document, None)

        self.session.flush()
        self.unsaved_modifications = True

    def add_document(self, collection, document, flush=True):
        """
        Adds a document to a collection
        :param collection: document collection (str)
        :param document: dictionary of document values (dict), or document primary_key (str)
        :param flush: boolean to know if flush to do, put False in the middle of filling the table => True by default
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
            raise ValueError("The primary_key {0} of the collection {1} is missing from the document dictionary".format(primary_key, collection))
        if isinstance(document, dict):
            document_row = self.get_document(collection, document[primary_key])
        else:
            document_row = self.get_document(collection, document)
        if document_row is not None:
            raise ValueError("A document with the name {0} already exists in the collection {1}".format(document, collection))

        if not isinstance(document, dict):
            document = {primary_key: document}

        document_id = document[primary_key]
        column_values = {primary_key: document_id}
        lists = []
        for k, v in document.items():
            column_name = self.field_name_to_column_name(collection, k)
            field_type = self.get_field(collection, k).type
            column_value = self.python_to_column(field_type, v)
            column_values[column_name] = column_value
            if self.list_tables and isinstance(v, list):
                table = 'list_%s_%s' % (collection, column_name)
                # sql = sql_text('INSERT INTO %s (document_id, i, value) VALUES (:document_id, :i, :value)' % table)
                sql = self.metadata.tables[table].insert()
                sql_params = []
                cvalues = [self.python_to_column(field_type[5:], i) for i in v]
                index = 0
                for i in cvalues:
                    sql_params.append({'document_id': document_id, 'i': index, 'value': i})
                    index += 1
                lists.append((sql, sql_params))

        if self.list_tables:
            for sql, sql_params in lists:
                if sql_params:
                    self.session.execute(sql, params=sql_params)

        document_row = self.table_classes[collection](**column_values)
        self.session.add(document_row)

        if self.__caches:
            document_row = FieldRow(self, collection, document_row)
            self.__documents[collection][document_id] = document_row

        if flush:
            self.session.flush()

        self.unsaved_modifications = True

    """ MODIFICATIONS """

    def save_modifications(self):
        """
        Saves the modifications by committing the session
        """
        self.session.commit()
        self.unsaved_modifications = False

    def unsave_modifications(self):
        """
        Unsaves the modifications by rolling back the session
        """

        self.session.rollback()
        self.unsaved_modifications = False
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

        return self.unsaved_modifications

    """ FILTERS """

    def filter_query(self, collection, filter, query_type=None):
        """
        Given a filter string, return a query that can be used with
        filter_documents() to select documents.
        :param query_type: type of query to build. Can be 'mixed', 
            'sql', 'python' or 'guess'. If None, self.query_type
            is used.
        """

        if query_type is None:
            query_type = self.query_type
        filter_to_query_class = populse_db.filter._filter_to_query_classes[query_type]
        tree = populse_db.filter.filter_parser().parse(filter)
        query = filter_to_query_class(self, collection).transform(tree)
        return query

    def filter_documents(self, collection, filter_query):
        """
        Iterate over documents selected by filter_query. Each item yield is a
        row of the column table returned by sqlalchemy. filter_query can be
        the result of self.filter_query() or a string containing a filter
        (in this case self.fliter_query() is called to get the actual query).
        """

        if isinstance(filter_query, six.string_types):
            filter_query = self.filter_query(collection, filter_query)
        if filter_query is None:
            select = self.metadata.tables[collection].select()
            python_filter = None
        elif isinstance(filter_query, types.FunctionType):
            select = self.metadata.tables[collection].select()
            python_filter = filter_query
        elif isinstance(filter_query, tuple):
            sql_condition, python_filter = filter_query
            select = select = self.metadata.tables[collection].select(
                sql_condition)
        else:
            select = self.metadata.tables[collection].select(
                filter_query)
            python_filter = None
        for row in self.session.execute(select):
            row = FieldRow(self, collection, row)
            if python_filter is None or python_filter(row):
                yield row

    """ UTILS """

    def python_to_column(self, column_type, value):
        """
        Convert a python value into a suitable value to put in a
        database column.
        """
        if isinstance(value, list):
            return self.list_to_column(column_type, value)
        elif isinstance(value, dict):
            return str(value)
        else:
            return value

    def column_to_python(self, column_type, value):
        """
        Convert a value of a database column into the corresponding
        Python value.
        """
        if column_type.startswith('list_'):
            return self.column_to_list(column_type, value)
        elif column_type == FIELD_TYPE_JSON:
            return ast.literal_eval(value)
        else:
            return value

    def list_to_column(self, column_type, value):
        """
        Convert a python list value into a suitable value to put in a
        database column.
        """
        converter = self._list_item_to_string.get(column_type)
        if converter is None:
            list_value = value
        else:
            list_value = [converter(i) for i in value]
        return repr(list_value)

    def column_to_list(self, column_type, value):
        """
        Convert a value of a database column into the corresponding
        Python list value.
        """
        if value is None:
            return None
        list_value = ast.literal_eval(value)
        converter = self._string_to_list_item.get(column_type)
        if converter is None:
            return list_value
        return [converter(i) for i in list_value]


class Undefined:
    pass


class FieldRow:
    '''
    A FieldRow is an object that makes it possible to access to attributes of
    a database row returned by sqlalchemy using the column name. If the
    attribute with the field name is not found, it is hashed and search in the
    actual row. If found, it is stored in the FieldRow instance.
    '''

    def __init__(self, database, collection, row):
        self.database = database
        self.collection = collection
        self.row = row
        primary_key = list(self.database.metadata.tables[collection].primary_key.columns.values())[0].name
        setattr(self, primary_key, getattr(self.row, primary_key))

    def __getattr__(self, name):
        try:
            return getattr(self.row, name)
        except AttributeError as e:
            hashed_name = hashlib.md5(name.encode('utf-8')).hexdigest()
            result = getattr(self.row, hashed_name, Undefined)
            if result is Undefined:
                raise
            result = self.database.column_to_python(
                self.database.get_field(self.collection, name).type, result)
            setattr(self, hashed_name, result)
            return result

    def __getitem__(self, name):
        return getattr(self, name)
