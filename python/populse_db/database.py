import ast
import copy
import hashlib
import os
import re
import types
from datetime import date, time, datetime

import dateutil.parser
import six
from sqlalchemy import (create_engine, Column, String,
                        MetaData, event, or_, Table, sql)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.schema import CreateTable, DropTable

from populse_db.database_model import (create_database, FIELD_TYPE_INTEGER,
                                       FIELD_TYPE_FLOAT, FIELD_TYPE_TIME,
                                       FIELD_TYPE_DATETIME, FIELD_TYPE_DATE,
                                       FIELD_TYPE_STRING, FIELD_TYPE_LIST_DATE,
                                       FIELD_TYPE_LIST_DATETIME,
                                       FIELD_TYPE_LIST_TIME, INITIAL_TABLE,
                                       LIST_TYPES, TYPE_TO_COLUMN, FIELD_TYPE_BOOLEAN,
                                       ALL_TYPES, DOCUMENT_PRIMARY_KEY, DOCUMENT_TABLE, FIELD_TABLE)
from populse_db.filter import filter_parser, FilterToQuery


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """
    Manages the pragmas during the database opening
    :param dbapi_connection:
    :param connection_record:
    """
    dbapi_connection.execute('pragma case_sensitive_like=ON')
    dbapi_connection.execute('pragma foreign_keys=ON')


class Database:

    """
    Database API

    attributes:
        - enigne: string engine of the database
        - classes: list of table classes, generated automatically
        - base: database base
        - engine: database engine
        - metadata: database metadata
        - session_maker: session manager
        - unsaved_modifications: to know if there are unsaved
          modifications in the database
        - documents: document rows
        - initial_documents: initial document rows
        - fields: fields rows
        - names: fields column names

    methods:
        - add_field: adds a field
        - add_fields: adds a list of fields
        - field_type_to_column_type: gives the column type corresponding
          to a field type
        - field_name_to_sql_column_name: gives the column name corresponding
          to the field name
        - remove_field: removes a field
        - get_field: Gives all fields rows
        - get_fields_names: gives all fields names
        - get_documents: gives all document rows
        - get_documents_names: gives all document names
        - get_current_value: gives the current value of <document, field>
        - get_initial_value: gives the initial value of <document, field>
        - is_value_modified: to know if a value has been modified
        - set_value: sets the value of <document, field>
        - reset_value: resets the value of <document, field>
        - remove_value: removes the value of <document, field>
        - check_type_value: checks the type of a value
        - add_value: adds a value to <document, field>
        - get_document: gives the document row given a document name
        - get_initial_document: gives the initial row given a document name
        - get_documents: gives all document rows
        - get_documents_names: gives all document names
        - add_document: adds a document
        - remove_document: removes a document
        - get_documents_matching_constraints: gives the documents matching the
          constraints given in parameter
        - get_documents_matching_search: gives the documents matching the
          search
        - get_documents_matching_advanced_search: gives the documents matching
          the advanced search
        - get_documents_matching_field_value_couples: gives the documents
          containing all <field, value> given in parameter
        - save_modifications: saves the pending modifications
        - unsave_modifications: unsaves the pending modifications
        - has_unsaved_modifications: to know if there are unsaved
          modifications
        - update_table_classes: redefines the model after schema update
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

    def __init__(self, string_engine, initial_table=False, document_caches=False):
        """
        Creates an API of the database instance
        :param string_engine: string engine of the database file, can be already existing, or not
        :param initial_table: to know if the initial table must be created, False by default
        :param document_caches: to know if the document caches must be used, False by default
        """

        self.string_engine = string_engine

        self.initial_table = initial_table
        self.documents_caches = document_caches

        # SQLite database: we create it if it does not exist
        if string_engine.startswith('sqlite'):
            self.db_file = re.sub("sqlite.*:///", "", string_engine)
            if not os.path.exists(self.db_file):
                parent_dir = os.path.dirname(self.db_file)
                if not os.path.exists(parent_dir):
                    os.makedirs(os.path.dirname(self.db_file))
                create_database(string_engine, self.initial_table)

        # Database opened
        self.engine = create_engine(self.string_engine)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.update_table_classes()

        # Database schema checked
        if (DOCUMENT_TABLE not in self.table_classes.keys() or
                FIELD_TABLE not in self.table_classes.keys()):
            raise ValueError(
                'The database schema is not coherent with the API')
        if self.initial_table and INITIAL_TABLE not in self.table_classes.keys():
            raise ValueError(
                'The initial_table flag cannot be True if the database has been created without the initial_table flag')

        self.session = scoped_session(sessionmaker(
            bind=self.engine, autocommit=False, autoflush=False))

        self.unsaved_modifications = False

        columns = self.session.query(self.table_classes[FIELD_TABLE])
        self.fields = dict((getattr(column, DOCUMENT_PRIMARY_KEY), column) for column in columns)

        self.names = {}

        # name is the only column not hashed
        self.names[DOCUMENT_PRIMARY_KEY] = DOCUMENT_PRIMARY_KEY

        if self.documents_caches:
            self.documents = {}
            if self.initial_table:
                self.initial_documents = {}

    """ FIELDS """

    def add_fields(self, fields):
        """
        Adds the list of fields
        :param fields: list of fields (name, type, description)
        """

        for field in fields:

            # Adding each field
            self.add_field(field[0], field[1], field[2], False)

        # Updating the table classes
        self.session.flush()
        self.update_table_classes()

    def add_field(self, name, field_type, description, flush=True):
        """
        Adds a field to the database, if it does not already exist
        :param name: field name (str)
        :param field_type: field type (string, int, float, boolean, date, datetime,
                     time, list_string, list_int, list_float, list_boolean, list_date,
                     list_datetime, or list_time)
        :param description: field description (str or None)
        :param flush: bool to know if the table classes must be updated (put False if in the middle of filling fields)
        """

        if not isinstance(name, str):
            raise ValueError("The field name must be of type " + str(str) +
                             ", but field name of type " + str(type(name)) + " given")
        field_row = self.get_field(name)
        if field_row != None:
            raise ValueError("A field with the name " +
                             str(name) + " already exists")
        if not field_type in ALL_TYPES:
            raise ValueError("The field type must be in " + str(ALL_TYPES) + ", but " + str(
                field_type) + " given")
        if not isinstance(description, str) and description is not None:
            raise ValueError(
                "The field description must be of type " + str(str) + " or None, but field description of type " + str(
                    type(description)) + " given")

        # Adding the field in the field table
        field_row = self.table_classes[FIELD_TABLE](name=name, type=field_type, description=description)

        self.session.add(field_row)
        self.fields[name] = field_row

        # Fields creation
        if field_type in LIST_TYPES:
            # String columns if it list type, as the str representation of the lists will be stored
            field_type = String
        else:
            field_type = self.field_type_to_column_type(field_type)
        column = Column(self.field_name_to_column_name(name), field_type)
        column_str_type = column.type.compile(self.engine.dialect)
        column_name = column.compile(dialect=self.engine.dialect)

        # Column created in document table, and in initial table if initial values are used

        document_query = str('ALTER TABLE %s ADD COLUMN %s %s' %
                         (DOCUMENT_TABLE, column_name, column_str_type))
        self.session.execute(document_query)
        self.table_classes[DOCUMENT_TABLE].__table__.append_column(column)

        if self.initial_table:
            column_initial = Column(
                self.field_name_to_column_name(name), field_type)
            initial_query = str('ALTER TABLE %s ADD COLUMN %s %s' % (
                INITIAL_TABLE, column_name, column_str_type))
            self.session.execute(initial_query)
            self.table_classes[INITIAL_TABLE].__table__.append_column(
                column_initial)

        if self.documents_caches:
            self.documents.clear()
            if self.initial_table:
                self.initial_documents.clear()

        self.unsaved_modifications = True

        # Redefinition of the table classes
        if flush:
            self.session.flush()
            self.update_table_classes()

    def field_type_to_column_type(self, field_type):
        """
        Gives the sqlalchemy column type corresponding to the field type
        :param field_type: column type
        :return: The sql column type given the field type
        """

        return TYPE_TO_COLUMN[field_type]

    def field_name_to_column_name(self, name):
        """
        Transforms the field name into a valid and unique column name, by hashing it
        :param name: field name (str)
        :return: Valid and unique (hashed) column name
        """

        if name in self.names:
            return self.names[name]
        else:
            field_name = hashlib.md5(name.encode('utf-8')).hexdigest()
            self.names[name] = field_name
            return field_name

    def remove_field(self, name):
        """
        Removes a field
        :param name: field name (str)
        """

        if not isinstance(name, str):
            raise ValueError(
                "The field name must be of type " + str(str) + ", but field name of type " + str(type(name)) + " given")
        field_row = self.get_field(name)
        if field_row is None:
            raise ValueError("The field with the name " +
                             str(name) + " does not exist")

        field_name = self.field_name_to_column_name(name)

        # Field removed from document table
        old_document_table = Table(DOCUMENT_TABLE, self.metadata)
        select = sql.select(
            [c for c in old_document_table.c if field_name not in c.name])

        remaining_columns = [copy.copy(c) for c in old_document_table.columns
                             if field_name not in c.name]

        document_backup_table = Table(DOCUMENT_TABLE + "_backup", self.metadata)
        for column in old_document_table.columns:
            if field_name not in str(column):
                document_backup_table.append_column(column.copy())
        self.session.execute(CreateTable(document_backup_table))

        insert = sql.insert(document_backup_table).from_select(
            [getattr(c, DOCUMENT_PRIMARY_KEY) for c in remaining_columns], select)
        self.session.execute(insert)

        self.metadata.remove(old_document_table)
        self.session.execute(DropTable(old_document_table))

        new_document_table = Table(DOCUMENT_TABLE, self.metadata)
        for column in document_backup_table.columns:
            new_document_table.append_column(column.copy())

        self.session.execute(CreateTable(new_document_table))

        select = sql.select(
            [c for c in document_backup_table.c if field_name not in c.name])
        insert = sql.insert(new_document_table).from_select(
            [getattr(c, DOCUMENT_PRIMARY_KEY) for c in remaining_columns], select)
        self.session.execute(insert)

        self.session.execute(DropTable(document_backup_table))

        # Field removed from initial table if initial values are used
        if self.initial_table:
            old_initial_table = Table(INITIAL_TABLE, self.metadata)
            select = sql.select(
                [c for c in old_initial_table.c if field_name not in c.name])

            remaining_columns = [copy.copy(c) for c in old_initial_table.columns
                                 if field_name not in c.name]

            initial_backup_table = Table(
                INITIAL_TABLE + "_backup", self.metadata)

            for column in old_initial_table.columns:
                if field_name not in str(column):
                    initial_backup_table.append_column(column.copy())
            self.session.execute(CreateTable(initial_backup_table))

            insert = sql.insert(initial_backup_table).from_select(
                [getattr(c, DOCUMENT_PRIMARY_KEY) for c in remaining_columns], select)
            self.session.execute(insert)

            self.metadata.remove(old_initial_table)
            self.session.execute(DropTable(old_initial_table))

            new_initial_table = Table(INITIAL_TABLE, self.metadata)
            for column in initial_backup_table.columns:
                new_initial_table.append_column(column.copy())

            self.session.execute(CreateTable(new_initial_table))

            select = sql.select(
                [c for c in initial_backup_table.c if field_name not in c.name])
            insert = sql.insert(new_initial_table).from_select(
                [getattr(c, DOCUMENT_PRIMARY_KEY) for c in remaining_columns], select)
            self.session.execute(insert)

            self.session.execute(DropTable(initial_backup_table))

        if self.documents_caches:
            self.documents.clear()
            if self.initial_table:
                self.initial_documents.clear()

        self.fields.pop(name, None)

        self.session.delete(field_row)

        self.session.flush()

        self.update_table_classes()

        self.unsaved_modifications = True

    def get_field(self, name):
        """
        Gives the column row given a column name
        :param name: column name
        :return: The column row if the column exists, None otherwise
        """
        return self.fields.get(name)

    def get_fields_names(self):
        """
        Gives the list of fields
        :return: List of fields names
        """
        return list(column.name for column in self.get_fields())

    def get_fields(self):
        """
        Gives the list of fields rows
        :return: List of fields rows
        """
        return self.fields.values()

    """ VALUES """

    def get_current_value(self, document, field):
        """
        Gives the current value of <document, field>
        :param document: Document name (str)
        :param field: Field name (str)
        :return: The current value of <document, field> if it exists, None otherwise
        """

        document_row = self.get_field(field)
        if document_row is None:
            return None
        field_row = self.get_document(document)
        if field_row is None:
            return None

        return FieldRow(self, field_row)[field]

    def get_initial_value(self, document, field):
        """
        Gives the initial value of <document, field>
        :param document: Document name
        :param field: Field name
        :return: The initial value of <document, field> if it exists, None otherwise
        """

        field_row = self.get_field(field)
        if field_row is None:
            return None
        initial_row = self.get_initial_document(document)
        if initial_row is None:
            return None

        return FieldRow(self, initial_row)[field]

    def is_value_modified(self, document, field):
        """
        To know if the value <document, field> has been modified
        :param document: document name
        :param field: Field name
        :return: True if the value <document, field> has been modified, False otherwise
        """

        field_row = self.get_field(field)
        if field_row is None:
            return False
        document_row = self.get_document(document)
        if document_row is None:
            return False

        return (self.get_current_value(document, field) !=
                self.get_initial_value(document, field))

    def set_current_value(self, document, field, new_value):
        """
        Sets the value associated to <document, column>
        :param document: document name
        :param field: Field name
        :param new_value: new value
        """

        field_row = self.get_field(field)
        if field_row is None:
            raise ValueError("The field with the name " +
                             str(field) + " does not exist")
        document_row = self.get_document(document)
        if document_row is None:
            raise ValueError("The document with the name " +
                             str(document) + " does not exist")
        if not self.check_type_value(new_value, field_row.type):
            raise ValueError("The value " + str(new_value) + " is invalid")

        new_value = self.python_to_column(field_row.type, new_value)

        setattr(document_row.row, self.field_name_to_column_name(field), new_value)

        self.session.flush()
        self.unsaved_modifications = True

    def reset_current_value(self, document, field):
        """
        Resets the value associated to <document, field>
        :param document: document name
        :param field: Field name
        """
        field_row = self.get_field(field)
        if field_row is None:
            raise ValueError("The field with the name " +
                             str(field) + " does not exist")
        document_row = self.get_document(document)
        if document_row is None:
            raise ValueError("The document with the name " +
                             str(document) + " does not exist")
        if not self.initial_table:
            raise ValueError(
                "Impossible to reset values if the initial values are not activated, you can activate the flag initial_table when creating the Database instance")

        initial_value = self.get_initial_value(document, field)

        if field_row.type in LIST_TYPES:
            initial_value = str(initial_value)

        setattr(document_row.row, self.field_name_to_column_name(field), initial_value)

        self.session.flush()
        self.unsaved_modifications = True

    def remove_value(self, document, field, flush=True):
        """
        Removes the value associated to <document, field>
        :param document: document name
        :param field: Field name
        :param flush: To know if flush to do (put False in the middle of removing values)
        """

        field_row = self.get_field(field)
        if field_row is None:
            raise ValueError("The field with the name " +
                             str(field) + " does not exist")
        document_row = self.get_document(document)
        if document_row is None:
            raise ValueError("The document with the name " +
                             str(document) + " does not exist")

        sql_column_name = self.field_name_to_column_name(field)

        setattr(document_row.row, sql_column_name, None)

        if self.initial_table:
            initial_row = self.get_initial_document(document)
            setattr(initial_row.row, sql_column_name, None)

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

    def new_value(self, document, field, current_value, initial_value=None, checks=True):
        """
        Adds a value for <document, field>
        :param document: document name
        :param field: Field name
        :param current_value: current value
        :param initial_value: initial value (initial values must be activated)
        :param checks: bool to know if flush to do and value check (Put False in the middle of adding values, during import)
        """

        field_row = self.get_field(field)
        document_row = self.get_document(document)
        if checks:
            if field_row is None:
                raise ValueError("The field with the name " +
                                 str(field) + " does not exist")
            if document_row is None:
                raise ValueError("The document with the name " +
                                 str(document) + " does not exist")
            if not self.check_type_value(current_value, field_row.type):
                raise ValueError("The current value " +
                                 str(current_value) + " is invalid")
            if not self.check_type_value(initial_value, field_row.type):
                raise ValueError("The initial value " +
                                 str(initial_value) + " is invalid")
            if not self.initial_table and not initial_value is None:
                raise ValueError(
                    "Impossible to add an initial value if the initial values are not activated, you can activate the flag initial_table when creating the Database instance")

        field_name = self.field_name_to_column_name(field)
        database_current_value = getattr(
            document_row, field_name)

        if self.initial_table:
            initial_row = self.get_initial_document(document)
            database_initial_value = getattr(
                initial_row, field_name)
        else:
            database_initial_value = None

        # We add the value only if it does not already exist
        if (database_current_value is None and
                database_initial_value is None):
            if initial_value is not None:
                initial_value = self.python_to_column(
                    field_row.type, initial_value)
                setattr(
                    initial_row.row, field_name,
                    initial_value)
            if current_value is not None:
                current_value = self.python_to_column(
                    field_row.type, current_value)
                setattr(
                    document_row.row, field_name,
                    current_value)

            if checks:
                self.session.flush()
            self.unsaved_modifications = True

        else:
            raise ValueError("The tuple <" + str(field) + ", " +
                             str(document) + "> already has a value")

    """ DOCUMENTS """

    def get_document(self, document):
        """
        Gives the document row of a document
        :param document: document name
        :return The document row if the document exists, None otherwise
        """

        if self.documents_caches and document in self.documents:
            return self.documents[document]
        else:
            document_row = self.session.query(self.table_classes[DOCUMENT_TABLE]).filter(
                getattr(self.table_classes[DOCUMENT_TABLE], DOCUMENT_PRIMARY_KEY) == document).first()
            if document_row is not None:
                document_row = FieldRow(self, document_row)
            if self.documents_caches:
                self.documents[document] = document_row
            return document_row

    def get_initial_document(self, document):
        """
        Gives the initial row of a document (initial values must be activated)
        :param document: document name
        :return The initial row if the document exists, None otherwise
        """

        if not self.initial_table:
            raise ValueError(
                "The initial values aren't activated, you can activate the flag initial_table when creating the Database instance")
        if self.documents_caches and document in self.initial_documents:
            return self.initial_documents[document]
        else:
            document_row = self.session.query(self.table_classes[INITIAL_TABLE]).filter(
                getattr(self.table_classes[INITIAL_TABLE], DOCUMENT_PRIMARY_KEY) == document).first()
            if document_row is not None:
                document_row = FieldRow(self, document_row)
            if self.documents_caches:
                self.initial_documents[document] = document_row
            return document_row

    def get_documents_names(self):
        """
        Gives the list of document names
        :return: list of document names
        """

        documents_list = []
        documents = self.session.query(self.table_classes[DOCUMENT_TABLE]).all()
        for document in documents:
            documents_list.append(document.name)
        return documents_list

    def get_documents(self):
        """
        Gives the list of document rows
        :return: list of document rows
        """

        documents_list = []
        documents = self.session.query(self.table_classes[DOCUMENT_TABLE]).all()
        for document in documents:
            documents_list.append(FieldRow(self, document))
        return documents_list

    def remove_document(self, document):
        """
        Removes a document
        :param document: document name
        """

        document_row = self.get_document(document)
        if document_row is None:
            raise ValueError("The document with the name " +
                             str(document) + " does not exist")

        self.session.query(self.table_classes[DOCUMENT_TABLE]).filter(
                getattr(self.table_classes[DOCUMENT_TABLE], DOCUMENT_PRIMARY_KEY) == document).delete()
        if self.initial_table:
            self.session.query(self.table_classes[INITIAL_TABLE]).filter(
                getattr(self.table_classes[INITIAL_TABLE], DOCUMENT_PRIMARY_KEY) == document).delete()

        if self.documents_caches:
            self.documents[document] = None
            self.initial_documents[document] = None

        self.session.flush()
        self.unsaved_modifications = True

    def add_document(self, document, checks=True):
        """
        Adds a document
        :param document: document name
        :param checks: checks if the document already exists and flushes, put False in the middle of filling the table
        """

        if checks:
            document_row = self.get_document(document)
            if document_row is not None:
                raise ValueError("A document with the name " +
                                 str(document) + " already exists")
        if not isinstance(document, str):
            raise ValueError(
                "The document name must be of type " + str(str) + ", but document name of type " + str(
                    type(document)) + " given")

        # Adding the index to document table
        document_row = self.table_classes[DOCUMENT_TABLE](name=document)
        self.session.add(document_row)

        if self.documents_caches:
            document_row = FieldRow(self, document_row)
            self.documents[document] = document_row

        # Adding the index to initial table if initial values are used
        if self.initial_table:
            initial_row = self.table_classes[INITIAL_TABLE](name=document)
            self.session.add(initial_row)

            if self.documents_caches:
                initial_row = FieldRow(self, initial_row)
                self.initial_documents[document] = initial_row

        if checks:
            self.session.flush()

        self.unsaved_modifications = True

    """ UTILS """

    def get_documents_matching_search(self, search, fields):
        """
        Returns the list of documents names matching the search
        :param search: search to match (str)
        :param fields: list of fields taken into account
        :return: List of document names matching the search
        """

        if not isinstance(fields, list):
            return []
        if not isinstance(search, str):
            return []
        for field in fields:
            field_row = self.get_field(field)
            if field_row is None:
                return []

        documents_matching = []
        simple_columns_filters = []

        # Iterating over all values and finding matches

        values = self.session.query(getattr(self.table_classes[DOCUMENT_TABLE], DOCUMENT_PRIMARY_KEY))

        # Search for each field
        for column in fields:

            simple_columns_filters.append(getattr(
                self.table_classes[DOCUMENT_TABLE], self.field_name_to_column_name(column)).like("%" + search + "%"))

        values = values.filter(or_(*simple_columns_filters)).distinct().all()
        for value in values:
            documents_matching.append(getattr(value, DOCUMENT_PRIMARY_KEY))

        return documents_matching

    def start_transaction(self):
        """
        Starts a new transaction
        """

        self.session.begin_nested()

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

    def has_unsaved_modifications(self):
        """
        Knowing if the database has pending modifications that are
        unsaved
        :return: True if there are pending modifications to save,
                 False otherwise
        """

        return self.unsaved_modifications

    def update_table_classes(self):
        """
        Redefines the model after an update of the schema
        """

        self.table_classes = {}
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(engine=self.engine)

        for table in self.metadata.tables.values():
            self.table_classes[table.name] = getattr(
                self.base.classes, table.name)

    def filter_query(self, filter):
        """
        Given a filter string, return a query that can be used with
        filter_documents() to select documents.
        """

        tree = filter_parser().parse(filter)
        query = FilterToQuery(self).transform(tree)
        return query

    def filter_documents(self, filter_query):
        """
        Iterate over documents selected by filter_query. Each item yield is a
        row of the column table returned by sqlalchemy. filter_query can be
        the result of self.filter_query() or a string containing a filter
        (in this case self.fliter_query() is called to get the actual query).
        """

        if isinstance(filter_query, six.string_types):
            filter_query = self.filter_query(filter_query)
        if filter_query is None:
            select = self.metadata.tables[DOCUMENT_TABLE].select()
            python_filter = None
        elif isinstance(filter_query, types.FunctionType):
            select = self.metadata.tables[DOCUMENT_TABLE].select()
            python_filter = filter_query
        elif isinstance(filter_query, tuple):
            sql_condition, python_filter = filter_query
            select = select = self.metadata.tables[DOCUMENT_TABLE].select(
                sql_condition)
        else:
            select = select = self.metadata.tables[DOCUMENT_TABLE].select(
                filter_query)
            python_filter = None
        for row in self.session.execute(select):
            row = FieldRow(self, row)
            if python_filter is None or python_filter(row):
                yield row

    def python_to_column(self, column_type, value):
        """
        Convert a python value into a suitable value to put in a
        database column.
        """
        if isinstance(value, list):
            return self.list_to_column(column_type, value)
        else:
            return value

    def column_to_python(self, column_type, value):
        """
        Convert a value of a database column into the corresponding
        Python value.
        """
        if column_type.startswith('list_'):
            return self.column_to_list(column_type, value)
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

    def __init__(self, database, row):
        self.database = database
        self.row = row

    def __getattr__(self, name):
        try:
            return getattr(self.row, name)
        except AttributeError as e:
            hashed_name = hashlib.md5(name.encode('utf-8')).hexdigest()
            result = getattr(self.row, hashed_name, Undefined)
            if result is Undefined:
                raise
            result = self.database.column_to_python(
                self.database.fields[name].type, result)
            setattr(self, hashed_name, result)
            return result

    def __getitem__(self, name):
        return getattr(self, name)
