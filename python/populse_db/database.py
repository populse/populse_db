from collections import OrderedDict
from datetime import date, time, datetime
import os
import re
import six

from  populse_db.engine import engine_factory

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

ALL_TYPES = {FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER, FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN,
             FIELD_TYPE_LIST_DATE, FIELD_TYPE_LIST_DATETIME,
             FIELD_TYPE_LIST_TIME, FIELD_TYPE_LIST_JSON, FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT,
             FIELD_TYPE_BOOLEAN, FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME, FIELD_TYPE_JSON}

class ListWithKeys(object):
    '''
    Reprsents a list of value of fixed size with a key string for each value.
    It allows to access to values with their index or with their key.
    It is also possible to acess to values as attributes.
    The function list_with_keys() is used to create derived classes
    with a fixed set of item names.
    '''
    _key_indices = {}
    
    def __init__(self, *args, **kwargs):
        '''
        Initialize values with their position (args)
        or name (kwargs)
        '''
        self._values = [None] * len(self._key_indices)
        i = 0
        for value in args:
            self._values[i] = value
            i += 1
        for key, value in kwargs.items():
            self._values[self._key_indices[key]] = value
    
    def __iter__(self):
        '''
        Iterate over names of items
        '''
        return iter(self._key_indices)
    
    def __getattr__(self, name):
        '''
        Get a value given its key
        '''
        try:
            return self._values[self._key_indices[name]]
        except KeyError:
            raise AttributeError(repr(name))

    def __getitem__(self, name_or_index):
        '''
        Get a value given its index or key
        '''
        if isinstance(name_or_index, six.string_types):
            return self._values[self._key_indices[name_or_index]]
        else:
            return self._values[name_or_index]
    
    @classmethod
    def keys(cls):
        return cls._key_indices.keys()
    
    
    @classmethod
    def _append_key(cls, key):
        '''
        Append a new key to the class
        '''
        cls._key_indices[key] = len(cls._key_indices)
    
    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, ','.join('%s = %s' % (k, repr(self._values[i])) for k, i in self._key_indices.items()))
    
    def _items(self):
        '''
        Iterate over key, value pairs
        '''
        return ((i, self[i]) for i in self._key_indices if self[i] is not None)
    
    
    def _dict(self):
        '''
        Create a dictionary using keys and values
        '''
        return dict(self._items())

    @classmethod
    def _delete_key(cls, name):
        '''
        Delete a key from the class
        '''
        index = cls._key_indices.pop(name)
        for n, i in list(cls._key_indices.items()):
            if i > index:
                cls._key_indices[n] = i - 1


def list_with_keys(name, keys):
    '''
    Return a new instance of ListWithNames with
    a given list of keys
    '''
    return type(str(name), (ListWithKeys,), {'_key_indices': OrderedDict(zip(keys, 
                                                        range(len(keys))))})        

class DictList(ListWithKeys):
    def __init__(self, keys, values):
        self._key_indices = keys
        super(DictList, self).__init__(*values)


class Database(object):
    """
    Database API

    attributes:
        - string_engine: String engine of the database
        - engine: database engine

    methods:
        - __enter__: Creates or gets a DatabaseSession instance
        - __exit__: Releases the latest created DatabaseSession
        - clear: Clears the database

    """

    def __init__(self, database_url, caches=None, list_tables=None,
                 query_type=None):
        """Initialization of the database

        :param database_url: Database engine

                              The engine is constructed this way: dialect://user:password@host/dbname[?key=value..]

                              The dialect can be sqlite or postgresql

                              For sqlite databases, the file can be not existing yet, it will be created in this case

                              Examples:
                                        - "sqlite:///foo.db"
                                        - "postgresql://scott:tiger@localhost/test"

        :param caches: obsolete parameter kept for backward compatibility

        :param list_tables: obsolete parameter kept for backward compatibility

        :param query_type: obsolete parameter kept for backward compatibility

        :raise ValueError: - If database_url is invalid
                           - If the schema is not coherent with the API (the database is not a populse_db database)
        """

        self.database_url = database_url
        self.__session = None


    def __enter__(self):
        """
        Return a DatabaseSession instance for using the database. This is
        supposed to be called using a "with" statement:
        
        with database as session:
           session.add_document(...)
           
        Therefore __exit__ must be called to get rid of the session.
        When called recursively, the underlying database session returned
        is the same. The commit/rollback of the session is done only by the
        outermost __enter__/__exit__ pair (i.e. by the outermost with
        statement).
        """
        if self.__session is None:            
            self.__session = self.database_session_class(self)
        self.__session.engine.__enter__()
        return self.__session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Release a DatabaseSession previously created by __enter__.
        If no recursive call of __enter__ was done, the session
        is commited if no error is reported (e.g. exc_type is None)
        otherwise it is rolled back. Nothing is done 
        """
        self.__session.engine.__exit__(exc_type, exc_val, exc_tb)
            
    def clear(self):
        """
        Removes all documents and collections in the database
        """
        with self as session:
            session.engine.clear()

_python_type_to_field_type = {
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

def python_value_type(value):
    """
    Returns the field type corresponding to a Python value.
    This type can be used in add_field(s) method.
    For list values, only the first item is considered to get the type.
    Type cannot be determined for empty list.
    If value is None, the result is None.
    """
    if isinstance(value, list):
        if value:
            item_type = python_value_type(value[0])
            return 'list_' + item_type
        else:
            # Raises a KeyError for empty list
            return _python_type_to_field_type[list]
    else:
        return _python_type_to_field_type[type(value)]


class DatabaseSession(object):
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

    def __init__(self, database):
        """
        Creates a session API of the Database instance

        :param database: Database instance to take into account
        """
        
        self.engine = engine_factory(database.database_url)
        self.__names = {}

    def commit(self):
        self.engine.commit()
    
    def rollback(self):
        self.engine.rollback()
    
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
                "The collection name must be of type {0}, but collection name of type {1} given".format(str,type(name)))
        if not isinstance(primary_key, str):
            raise ValueError(
                "The collection primary_key must be of type {0}, but collection primary_key of type {1} given".format(
                    str, type(primary_key)))
        if self.engine.has_collection(name):
            raise ValueError("A collection/table with the name {0} already exists".format(name))

        self.engine.add_collection(name, primary_key)


    def remove_collection(self, name):
        """
        Removes a collection

        :param name: Collection to remove (str, must be existing)

        :raise ValueError: If the collection does not exist
        """

        # Checks
        
        if not self.engine.has_collection(name):
            raise ValueError("The collection {0} does not exist".format(name))

        self.engine.remove_collection(name)

    def get_collection(self, name):
        """
        Returns the collection row of the collection

        :param name: Collection name (str, must be existing)

        :return: The collection row if it exists, None otherwise
        """
        return self.engine.collection(name)

    def get_collections_names(self):
        """
        Gives the list of all collection names

        :return: List of all collection names
        """
        return [i[0] for i in self.engine.collections()]

    def get_collections(self):
        """
        Gives the list of all collection rows

        :return: List of all collection rows
        """

        return self.engine.collections()

    """ FIELDS """

    def add_fields(self, fields):
        """
        Adds the list of fields

        :param fields: List of fields: [collection, name, type, description]
        """
        if not isinstance(fields, list):
            raise ValueError(
                "The fields must be of type {0}, but fields of type {1} given".format(list, type(fields)))

        for field in fields:

            # Adding each field
            if not isinstance(field, list) or len(field) != 4:
                raise ValueError("Invalid field, it must be a list of four elements: [collection, name, type, description]")
            self.add_field(collection=field[0], 
                           name=field[1],
                           field_type=field[2],
                           description=field[3])

    def add_field(self, collection, name, field_type, description=None,
                  index=False, flush=None):
        """
        Adds a field to the database

        :param collection: Field collection (str, must be existing)

        :param name: Field name (str, must not be existing)

        :param field_type: Field type, in ('string', 'int', 'float', 'boolean', 'date', 'datetime',
                     'time', 'json', 'list_string', 'list_int', 'list_float', 'list_boolean', 'list_date',
                     'list_datetime', 'list_time', 'list_json')

        :param description: Field description (str or None) => None by default

        :param index: Bool to know if indexing must be done => False by default

        :param flush: obsolete parameter kept for backward compatibility

        :raise ValueError: - If the collection does not exist
                           - If the field already exists
                           - If the field name is invalid
                           - If the field type is invalid
                           - If the field description is invalid
        """

        # Checks
        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        if self.engine.has_field(collection, name):
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

        self.engine.add_field(collection, name, field_type, description, index)

    def remove_field(self, collection, fields):
        """
        Removes a field in the collection

        :param collection: Field collection (str, must be existing)

        :param field: Field name (str, must be existing), or list of fields (list of str, must all be existing)

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
        """

        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        if isinstance(fields, six.string_types):
            fields = [fields]
        for field in fields:
            if not self.engine.has_field(collection, field):
                raise ValueError(
                    "The field with the name {0} does not exist in the collection {1}".format(field,
                                                                                              collection))
        self.engine.remove_fields(collection, fields)

    def get_field(self, collection, name):
        """
        Gives the field row, given a field name and a collection

        :param collection: Document collection (str, must be existing)

        :param name: Field name (str, must be existing)

        :return: The field row if the field exists, None otherwise
        """

        return self.engine.field(collection, name)
    
    def get_fields_names(self, collection):
        """
        Gives the list of all fields, given a collection

        :param collection: Fields collection (str, must be existing)

        :return: List of all fields names of the collection if it exists, None otherwise
        """

        return [i.field_name for i in self.engine.fields(collection)]

    def get_fields(self, collection):
        """
        Gives the list of all fields rows, given a collection

        :param collection: Fields collection (str, must be existing)

        :return: List of all fields rows of the collection if it exists, None otherwise
        """

        return list(self.engine.fields(collection))

    """ VALUES """

    def get_value(self, collection, document_id, field):
        """
        Gives the current value of <collection, document, field>

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :return: The current value of <collection, document, field> if it exists, None otherwise
        """
        
        r = self.get_document(collection, document_id, fields=[field],
                              as_list=True)
        if r is None:
            return None
        return r[0]

    def set_value(self, collection, document_id, field, new_value, flush=None):
        """
        Sets the value associated to <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :param new_value: New value

        :param flush: unused obsolete parmeter

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the value is invalid
                           - If trying to set the primary_key
        """

        self.set_values(collection, document_id, {field: new_value})


    def set_values(self, collection, document_id, values, flush=None):
        """
        Sets the values of a <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param values: Dict of values (key=field, value=value)

        :param flush: unused obsolete parmeter

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the values are invalid
                           - If trying to set the primary_key
        """

        # Checks
        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        if not self.engine.has_document(collection, document_id):
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document_id, collection))
        for field, value in values.items():
            if not self.engine.has_field(collection, field):
                raise ValueError(
                    "The field with the name {0} does not exist in the collection {1}".format(field, collection))
            field_row = self.engine.field(collection, field)
            if not self.check_value_type(value, field_row.field_type):
                raise ValueError("The value {0} is invalid for the type {1}".format(value, field_row.field_type))
        self.engine.set_values(collection, document_id, values)
    
    
    def remove_value(self, collection, document_id, field, flush=None):
        """
        Removes the value <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :param flush: unused obsolete parameter

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
        """

        # Checks
        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        if not self.engine.has_field(collection, field):
            raise ValueError(
                "The field with the name {0} does not exist in the collection {1}".format(field, collection))
        if not self.engine.has_document(collection, document_id):
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document_id, collection))
        if self.engine.has_value(collection, document_id, field):
            self.engine.remove_value(collection, document_id, field)

    def add_value(self, collection, document_id, field, value, checks=True):
        """
        Adds a value for <collection, document_id, field>

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :param value: Value to add

        :param checks: if False, do not perform any type or value checking

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the value is invalid
                           - If <collection, document_id, field> already has a value
        """
        if self.engine.has_value(collection, document_id, field):
            raise ValueError(
                "The document with the name {1} already have a value for field {2} in the collection {0}".format(collection, document_id, field))
        if checks:
            if not self.engine.has_collection(collection):
                raise ValueError("The collection {0} does not exist".format(collection))
            field_row = self.engine.field(collection, field)
            if not field_row:
                raise ValueError(
                    "The field with the name {0} does not exist in the collection {1}".format(field, collection))
            if not self.engine.has_document(collection, document_id):
                raise ValueError(
                    "The document with the name {0} does not exist in the collection {1}".format(document_id, collection))
            if not self.check_value_type(value, field_row.field_type):
                raise ValueError("The value {0} is invalid for the type {1}".format(value, field_row.field_type))
        
        self.engine.set_values(collection, document_id, {field: value})
        
    """ DOCUMENTS """

    def has_document(self, collection, document_id):
        return self.engine.has_document(collection, document_id)

    def get_document(self, collection, document_id, fields=None,
                     as_list=False):
        """
        Gives a Document instance given a collection and a document identifier

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :return: The document row if the document exists, None otherwise
        """
        try:
            result = self.engine.document(collection, document_id,
                                          fields=fields, as_list=as_list)
        except KeyError:
            result = None
        return result
    
    def get_documents_names(self, collection):
        """
        Gives the list of all document names, given a collection

        :param collection: Documents collection (str, must be existing)

        :return: List of all document names of the collection if it exists, None otherwise
        """

        if not self.engine.has_collection(collection):
            return []
        primary_key = self.engine.primary_key(collection)
        return [i[0] for i in self.get_documents(collection, fields=[primary_key],
                                                 as_list=True)]
     

    def get_documents(self, collection, fields=None, as_list=False,
                      document_ids=None):
        """
        Gives the list of all document rows, given a collection

        :param collection: Documents collection (str, must be existing)

        :return: List of all document rows of the collection if it exists, None otherwise
        """

        if not self.engine.has_collection(collection):
            return []
        if document_ids is None:
            return list(self.filter_documents(collection, None, fields=fields,
                                              as_list=as_list))
        # get a list of documents
        primary_key = self.get_collection(collection).primary_key
        pk_column = self.engine.field_column[collection][primary_key]
        filter_query = '[%s] in (%s)' \
            % (pk_column, ', '.join('?' for document_id in document_ids))
        return self.engine._select_documents(
            collection, filter_query, document_ids, fields=fields,
            as_list=as_list)


    def remove_document(self, collection, document_id):
        """
        Removes a document in the collection

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :raise ValueError: - If the collection does not exist
                           - If the document does not exist
        """

        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        if not self.engine.has_document(collection, document_id):
            raise ValueError(
                "The document with the name {0} does not exist in the collection {1}".format(document_id, collection))
        self.engine.remove_document(collection, document_id)
    
    
    def add_document(self, collection, document, create_missing_fields=True, flush=None):
        """
        Adds a document to a collection

        :param collection: Document collection (str, must be existing)

        :param document: Dictionary of document values (dict), or document primary_key (str)

                            - The primary_key must not be existing

        :param create_missing_fields: Boolean to know if the missing fields must be created

            - If True, fields that are in the document but not in the collection are created if the type can be guessed from the value in the document
              (possible for all valid values except None and []).
            
        :param flush: ignored obsolete parameter

        :raise ValueError: - If the collection does not exist
                           - If the document already exists
                           - If document is invalid (invalid name or no primary_key)
        """

        # Checks
        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        if not isinstance(document, dict) and not isinstance(document, str):
            raise ValueError(
                "The document must be of type {0} or {1}, but document of type {2} given".format(dict, str, document))
        primary_key = self.engine.primary_key(collection)
        if isinstance(document, dict) and primary_key not in document:
            raise ValueError(
                "The primary_key {0} of the collection {1} is missing from the document dictionary".format(primary_key,
                                                                                                           collection))
        if not isinstance(document, dict):
            document = {primary_key: document}
        self.engine.add_document(collection, document, create_missing_fields)
        
    """ FILTERS """

    def filter_documents(self, collection, filter_query, fields=None, as_list=False):
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

        if not self.engine.has_collection(collection):
            raise ValueError("The collection {0} does not exist".format(collection))
        parsed_filter = self.engine.parse_filter(collection, filter_query)
        for doc in self.engine.filter_documents(parsed_filter,fields=fields, as_list=as_list):
            yield doc
            
    
    """ UTILS """


    _value_type_checker = {
        FIELD_TYPE_INTEGER: lambda v: isinstance(v, int),
        FIELD_TYPE_FLOAT: lambda v: isinstance(v, (int, float)),
        FIELD_TYPE_BOOLEAN: lambda v: isinstance(v, bool),
        FIELD_TYPE_STRING: lambda v: isinstance(v, six.string_types),
        FIELD_TYPE_JSON: lambda v: isinstance(v, dict),
        FIELD_TYPE_DATETIME: lambda v: isinstance(v, datetime),
        FIELD_TYPE_DATE: lambda v: isinstance(v, date),
        FIELD_TYPE_TIME: lambda v: isinstance(v, time),
    }
    
    @classmethod
    def check_value_type(cls, value, field_type):
        """
        Checks the type of the value

        :param value: Value

        :param field_type: Type that the value is supposed to have

        :return: True if the value is None or has a valid type, False otherwise
        """

        if field_type is None:
            return False
        if value is None:
            return True
        if field_type.startswith('list_'):
            if isinstance(value, list):
                item_type = field_type[5:]
                for v in value:
                    if not cls.check_value_type(v, item_type):
                        return False
                return True
        else:
            return cls._value_type_checker[field_type](value)


# Default link between Database and DatabaseSession class is defined below.
# It is used whenever a database session is created. This allow to derive
# Database class and to also use a derived version of DatabaseSession.
Database.database_session_class = DatabaseSession
