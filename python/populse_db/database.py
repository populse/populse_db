from datetime import date, time, datetime

from  populse_db.engine import engine_factory

# Field types
FIELD_TYPE_STRING = str
FIELD_TYPE_INTEGER = int
FIELD_TYPE_FLOAT = float
FIELD_TYPE_BOOLEAN = bool
FIELD_TYPE_DATE = date
FIELD_TYPE_DATETIME = datetime
FIELD_TYPE_TIME = time
FIELD_TYPE_JSON = dict
FIELD_TYPE_LIST_STRING = list[string]
FIELD_TYPE_LIST_INTEGER = list[int]
FIELD_TYPE_LIST_FLOAT = list[float]
FIELD_TYPE_LIST_BOOLEAN = list[bool]
FIELD_TYPE_LIST_DATE = list[date]
FIELD_TYPE_LIST_DATETIME = list[datetime]
FIELD_TYPE_LIST_TIME = list[time]
FIELD_TYPE_LIST_JSON = list[dict]

ALL_TYPES = {FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER, FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN,
             FIELD_TYPE_LIST_DATE, FIELD_TYPE_LIST_DATETIME,
             FIELD_TYPE_LIST_TIME, FIELD_TYPE_LIST_JSON, FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT,
             FIELD_TYPE_BOOLEAN, FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME, FIELD_TYPE_JSON}


class Database(object):
    """
    Database API

    attributes:
        - engine: database engine

    methods:
        - __enter__: Creates or gets a DatabaseSession instance
        - __exit__: Releases the latest created DatabaseSession
        - clear: Clears the database

    """

    def __init__(self, database_url):
        """Initialization of the database

        :param database_url: Database engine

                              The engine is constructed this way: dialect://user:password@host/dbname[?key=value..]

                              The dialect can be sqlite or postgresql

                              For sqlite databases, the file can be not existing yet, it will be created in this case

                              Examples:
                                        - "sqlite:///foo.db"
                                        - "postgresql://scott:tiger@localhost/test"

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

def python_value_type(value):
    """
    Returns the field type corresponding to a Python value.
    This type can be used in add_field(s) method.
    For list values, only the first item is considered to get the type.
    Type cannot be determined for empty list.
    """
    if isinstance(value, list) and value:
        return list[type(value[0])]
    type(value)


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

    def commit(self):
        self.engine.commit()
    
    def rollback(self):
        self.engine.rollback()
    
    def add_collection(self, name, primary_key):
        """
        Adds a collection

        :param name: New collection name (str, must not be existing)

        :param primary_key: New collection primary_key column (str) => "index" by default

        :raise ValueError: - If the collection is already existing
                           - If the collection name is invalid
                           - If the primary_key is invalid
        """
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
        Iterates over all collection names

        :return: generator
        """
        return (i[0] for i in self.engine.collections())

    def get_collections(self):
        """
        Iterates over collections

        :return: generator
        """

        return self.engine.collections()

    def add_field(self, collection, name, field_type, description=None,
                  index=False):
        """
        Adds a field to the database

        :param collection: Field collection (str, must be existing)

        :param name: Field name (str, must not be existing)

        :param field_type: Field type, in ('string', 'int', 'float', 'boolean', 'date', 'datetime',
                     'time', 'json', 'list_string', 'list_int', 'list_float', 'list_boolean', 'list_date',
                     'list_datetime', 'list_time', 'list_json')

        :param description: Field description (str or None) => None by default

        :param index: Bool to know if indexing must be done => False by default

        :raise ValueError: - If the collection does not exist
                           - If the field already exists
                           - If the field name is invalid
                           - If the field type is invalid
                           - If the field description is invalid
        """

        # Checks
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
        if isinstance(fields, str):
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
        Iterates over field names of a given a collection

        :param collection: Fields collection (str, must be existing)

        :return: generator
        """

        return (i.field_name for i in self.engine.fields(collection))

    def get_fields(self, collection):
        """
        Iterates over all fields of a given a collection

        :param collection: Fields collection (str, must be existing)

        :return: generator
        """

        return self.engine.fields(collection)

    def set_value(self, collection, document_id, field, new_value):
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


    def set_values(self, collection, document_id, values):
        """
        Sets the values of a <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param values: Dict of values (key=field, value=value)

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
                           - If the values are invalid
                           - If trying to set the primary_key
        """
        self.engine.set_values(collection, document_id, values)
    
    
    def remove_value(self, collection, document_id, field):
        """
        Removes the value <collection, document, field> if it exists

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :param field: Field name (str, must be existing)

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
                           - If the document does not exist
        """
        self.engine.remove_value(collection, document_id, field)

    def add_value(self, collection, document_id, field, value):
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
                f"The document with the name {document_id} already have a value for field {field} in the collection {collection}")
        self.engine.set_values(collection, document_id, {field: value})
        
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
    
    def get_documents_ids(self, collection):
        """
        Iterates over document primary keys of a given a collection

        :param collection: Documents collection (str, must be existing)

        :return: generator
        """

        if not self.engine.has_collection(collection):
            return []
        primary_key = self.engine.primary_key(collection)
        return [i[0] for i in self.get_documents(collection, fields=[primary_key],
                                                 as_list=True)]
     

    def get_documents(self, collection, fields=None, as_list=False,
                      document_ids=None):
        """
        Iterate over of all or selected document of a collection

        :param collection: Documents collection (str, must be existing)

        :param fields: List of fields to retrieve in the document

        :param as_list: If True, document values are returned in a list using
                        fields order

        :param document_ids: Restrict the result to the document ids contained
                             in this iterable 

        :return: generator
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
        self.engine.remove_document(collection, document_id)
    
    
    def add_document(self, collection, document, create_missing_fields=False):
        """
        Adds a document to a collection

        :param collection: Document collection (str, must be existing)

        :param document: Dictionary of document values (dict), or document primary_key (str)

                            - The primary_key must not be existing

        :param create_missing_fields: Boolean to know if the missing fields must be created

            - If True, fields that are in the document but not in the collection are created if the type can be guessed from the value in the document
              (possible for all valid values except None and []).
            
        :raise ValueError: - If the collection does not exist
                           - If the document already exists
                           - If document is invalid (invalid name or no primary_key)
        """

        self.engine.add_document(collection, document, create_missing_fields)
        

    def filter_documents(self, collection, filter_query, fields=None, as_list=False):
        """
        Iterates over the collection documents selected by filter_query

        Each item yield is a row of the collection table returned

        filter_query can be the result of self.filter_query() or a string containing a filter
        (in this case self.fliter_query() is called to get the actual query)

        :param collection: Filter collection (str, must be existing)

        :param filter_query: Filter query (str)

                    - A filter row must be written this way: {<field>} <operator> "<value>"
                    - The operator must be in ('==', '!=', '<=', '>=', '<', '>', 'IN', 'ILIKE', 'LIKE')
                    - The filter rows can be linked with ' AND ' or ' OR '
                    - Example: "((({BandWidth} == "50000")) AND (({FileName} LIKE "%G1%")))"

        :param fields: List of fields to retrieve in the document

        :param as_list: If True, document values are returned in a list using
                        fields order

        """

        parsed_filter = self.engine.parse_filter(collection, filter_query)
        for doc in self.engine.filter_documents(parsed_filter,fields=fields, as_list=as_list):
            yield doc
            
    
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
        origin = getattr(field_type, '__origin__', None)
        if origin:
            # field_type is a parameterized type such as list[str]
            # origin is the parent type (e.g. list)
            if isinstance(value, origin):
                # The following code works only on list[...]
                # because other parameterized types are not
                # supported.
                item_type = field_type.__args__[0]
                for v in value:
                    if not cls.check_value_type(v, item_type):
                        return False
                return True
        else:
            return isinstance(field_type, value)
        return False

# Default link between Database and DatabaseSession class is defined below.
# It is used whenever a database session is created. This allow to derive
# Database class and to also use a derived version of DatabaseSession.
Database.database_session_class = DatabaseSession
