from datetime import date, time, datetime

# Field types
FIELD_TYPE_STRING = str
FIELD_TYPE_INTEGER = int
FIELD_TYPE_FLOAT = float
FIELD_TYPE_BOOLEAN = bool
FIELD_TYPE_DATE = date
FIELD_TYPE_DATETIME = datetime
FIELD_TYPE_TIME = time
FIELD_TYPE_JSON = dict
FIELD_TYPE_LIST_STRING = list[str]
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



def check_value_type(value, field_type):
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
                if not check_value_type(v, item_type):
                    return False
            return True
    elif field_type is float:
        return isinstance(value, (int, float))
    else:
        return isinstance(value, field_type)
    return False

def type_to_str(type):
    args = getattr(type, '__args__', None)
    if args:
        return f'{type.__name__}[{",".join(type_to_str(i) for i in args)}]'
    else:
        return type.__name__

_str_to_type = dict((type_to_str(i), i) for i in (
    str, int, float, bool, date, datetime, time, dict, list
))

def str_to_type(str):
    global _str_to_type

    s = str.split('[',1)
    if len(s) == 1:
        return _str_to_type[s[0]]
    else:
        args = tuple(str_to_type(i) for i in s[1][:-1].split(","))
        return _str_to_type[s[0]][args]

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
    populse_db_table = 'populse_db'
    default_primary_key = 'primary_key'

    def __getitem__(self, collection_name):
        raise NotImplemented()
    
    def execute(self, *args, **kwargs):
        raise NotImplemented()

    def commit(self):
        raise NotImplemented()
    
    def rollback(self):
        raise NotImplemented()

    def get_settings(self, category, key, default=None):
        raise NotImplemented()

    def set_settings(self, category, key, value):
        raise NotImplemented()
    
    def add_collection(self, name, primary_key=default_primary_key):
        """
        Adds a collection

        :param name: New collection name (str, must not be existing)

        :param primary_key: New collection primary_key column (str) => "index" by default

        :raise ValueError: - If the collection is already existing
                           - If the collection name is invalid
                           - If the primary_key is invalid
        """
        raise NotImplemented()

    def remove_collection(self, name):
        """
        Removes a collection

        :param name: Collection to remove (str, must be existing)

        :raise ValueError: If the collection does not exist
        """
        raise NotImplemented()

    def get_collection(self, name):
        """
        Returns the collection row of the collection

        :param name: Collection name (str, must be existing)

        :return: The collection row if it exists, None otherwise
        """
        try:
            return self[name]
        except ValueError:
            return None

    def get_collections_names(self):
        """
        Iterates over all collection names

        :return: generator
        """
        return (i.name for i in self)

    def get_collections(self):
        """
        Iterates over collections

        :return: generator
        """

        yield from self

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
        self[collection].add_field(name, field_type, description=description, 
                                     index=index)

    def remove_field(self, collection, field):
        """
        Removes a field in the collection

        :param collection: Field collection (str, must be existing)

        :param field: Field name (str, must be existing))

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
        """
        self[collection].remove_field(field)

    def get_field(self, collection, name):
        """
        Gives the field row, given a field name and a collection

        :param collection: Document collection (str, must be existing)

        :param name: Field name (str, must be existing)

        :return: The field row if the field exists, None otherwise
        """

        return self[collection].field(name)
    
    def get_fields_names(self, collection):
        """
        Iterates over field names of a given a collection

        :param collection: Fields collection (str, must be existing)

        :return: generator
        """

        return self[collection].fields.keys()

    def get_fields(self, collection):
        """
        Iterates over all fields of a given a collection

        :param collection: Fields collection (str, must be existing)

        :return: generator
        """
        
        return self[collection].fields.values()

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
        self[collection].update_document(document_id, values)
            
    def has_document(self, collection, document_id):
        return self[collection].has_document(document_id)

    def get_document(self, collection, document_id, fields=None,
                     as_list=False):
        """
        Gives a Document instance given a collection and a document identifier

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :return: The document row if the document exists, None otherwise
        """
        try:
            collection = self[collection]
        except ValueError:
            return None
        return collection.document(document_id, fields, as_list)
    
    def get_documents_ids(self, collection):
        """
        Iterates over document primary keys of a given a collection

        :param collection: Documents collection (str, must be existing)

        :return: generator
        """
        c = self[collection]
        return (i for i in c.documents(fields=tuple(c.primary_key), as_list=True))
     

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
        c = self[collection]
        if document_ids is None:
            yield from c.documents(fields=fields, as_list=as_list)
        else:
            for document_id in document_ids:
                document = c.get(document_id)
                if document is not None:
                    yield document

    def remove_document(self, collection, document_id):
        """
        Removes a document in the collection

        :param collection: Document collection (str, must be existing)

        :param document_id: Document name (str, must be existing)

        :raise ValueError: - If the collection does not exist
                           - If the document does not exist
        """
        del self[collection][document_id]
    
    
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
        if create_missing_fields:
            raise NotImplementedError('create_missing_field option is not implemented yet')
        self[collection].add(document)
        

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

        yield from self[collection].filter(filter_query, fields=fields, as_list=as_list)
