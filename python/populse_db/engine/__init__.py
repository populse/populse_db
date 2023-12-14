"""
This Python package contains engines that contains the minimum API to connect
an external database engine to a Populse database.
"""


class Engine:
    """
    Base class for database engines. All methods of this base class raise
    NotImplementedError. This class exists to list all the methods that
    must be implemented by an engine.
    """

    def __init__(self):
        """
        Parameters passed to __init__ may differ between engine classes.
        It is up to engine_factory to extract appropriate parameter(s)
        from URL.
        """
        raise NotImplementedError()

    def __enter__(self):
        """
        This method is called at the beginning of a database modification
        session before any other method (normally within a "with" statement).
        Typically, it creates a connection with the database system, starts a
        session then checks the existence of the base schema and creates it if
        necessary.
        """
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This method is called when the user do not need to use the engine
        anymore. After this call, all database resources are freed and a
        call to __enter__ is necessary to be able to reuse the engine.
        Parameters are the exception information as for a "with" statement.
        It must call commit() if exc_type is None or else rollback().
        """
        raise NotImplementedError()

    def commit(self):
        """
        Store in the database all modifications done since the last call to
        either __enter__, commit or rollback.
        """
        raise NotImplementedError()

    def rollback(self):
        """
        Store discards all database modifications done since the last call to
        either __enter__, commit or rollback.
        """
        raise NotImplementedError()

    def clear(self):
        """
        Erase database data and schema.
        """
        raise NotImplementedError()

    def has_collection(self, collection):
        """
        Checks existence of a collection. May be called often,
        must be fast.

        :param collection: collection name (str)
        """
        raise NotImplementedError()

    def add_collection(self, collection, primary_key):
        """
        Create a new collection given the name of a field that will be created
        with a text type. This field is the primary key of each document. It
        means that, in a single collection, a value must uniquely identify a
        document. In other word, two documents cannot have the same value for
        this field.

        :param collection: collection name (str)

        :param primary_key: name of the primary key field (str)
        """
        raise NotImplementedError()

    def collection(self, collection):
        """
        Returns a Row Object with at least the following items:
        collection_name: name of the collection
        primary_key: name of the primary key field
        More engine specific items may be present but they must
        not be used in the general API.
        Returns None if the collection does not exist.

        :param collection: collection name (str)
        """
        raise NotImplementedError()

    def primary_key(self, collection):
        """
        Return the name of the primary key of a collection.
        Returns None if the collection does not exist.

        :param collection: collection name (str)
        """
        raise NotImplementedError()

    def remove_collection(self, collection):
        """
        Delete a collection and its data.

        :param collection: collection name (str)
        """
        raise NotImplementedError()

    def collections(self):
        """
        Return an iterable (e.g. list, generator, etc.) browsing the
        collections presents in the database. See collection() method
        for the format of each collection object.
        """
        raise NotImplementedError()

    def add_field(self, collection, field, type, description, index):
        """
        Adds a new field in a collection.

        :param collection: collection name (str, must be existing)
        :param field: new field name (str, must not be existing)
        :param type: field type, in ('string', 'int', 'float',
           'boolean', 'date', 'datetime', 'time', 'json', 'list_string',
           'list_int', 'list_float', 'list_boolean', 'list_date',
           'list_datetime', 'list_time', 'list_json')
        :param description: field description (str or None)
        :param index: boolean indicating if a database index must be created
           for this field to speed-ud queries involving values for this field.
        """
        raise NotImplementedError()

    def has_field(self, collection, field):
        """
        Checks existence of a field in a collection. May be called often,
        must be fast.
        Returns False if collection does not exists.

        :param collection: collection name (str)

        :param field: field name (str)
        """
        raise NotImplementedError()

    def field(self, collection, field):
        """
        Returns a Row Object corresponding to a collection field with at
        least the following items:
        collection_name: name of the collection
        field_name: field name
        field_type: type of values for this field
        description: text describing the field usage
        has_index: boolean indicating if an index was created for this field
        More engine specific items may be present but they must
        not be used in the general API.
        Returns None if the collection or the field does not exist.

        :param collection: collection name (str)

        :param field: field name (str)
        """
        raise NotImplementedError()

    def fields(self, collection=None):
        """
        Return an iterable (e.g. list, generator, etc.) browsing the
        fields presents in the given collection (or all collections if
        collection parameter is None). See field() method
        for the format of each field object.

        :param collection: collection name (str)
        """
        raise NotImplementedError()

    def remove_fields(self, collection, fields):
        """
        Remove given fields from a collection as well as all corresponding data.

        :param collection: collection name (str)

        :param fields: field name (str)
        """
        raise NotImplementedError()

    def has_document(self, collection, document_id):
        """
        Checks existence of a document in a collection.

        :param collection: collection name (str)

        :param document_id: document identifier: the value of the primary
             key field (str)

        """
        raise NotImplementedError()

    def document(self, collection, document_id, fields=None, as_list=False):
        """
        Returns a Row Object corresponding to a document in the collection.
        The object has one item per selected fields. If fields is not given,
        fields returned by the fields() method are used.
        If as_list is True, a list of values is returned (one value per selected
        field).
        Returns None if the collection or document does not exists.

        :param collection: collection name (str)

        :param document_id: document identifier: the value of the primary
            key field (str)

        :param fields: list of fields to get values from (other fields are
            ignored) (list of str or None)

        :param as_list: if True, return a list of values instead of a Row Object (str)
        """
        raise NotImplementedError()

    def has_value(self, collection, document_id, field):
        """
        Check if a document has a not null value for a given field.
        """
        raise NotImplementedError()

    def set_values(self, collection, document_id, values):
        """
        Change some values in an existing document.

        :param collection: collection name (str)

        :param document_id: document identifier: the value of the primary
            key field (str)

        :param values: dictionary with field/value pairs (dict)
        """
        raise NotImplementedError()

    def remove_value(self, collection, document_id, field):
        """
        Remove a value from a document (setting its value to None).

        :param collection: collection name (str)

        :param document_id: document identifier: the value of the primary
            key field (str)

        :param fields: field name (str)
        """
        raise NotImplementedError()

    def remove_document(self, collection, document_id):
        """
        Remove a document from a collection.

        :param collection: collection name (str)

        :param document_id: document identifier: the value of the primary
            key field (str)
        """
        raise NotImplementedError()

    def parse_filter(self, collection, filter):
        """
        Given a filter string, return a internal query representation that
        can be used with filter_documents() to select documents


        :param collection: the collection for which the filter is intended
               (str, must be existing)

        :param filter: the selection string using the populse_db selection
                       language.
        """
        raise NotImplementedError()

    def filter_documents(
        self, parsed_filter, fields=None, as_list=False, distinct=False
    ):
        """
        Iterate over document selected by a filter. See document() method
        for the format of a document object.

        :param parsed_filter: internal object representing a filter on a
            collection (filter object returned by parse_filter())

        :param fields: list of fields to get values from (other fields are
            ignored) (list of str or None)

        :param as_list: if True, return a list of values instead of a Row Object (str)

        :param distinct: if True, return only a series of different values.
        """
        raise NotImplementedError()
