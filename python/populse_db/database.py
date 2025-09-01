import json
from datetime import date, datetime, time

import dateutil

populse_db_table = "populse_db"


def check_value_type(value, field_type):
    """
    Checks the type of a value

    :param value: value to check

    :param field_type: type that the value is supposed to have

    :return: true if the value is ``None`` or if the value is of that type,
        ``False`` otherwise
    """

    if field_type is None:
        return False
    if value is None:
        return True
    origin = getattr(field_type, "__origin__", None)
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
        return isinstance(value, int | float)
    else:
        return isinstance(value, field_type)
    return False


def type_to_str(type):
    """Convert a Python type to a string.

    Examples:

        - ``type_to_str(str) == 'str'``
        - ``type_to_str(list[str]) == 'list[str]'``
    """
    args = getattr(type, "__args__", None)
    if args:
        return f"{type.__name__}[{','.join(type_to_str(i) for i in args)}]"
    else:
        return type.__name__


_type_to_sqlite = {
    str: "text",
}


def type_to_sqlite(type):
    """
    Like type_to_str(type) but for internal use in SQLite column type
    definitions in order to avoid conversion problems due to SQlite type
    affinity. See https://www.sqlite.org/datatype3.html
    """
    result = _type_to_sqlite.get(type)
    if result is None:
        args = getattr(type, "__args__", None)
        if args:
            result = f"{type.__name__}[{','.join(type_to_sqlite(i) for i in args)}]"
        else:
            result = type.__name__
    return result


_str_to_type = {
    type_to_sqlite(i): i
    for i in (str, int, float, bool, date, datetime, time, dict, list)
}
_str_to_type.update(
    {
        type_to_str(i): i
        for i in (str, int, float, bool, date, datetime, time, dict, list)
    }
)


def str_to_type(str):
    """Convert a string to a Python type.

    Examples:

        - ``str_to_type('str') == str``
        - ``str_to_type('list[str]') == list[str]``
    """
    global _str_to_type

    if not str:
        return None
    s = str.split("[", 1)
    if len(s) == 1:
        result = _str_to_type.get(s[0])
    else:
        args = tuple(str_to_type(i) for i in s[1][:-1].split(","))
        result = _str_to_type.get(s[0])
        if result:
            result = result[args]
    if not result:
        raise ValueError(f'invalid type: "{str}"')
    return result


def python_value_type(value):
    """
    Returns the Python type corresponding to a Python value.
    This type can be used in add_field(s) method.
    For list values, only the first item is considered to get the subtype
    of the list.

    Examples:

        - ``python_value_type('a value') == str``
        - ``python_value_type([]) == list``
        - ``python_value_type([1, 2, 3]) == list[int]``
        - ``python_value_type(['string', 2, {}]) == list[str]``
        - ``python_value_type({'one': 1, 'two': 2}) == dict``
    """
    if isinstance(value, list) and value:
        return list[type(value[0])]
    type(value)


class DatabaseSession:
    """
    Base class

    methods:

        *Database related methods:*

        - :py:meth:`execute`
        - :py:meth:`commit`
        - :py:meth:`rollback`

        *Database configuration methods:*

        - :any:`settings`
        - :py:meth:`set_settings`

        *Collections related methods*

        - :py:meth:`add_collection`
        - :py:meth:`remove_collection`
        - :py:meth:`has_collection`
        - :py:meth:`__getitem__`
        - :py:meth:`collections`

        *Obsolete methods kept for backward compatibility*

        - :py:meth:`get_collection`
        - :py:meth:`get_collections`
        - :py:meth:`get_collections_names`
        - :py:meth:`add_field`
        - :py:meth:`remove_field`
        - :py:meth:`get_field`
        - :py:meth:`get_fields_names`
        - :py:meth:`get_fields`
        - :py:meth:`set_values`
        - :py:meth:`has_document`
        - :py:meth:`get_document`
        - :py:meth:`get_documents_ids`
        - :py:meth:`get_documents`
        - :py:meth:`remove_document`
        - :py:meth:`add_document`
        - :py:meth:`filter_documents`

    .. automethod:: __getitem__
    """

    default_primary_key = "primary_key"

    def execute(self, *args, **kwargs):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def settings(self, category, key, default=None):
        raise NotImplementedError()

    def set_settings(self, category, key, value):
        raise NotImplementedError()

    def add_collection(self, name, primary_key=default_primary_key):
        """
        Adds a collection

        :param name: New collection name (str, must not be existing)

        :param primary_key: New collection primary_key column (str) => "index" by default

        :raise ValueError: - If the collection is already existing
                           - If the collection name is invalid
                           - If the primary_key is invalid
        """
        raise NotImplementedError()

    def remove_collection(self, name):
        """
        Removes a collection

        :param name: Collection to remove (str, must be existing)

        :raise ValueError: If the collection does not exist
        """
        raise NotImplementedError()

    def has_collection(self, name):
        """
        Check if a collection with the given name exists.
        """
        raise NotImplementedError()

    def __getitem__(self, collection_name):
        """Return a collection object given its name."""
        raise NotImplementedError()

    def collections(self):
        """
        Iterates over collections

        :return: generator
        """

        yield from self

    def get_collection(self, name):
        """
        .. deprecated:: 3.0
            Use ``db_session[name]`` instead
        """
        try:
            return self[name]
        except ValueError:
            return None

    def get_collections(self):
        """
        .. deprecated:: 3.0
            Use :py:meth:`collections()` instead
        """
        return self.collections()

    def get_collections_names(self):
        """
        .. deprecated:: 3.0
            Use ``(i.name for i in db_session)`` instead
        """
        return (i.name for i in self)

    def add_field(self, collection, name, field_type, description=None, index=False):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].add_field(...)`` instead.
            See :py:meth:`DatabaseCollection.add_field`.
        """
        self[collection].add_field(
            name, field_type, description=description, index=index
        )

    def remove_field(self, collection, field):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].remove_field(...)`` instead.
            See :py:meth:`DatabaseCollection.remove_field`.
        """
        self[collection].remove_field(field)

    def get_field(self, collection, name):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].fields.get(name)`` instead.
            See :py:attr:`DatabaseCollection.fields`.
        """
        try:
            return self[collection].fields.get(name)
        except ValueError:
            return None

    def get_fields_names(self, collection):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].fields.keys()`` instead.
            See :py:attr:`DatabaseCollection.fields`.
        """
        try:
            return self[collection].fields.keys()
        except ValueError:
            return ()

    def get_fields(self, collection):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].fields.values()`` instead.
            See :py:attr:`DatabaseCollection.fields`.
        """
        try:
            return self[collection].fields.values()
        except ValueError:
            return ()

    def set_values(self, collection, document_id, values):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].update_document(...)`` instead.
            See :py:meth:`DatabaseCollection.update_document`.
        """
        self[collection].update_document(document_id, values)

    def has_document(self, collection, document_id):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].has_document(...)`` instead.
            See :py:meth:`DatabaseCollection.has_document`.
        """
        return self[collection].has_document(document_id)

    def get_document(self, collection, document_id, fields=None, as_list=False):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].document(...)`` instead.
            See :py:meth:`DatabaseCollection.document`.
        """
        try:
            collection = self[collection]
        except ValueError:
            return None
        return collection.document(document_id, fields, as_list)

    def get_documents_ids(self, collection):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].documents_ids(...)`` instead.
            See :py:meth:`DatabaseCollection.documents_ids`.
        """
        try:
            c = self[collection]
        except ValueError:
            return
        yield from c.documents_ids()

    def get_documents(self, collection, fields=None, as_list=False, document_ids=None):
        """
        .. deprecated:: 3.0
            Use ``db_session[collection].documents_ids(...)`` instead.
            See :py:meth:`DatabaseCollection.documents_ids`.
        """
        try:
            c = self[collection]
        except ValueError:
            return
        if document_ids is None:
            yield from c.documents(fields=fields, as_list=as_list)
        else:
            for document_id in document_ids:
                document = c.get(document_id)
                if document is not None:
                    yield document

    def remove_document(self, collection, document_id):
        """
        .. deprecated:: 3.0
            Use ``del db_session[collection][document_id]`` instead.
            See :py:meth:`DatabaseCollection.__delitem__`.
        """
        del self[collection][document_id]

    def add_document(self, collection, document):
        """
        .. deprecated:: 3.0
            Use ``del db_session[collection].add(document)`` instead.
            See :py:meth:`DatabaseCollection.add`.
        """
        self[collection].add(document)

    def filter_documents(self, collection, filter_query, fields=None, as_list=False):
        """
        .. deprecated:: 3.0
            Use ``del db_session[collection].filter(...)`` instead.
            See :py:meth:`DatabaseCollection.filter`.
        """
        yield from self[collection].filter(filter_query, fields=fields, as_list=as_list)


class DatabaseCollection:
    def __init__(self, session, name):
        self.session = session
        self.name = name
        self.catchall_column = self.settings().get("catchall_column", "_catchall")
        self.primary_key = {}
        self.bad_json_fields = set()
        self.fields = {}

    def settings(self):
        return self.session.settings("collection", self.name, {})

    def set_settings(self, settings):
        self.session.set_settings("collection", self.name, settings)

    def document_id(self, document_id):
        if not isinstance(document_id, tuple | list):
            document_id = (document_id,)
        if len(document_id) != len(self.primary_key):
            raise KeyError(
                f"key for table {self.name} requires {len(self.primary_key)} value(s), {len(document_id)} given"
            )
        return document_id

    def update_settings(self, **kwargs):
        settings = self.settings()
        settings.update(kwargs)
        self.set_settings(settings)

    def add_field(
        self, name, field_type, description=None, index=False, bad_json=False
    ):
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
        raise NotImplementedError()

    def remove_field(self, name):
        """
        Removes a field in the collection

        :param collection: Field collection (str, must be existing)

        :param field: Field name (str, must be existing))

        :raise ValueError: - If the collection does not exist
                           - If the field does not exist
        """
        raise NotImplementedError()

    def update_document(self, document_id, partial_document):
        raise NotImplementedError()

    def has_document(self, document_id):
        raise NotImplementedError()

    def document(self, document_id, fields=None, as_list=False):
        raise NotImplementedError()

    def documents(self, fields=None, as_list=False):
        raise NotImplementedError()

    def documents_ids(self):
        yield from (
            i for i in self.documents(fields=tuple(self.primary_key), as_list=True)
        )

    def __iter__(self):
        return self.documents()

    def add(self, document, replace=False):
        raise NotImplementedError()

    def __setitem__(self, document_id, document):
        raise NotImplementedError()

    def _encode_column_value(self, field, value):
        encoding = self.fields.get(field, {}).get("encoding")
        if encoding:
            encode, decode = encoding
            try:
                column_value = encode(value)
            except TypeError:
                # Error with JSON encoding
                column_value = ...
            if column_value is ...:
                column_value = encode(json_encode(value))
                self.bad_json_fields.add(field)
                settings = self.settings()
                settings.setdefault("fields", {}).setdefault(field, {})["bad_json"] = (
                    True
                )
                self.set_settings(settings)
            return column_value
        return value

    def __getitem__(self, document_id):
        return self.document(document_id)

    def __delitem__(self, document_id):
        raise NotImplementedError()

    def parse_filter(self, filter):
        raise NotImplementedError()

    def filter(self, filter, fields=None, as_list=False):
        """
        Iterates over the collection documents selected by filter_query

        Each item yield is a row of the collection table returned

        filter_query can be the result of self.filter_query() or a string containing a filter
        (in this case self.fliter_query() is called to get the actual query)

        :param filter_query: Filter query (str)

                    - A filter row must be written this way: {<field>} <operator> "<value>"
                    - The operator must be in ('==', '!=', '<=', '>=', '<', '>', 'IN', 'ILIKE', 'LIKE')
                    - The filter rows can be linked with ' AND ' or ' OR '
                    - Example: "((({BandWidth} == "50000")) AND (({FileName} LIKE "%G1%")))"

        :param fields: List of fields to retrieve in the document

        :param as_list: If True, document values are returned in a list using
                        fields order

        """
        raise NotImplementedError()

    def delete(self, filter):
        """
        Delete documents corresponding to the given filter

        :param filter_query: Filter query (str)
        """
        raise NotImplementedError()


def json_dumps(value):
    return json.dumps(value, separators=(",", ":"))


_json_encodings = {
    datetime: lambda d: f"{d.isoformat()}ℹdatetimeℹ",
    date: lambda d: f"{d.isoformat()}ℹdateℹ",
    time: lambda d: f"{d.isoformat()}ℹtimeℹ",
    list: lambda l: [json_encode(i) for i in l],  # noqa: E741
    dict: lambda d: {k: json_encode(v) for k, v in d.items()},
}

_json_decodings = {
    "datetime": lambda s: dateutil.parser.parse(s),
    "date": lambda s: dateutil.parser.parse(s).date(),
    "time": lambda s: dateutil.parser.parse(s).time(),
}


def json_encode(value):
    global _json_encodings

    type_ = type(value)
    encode = _json_encodings.get(type_)
    if encode is not None:
        return encode(value)
    return value


def json_decode(value):
    global _json_decodings

    if isinstance(value, list):
        return [json_decode(i) for i in value]
    elif isinstance(value, dict):
        return {k: json_decode(v) for k, v in value.items()}
    elif isinstance(value, str):
        if value.endswith("ℹ"):
            split_value = value[:-1].rsplit("ℹ", 1)
            if len(split_value) == 2:
                encoded_value, decoding_name = split_value
                decode = _json_decodings.get(decoding_name)
                if decode is None:
                    raise ValueError(f'Invalid JSON encoding type for value "{value}"')
                return decode(encoded_value)
    return value


# Obsolete constants kept for backward compatibility with API v2

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

ALL_TYPES = {
    FIELD_TYPE_LIST_STRING,
    FIELD_TYPE_LIST_INTEGER,
    FIELD_TYPE_LIST_FLOAT,
    FIELD_TYPE_LIST_BOOLEAN,
    FIELD_TYPE_LIST_DATE,
    FIELD_TYPE_LIST_DATETIME,
    FIELD_TYPE_LIST_TIME,
    FIELD_TYPE_LIST_JSON,
    FIELD_TYPE_STRING,
    FIELD_TYPE_INTEGER,
    FIELD_TYPE_FLOAT,
    FIELD_TYPE_BOOLEAN,
    FIELD_TYPE_DATE,
    FIELD_TYPE_DATETIME,
    FIELD_TYPE_TIME,
    FIELD_TYPE_JSON,
}
