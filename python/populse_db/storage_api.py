import importlib
import json
import os
import re
import typing
from uuid import uuid4

import requests
from cryptography.fernet import Fernet, InvalidToken

import populse_db.storage
from populse_db.database import json_decode, json_encode, str_to_type

from . import Database
from .database import populse_db_table


def serialize_exception(e):
    try:
        # Import tblib in this function avoid to make it a mandatory dependency
        import tblib

        tb = tblib.Traceback(e.__traceback__)
        result = {
            "class_module": e.__class__.__module__,
            "class_name": e.__class__.__name__,
            "traceback": tb.to_dict(),
        }

        kwargs = e.__getstate__() if hasattr(e, "__getstate__") else None
        if kwargs is None:
            result["args"] = e.args
        else:
            kwargs.pop("lineno", None)
            kwargs.pop("colno", None)
            result["kwargs"] = kwargs
    except Exception as e2:
        result = {
            "class_module": e2.__class__.__module__,
            "class_name": e2.__class__.__name__,
            "args": [f"Error while managing exception ({e}): {e2}"],
        }

    return result


def deserialize_exception(je):
    exception_class = getattr(
        importlib.import_module(je["class_module"]), je["class_name"]
    )
    exception = exception_class(*je.get("args", []), **je.get("kwargs", {}))
    tb = je.get("traceback")
    if tb is not None:
        # Import tblib in this function avoid to make it a mandatory dependency
        import tblib

        tb = tblib.Traceback.from_dict(tb)
        exception.with_traceback(tb.as_traceback())
    return exception


class StorageAPI:
    """
    Select between StorageFileAPI() and StorageServerAPI()
    """

    def __new__(
        cls,
        database_file: str,
        timeout: float | None = None,
        create: bool = False,
        echo_sql: typing.TextIO | None = None,
    ):
        if re.match("^https?:.*", database_file):
            return StorageServerAPI(database_file)
        if os.path.exists(database_file) or create:
            # sqlite3 module is optional because it does not
            # exist in Pyodide distribution (i.e. PyScript)
            import sqlite3

            cnx = sqlite3.connect(database_file, timeout=10)
            # Check if storage_server table exists
            cur = cnx.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                f"WHERE type='table' AND name='{populse_db_table}'"
            )
            if cur.fetchone()[0]:
                row = cnx.execute(
                    f"SELECT _json FROM [{populse_db_table}] WHERE category='server' AND key='url'"
                ).fetchone()
                if row:
                    return StorageServerAPI(row[0])
        return StorageFileAPI(database_file, timeout, create, echo_sql)


class StorageFileAPI:
    _read_only_error = "database is read-only"

    def __init__(
        self,
        database_file: str,
        timeout: float | None = None,
        create: bool = False,
        echo_sql: typing.TextIO | None = None,
    ):
        self.key = Fernet.generate_key()
        self.database = Database(
            database_file, timeout=timeout, create=create, echo_sql=echo_sql
        )
        self.sessions = {}

    @staticmethod
    def _init_database(dbs):
        # Initialize schema
        if not dbs.has_collection(populse_db.storage.Storage.default_collection):
            dbs.add_collection(
                populse_db.storage.Storage.default_collection,
                populse_db.Storage.default_field,
            )
            dbs[populse_db.storage.Storage.default_collection][
                populse_db.storage.Storage.default_document_id
            ] = {}

    @staticmethod
    def _parse_path(dbs, path):
        if not path:
            return (None, None, None, None)
        if dbs.has_collection(path[0]):
            collection = dbs[path[0]]
            path = path[1:]
        else:
            collection = dbs[populse_db.storage.Storage.default_collection]
        if populse_db.storage.Storage.default_field in collection.primary_key:
            document_id = populse_db.storage.Storage.default_document_id
        else:
            if not path:
                return (collection, None, None, None)
            document_id = path[0]
            path = path[1:]
        if not path:
            return (collection, document_id, None, None)
        field = path[0]
        path = path[1:]
        return (collection, document_id, field, path)

    def access_token(self):
        # For local file, we chose to not check access rights
        # at this time but to grant write access and let the
        # filesystem raise an exception when a write access is
        # tried on a read only file.
        f = Fernet(self.key)
        return f.encrypt(b"write").decode()

    def connect(self, access_token, exclusive, write, create):
        connection_id = str(uuid4())
        f = Fernet(self.key)
        try:
            access_rights = f.decrypt(access_token.encode()).decode()
        except InvalidToken:
            raise PermissionError("invalid token") from None
        if not write and access_rights in ("read", "write"):
            dbs = self.database.session(exclusive=exclusive, create=False)
        elif access_rights == "write":
            # Write access must be in an exclusive transaction to avoid
            # "Database is locked" error in SQLite with parallel accesses:
            # https://www2.sqlite.org/cvstrac/wiki?p=DatabaseIsLocked
            dbs = self.database.session(exclusive=True, create=create)
        else:
            raise PermissionError("database access refused")
        if dbs:
            self.sessions[connection_id] = (dbs, write)
            if write:
                self._init_database(dbs)
            return connection_id
        return None

    def _get_database_session(self, connection_id, write):
        session = self.sessions.get(connection_id)
        if session is None:
            raise ValueError("invalid storage connection id")
        dbs, writable = session
        if write and not writable:
            raise PermissionError(self._read_only_error)
        return dbs

    def add_schema_collections(self, connection_id, schema_to_collections):
        dbs = self._get_database_session(connection_id, write=True)
        if not dbs.has_collection(populse_db.storage.Storage.schema_collection):
            dbs.add_collection(populse_db.storage.Storage.schema_collection, "name")
            schema_collection = dbs[populse_db.storage.Storage.schema_collection]
            schema_collection.add_field("version", str)
            schema_collection[schema_to_collections["name"]] = {
                "version": schema_to_collections["version"]
            }
        collections = schema_to_collections["collections"]
        for collection_name, fields_definition in collections.items():
            if not dbs.has_collection(collection_name):
                primary_keys = {}
                for field_name, field_def in fields_definition.items():
                    type_str, kwargs = field_def
                    if kwargs.get("primary_key", False):
                        primary_keys[field_name] = str_to_type(type_str)
                dbs.add_collection(collection_name, primary_keys)
                if populse_db.storage.Storage.default_field in primary_keys:
                    dbs[collection_name][
                        populse_db.storage.Storage.default_document_id
                    ] = {}

            collection = dbs[collection_name]
            for field_name, field_def in fields_definition.items():
                type_str, kwargs = field_def
                field = collection.fields.get(field_name)
                if field:
                    type_python = str_to_type(type_str)
                    if field["type"] != type_python:
                        raise ValueError(
                            f"database has a {collection_name}.{field_name} field "
                            f"with type {field['type']} but schema requires type {type_python}"
                        )
                    if field["primary_key"] != kwargs.get("primary_key", False):
                        raise ValueError(
                            f"primary key difference between database and schema for {collection_name}.{field_name}"
                        )
                else:
                    collection.add_field(field_name, str_to_type(type_str), **kwargs)

    def add_collection(self, connection_id, name, primary_key):
        dbs = self._get_database_session(connection_id, write=True)
        collection = dbs.get_collection(name)
        if collection is None:
            dbs.add_collection(name, primary_key)
        else:
            # Check that primary key of existing collection is compatible
            # with requested one.
            if isinstance(primary_key, str):
                dict_primary_key = {primary_key: str}
            elif not isinstance(primary_key, dict):
                dict_primary_key = dict((i, str) for i in primary_key)
            else:
                dict_primary_key = dict(
                    (k, str_to_type(v)) for k, v in primary_key.items()
                )
            if collection.primary_key != dict_primary_key:
                raise ValueError(
                    f"primary key {primary_key} is not compatible with the one defined for collection {name}"
                )

    def add_field(
        self,
        connection_id,
        collection_name,
        field_name,
        field_type,
        description=None,
        index=False,
    ):
        dbs = self._get_database_session(connection_id, write=True)
        if collection_name is None:
            collection_name = populse_db.storage.Storage.default_collection
        collection = dbs.get_collection(collection_name)
        if collection is None:
            raise ValueError(f'No collection named "{collection_name}"')
        field = collection.fields.get(field_name)
        if field is None:
            collection.add_field(
                field_name, field_type, description=description, index=index
            )
        else:
            # Check compatibility with existing field
            if field["type"] != str_to_type(field_type):
                raise ValueError(
                    f"Incompatible type for field {field_name} of collection {collection_name}, requested {field_type} but existing database has {field['type']}"
                )

    def remove_field(self, connection_id, collection_name, field_name):
        """
        Removes a specified field from a collection in the database.

        Args:
            connection_id (str): The identifier for the database connection.
            collection_name (str): The name of the collection.
                                   Defaults to the storage's default
                                   collection.
            field_name (str): The name of the field to remove.

        Raises:
            ValueError: If the specified collection does not exist.
        """
        # Retrieve the database session with write access
        dbs = self._get_database_session(connection_id, write=True)
        # Use default collection name if none is provided
        collection_name = (
            collection_name or populse_db.storage.Storage.default_collection
        )
        # Retrieve the specified collection
        collection = dbs.get_collection(collection_name)

        if collection is None:
            raise ValueError(f'No collection named "{collection_name}"')

        # Remove the field if it exists
        if collection.fields.get(field_name):
            collection.remove_field(field_name)

    def disconnect(self, connection_id, rollback):
        dbs = self._get_database_session(connection_id, write=False)
        dbs.close(rollback)

    def get(
        self,
        connection_id,
        path,
        default=None,
        fields=None,
        as_list=None,
        distinct=False,
    ):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if not collection:
            raise ValueError("cannot get the whole content of a database")
        if distinct and document_id:
            raise ValueError("distinct is only allowed on collection")
        if field:
            if fields:
                raise ValueError(
                    "fields selection is only allowed for collection or document"
                )
            if as_list:
                raise ValueError("as_list is only allowed for collection or document")
            row = collection.document(document_id, fields=[field], as_list=True)
            if not row:
                return default
            value = row[0]
            if value is None:
                return default
            for i in path:
                if isinstance(value, list) and isinstance(i, str):
                    i = int(i)
                try:
                    value = value[i]
                except (KeyError, IndexError):
                    return default
            return value
        elif document_id:
            document = collection.document(document_id, fields=fields, as_list=as_list)
            if not document:
                return default
            if document_id == populse_db.storage.Storage.default_document_id:
                del document[document_id]
            return document
        else:
            return list(
                collection.documents(fields=fields, as_list=as_list, distinct=distinct)
            )

    def count(self, connection_id, path, query=None):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if path or field:
            raise ValueError("only collections can be counted")
        if document_id:
            document_id = collection.document_id(document_id)
            primary_key = list(collection.primary_key)
            document_query = " and ".join(
                f'{{{primary_key[i]}}}=="{document_id[i]}"'
                for i in range(len(document_id))
            )
            if query:
                query = f"{query} and {document_query}"
            else:
                query = document_query
        return collection.count(query)

    def primary_key(self, connection_id, path):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if not collection or document_id:
            raise ValueError("primary_key is only allowed on collections")
        return list(collection.primary_key)

    def set(self, connection_id, path, value):
        dbs = self._get_database_session(connection_id, write=True)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if not collection:
            raise ValueError("cannot set the whole content of a database")
        if field:
            if path:
                db_value = collection.document(
                    document_id, fields=[field], as_list=True
                )[0]
                container = db_value
                for i in path[:-1]:
                    if isinstance(container, list) and isinstance(i, str):
                        i = int(i)
                    container = container[i]
                i = path[-1]
                if isinstance(container, list) and isinstance(i, str):
                    i = int(i)
                container[i] = value
                value = db_value
            collection.update_document(document_id, {field: value})
        elif document_id:
            collection[document_id] = value
        else:
            collection.delete(None)
            for document in value:
                collection.add(document)

    def delete(self, connection_id, path):
        dbs = self._get_database_session(connection_id, write=True)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if not collection:
            raise ValueError("cannot delete the whole content of a database")
        if not document_id:
            raise ValueError("cannot delete a collection in a data session")
        if field:
            if path:
                db_value = collection.document(
                    document_id, fields=[field], as_list=True
                )[0]
                container = db_value
                for i in path[:-1]:
                    container = container[i]
                del container[path[-1]]
            else:
                db_value = None
            collection.update_document(document_id, {field: db_value})
        else:
            del collection[document_id]

    def update(self, connection_id, path, value):
        dbs = self._get_database_session(connection_id, write=True)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if not document_id:
            raise ValueError("cannot update a database or a collection")
        if field:
            db_value = collection.document(document_id, fields=[field], as_list=True)[0]
            if path:
                container = db_value
                for i in path:
                    container = container[i]
                container.update(value)
            else:
                db_value.update(value)
            collection.update_document(document_id, {field: db_value})
        else:
            collection.update_document(document_id, value)

    def append(self, connection_id, path, value):
        dbs = self._get_database_session(connection_id, write=True)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if not collection:
            raise ValueError("cannot set the whole content of a database")
        if field:
            db_value = collection.document(document_id, fields=[field], as_list=True)[0]
            container = db_value
            for i in path:
                container = container[i]
            container.append(value)
            collection.update_document(document_id, {field: db_value})
        elif document_id:
            raise TypeError(
                f"cannot append to document {document_id} in collection {collection.name}"
            )
        else:
            collection.add(value)

    def search(
        self, connection_id, path, query, fields=None, as_list=None, distinct=False
    ):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if path or field:
            raise ValueError("only collections can be searched")
        if document_id:
            document_id = collection.document_id(document_id)
            primary_key = list(collection.primary_key)
            document_query = " and ".join(
                f'{{{primary_key[i]}}}=="{document_id[i]}"'
                for i in range(len(document_id))
            )
            if query:
                query = f"{query} and {document_query}"
            else:
                query = document_query
        result = list(
            collection.filter(query, fields=fields, as_list=as_list, distinct=distinct)
        )
        return result

    def search_and_delete(self, connection_id, path, query):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, field, path = self._parse_path(dbs, path)
        if path or field:
            raise ValueError("only collections can be searched")
        if document_id:
            document_id = collection.document_id(document_id)
            primary_key = list(collection.primary_key)
            document_query = " and ".join(
                f'{{{primary_key[i]}}}=="{document_id[i]}"'
                for i in range(len(document_id))
            )
            if query:
                query = f"{query} and {document_query}"
            else:
                query = document_query
        collection.delete(query)

    def distinct_values(self, connection_id, path, field):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, f, path = self._parse_path(dbs, path)
        if path or f or document_id:
            raise ValueError("only collections support distinct values searching")
        for row in collection.documents(fields=[field], as_list=True, distinct=True):
            yield row[0]

    def clear_database(self, connection_id, path):
        dbs = self._get_database_session(connection_id, write=True)
        collection, document_id, f, path = self._parse_path(dbs, path)
        if collection:
            raise ValueError("clear_database can only be called on a whole database")
        dbs.clear()
        self._init_database()

    def has_collection(self, connection_id, path, collection):
        dbs = self._get_database_session(connection_id, write=False)
        c, document_id, f, path = self._parse_path(dbs, path)
        if c:
            raise ValueError("has_collection can only be called on a whole database")
        return dbs.has_collection(collection)

    def collection_names(self, connection_id, path):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, f, path = self._parse_path(dbs, path)
        if collection:
            raise ValueError("collection_names can only be called on a whole database")
        return [
            i.name
            for i in dbs.get_collections()
            if i.name
            not in {
                populse_db.storage.Storage.default_collection,
                populse_db.storage.Storage.schema_collection,
            }
        ]

    def keys(self, connection_id, path):
        dbs = self._get_database_session(connection_id, write=False)
        collection, document_id, f, path = self._parse_path(dbs, path)
        if not collection or path or f or document_id:
            raise ValueError("only collections support keys()")
        return collection.fields.keys()


def json_to_str(value):
    if isinstance(value, str):
        if not value or (value[0] not in {"[", "{", '"'} and not value[0].isdigit()):
            return value
    return json.dumps(value)


class StorageServerAPI:
    def __init__(self, url):
        self.url = url

    def access_token(self):
        return self._call("get", "access_token", None)

    def _call(self, method, route, payload, decode=False):
        if method == "get":
            if payload:
                params = {k: v for k, v in payload.items() if v is not None}
            else:
                params = None
            j = None
        else:
            j = payload
            params = None
        response = requests.request(
            method, f"{self.url}/{route}", json=j, params=params
        )
        if response.status_code == 500:
            exc = deserialize_exception(response.json())
            raise exc
        response.raise_for_status()
        result = response.json()
        if decode:
            result = json_decode(result)
        return result

    def connect(self, access_token, exclusive, write, create):
        return self._call(
            "post",
            "connection",
            dict(
                access_token=access_token,
                exclusive=bool(exclusive),
                write=bool(write),
                create=bool(create),
            ),
        )

    def disconnect(self, connection_id, rollback):
        return self._call(
            "delete",
            "connection",
            dict(connection_id=connection_id, rollback=rollback),
        )

    def add_schema_collections(self, connection_id, schema_to_collections):
        return self._call(
            "post",
            "schema_collection",
            dict(
                connection_id=connection_id,
                schema_to_collections=schema_to_collections,
            ),
        )

    def add_collection(self, connection_id, name, primary_key):
        return self._call(
            "post",
            f"schema/{name}",
            dict(connection_id=connection_id, primary_key=primary_key),
        )

    def add_field(
        self,
        connection_id,
        collection_name,
        field_name,
        field_type,
        description=None,
        index=False,
    ):
        return self._call(
            "post",
            f"schema/{collection_name}/{field_name}",
            dict(
                connection_id=connection_id,
                field_type=field_type,
                description=description,
                index=index,
            ),
        )

    def remove_field(
        self,
        connection_id,
        collection_name,
        field_name,
    ):
        return self._call(
            "delete",
            f"schema/{collection_name}/{field_name}",
            dict(
                connection_id=connection_id,
            ),
        )

    def get(
        self,
        connection_id,
        path,
        default=None,
        fields=None,
        as_list=None,
        distinct=False,
    ):
        path = json_to_str(path)
        if default is not None:
            default = json_to_str(default)
        return self._call(
            "get",
            "data",
            dict(
                connection_id=connection_id,
                path=path,
                default=default,
                fields=fields,
                as_list=as_list,
                distinct=distinct,
            ),
            decode=True,
        )

    def count(self, connection_id, path, query=None):
        path = json_to_str(path)
        return self._call(
            "get",
            "count",
            dict(connection_id=connection_id, path=path, query=query),
        )

    def primary_key(self, connection_id, path):
        path = json_to_str(path)
        return self._call(
            "get",
            "primary_key",
            dict(connection_id=connection_id, path=path),
        )

    def set(self, connection_id, path, value):
        return self._call(
            "post",
            "data",
            dict(connection_id=connection_id, path=path, value=json_encode(value)),
        )

    def delete(self, connection_id, path):
        return self._call(
            "delete", "data", dict(connection_id=connection_id, path=path)
        )

    def update(self, connection_id, path, value):
        return self._call(
            "put",
            "data",
            dict(connection_id=connection_id, path=path, value=json_encode(value)),
        )

    def append(self, connection_id, path, value):
        return self._call(
            "patch",
            "data",
            dict(connection_id=connection_id, path=path, value=json_encode(value)),
        )

    def search(
        self, connection_id, path, query, fields=None, as_list=None, distinct=False
    ):
        path = json_to_str(path)
        return self._call(
            "get",
            "search",
            dict(
                connection_id=connection_id,
                path=path,
                query=query,
                fields=fields,
                as_list=as_list,
                distinct=distinct,
            ),
            decode=True,
        )

    def search_and_delete(self, connection_id, path, query):
        return self._call(
            "delete",
            "search",
            dict(connection_id=connection_id, path=path, query=query),
            decode=True,
        )

    def distinct_values(self, connection_id, path, field):
        path = json_to_str(path)
        return self._call(
            "get",
            "distinct",
            dict(connection_id=connection_id, path=path, field=field),
            decode=True,
        )

    def clear_database(self, connection_id, path):
        return self._call(
            "delete",
            "",
            dict(connection_id=connection_id, path=path),
        )

    def has_collection(self, connection_id, path, collection):
        path = json_to_str(path)
        return self._call(
            "get",
            "has_collection",
            dict(connection_id=connection_id, path=path, collection=collection),
        )

    def collection_names(self, connection_id, path):
        path = json_to_str(path)
        return self._call(
            "get",
            "collection_names",
            dict(connection_id=connection_id, path=path),
        )

    def keys(self, connection_id, path):
        path = json_to_str(path)
        return self._call(
            "get",
            "keys",
            dict(connection_id=connection_id, path=path),
        )
