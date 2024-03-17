from uuid import uuid4

import populse_db.storage
from populse_db.database import str_to_type, type_to_str

from . import Database


class StorageClient:
    def __init__(self, *args, **kwargs):
        self.database = Database(*args, **kwargs)
        self.connections = {}

    def access_rights(self, access_token):
        return "write"

    def connect(self, access_token, exclusive, write):
        connection_id = str(uuid4())
        access_rights = self.access_rights(access_token)
        if not write and access_rights in ("read", "write"):
            self.connections[connection_id] = StorageServerRead(
                self.database, exclusive
            )
        elif access_rights == "write":
            self.connections[connection_id] = StorageServerWrite(
                self.database, exclusive
            )
        else:
            raise PermissionError("database access refused")
        return connection_id

    def add_schema_collections(self, connection_id, schema_to_collections):
        return self.connections[connection_id].add_schema_collections(
            schema_to_collections
        )

    def add_collection(self, connection_id, name, primary_key):
        # Make primary_key json compatible
        if isinstance(primary_key, dict):
            primary_key = dict(
                (k, (v if isinstance(v, str) else type_to_str(v)))
                for k, v in primary_key.items()
            )
        return self.connections[connection_id].add_collection(name, primary_key)

    def add_field(
        self,
        connection_id,
        collection_name,
        field_name,
        field_type,
        description=None,
        index=False,
    ):
        if isinstance(field_type, type):
            field_type = type_to_str(field_type)
        self.connections[connection_id].add_field(
            collection_name,
            field_name,
            field_type,
            description,
            index,
        )

    def disconnect(self, connection_id, rollback):
        self.connections[connection_id]._close(rollback)

    def get(
        self,
        connection_id,
        path,
        default=None,
        fields=None,
        as_list=None,
        distinct=False,
    ):
        return self.connections[connection_id].get(
            path, default=default, fields=fields, as_list=as_list, distinct=distinct
        )

    def set(self, connection_id, path, value):
        self.connections[connection_id].set(path, value)

    def delete(self, connection_id, path):
        self.connections[connection_id].delete(path)

    def update(self, connection_id, path, value):
        self.connections[connection_id].update(path, value)

    def append(self, connection_id, path, value):
        self.connections[connection_id].append(path, value)

    def search(self, connection_id, path, query, fields=None, as_list=None):
        if isinstance(fields, tuple):
            fields = list(fields)
        return self.connections[connection_id].search(
            path, query, fields=fields, as_list=as_list
        )

    def distinct_values(self, connection_id, path, field):
        return self.connections[connection_id].distinct_values(path, field)

    def clear_database(self, connection_id):
        return self.connections[connection_id].clear_database()


class StorageServerRead:
    _read_only_error = "database is read-only"

    def __init__(self, database, exclusive):
        self._dbs = database.session(exclusive=exclusive)

    def _close(self, rollback):
        self._dbs.close(rollback=rollback)

    def _parse_path(self, path):
        if not path:
            return (None, None, None, None)
        if self._dbs.has_collection(path[0]):
            collection = self._dbs[path[0]]
            path = path[1:]
        else:
            collection = self._dbs[populse_db.storage.Storage.default_collection]
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

    def get(self, path, default=None, fields=None, as_list=False, distinct=False):
        collection, document_id, field, path = self._parse_path(path)
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

    def search(self, path, query, fields, as_list):
        collection, document_id, field, path = self._parse_path(path)
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
        result = list(collection.filter(query, fields=fields, as_list=as_list))
        return result

    def distinct_values(self, path, field):
        collection, document_id, f, path = self._parse_path(path)
        if path or f or document_id:
            raise ValueError("only collections support distinct values searching")
        for row in collection.documents(fields=[field], as_list=True, distinct=True):
            yield row[0]

    def add_schema_collections(self, schema_to_collections):
        raise PermissionError(self._read_only_error)

    def add_collection(self, name, primary_key):
        raise PermissionError(self._read_only_error)

    def add_field(
        self,
        collection_name,
        field_name,
        field_type,
        description,
        index,
    ):
        raise PermissionError(self._read_only_error)

    def append(self, path, value):
        raise PermissionError(self._read_only_error)

    def set(self, path, value):
        raise PermissionError(self._read_only_error)

    def delete(self, path):
        raise PermissionError(self._read_only_error)

    def update(self, path, value):
        raise PermissionError(self._read_only_error)

    def clear_database(self):
        raise PermissionError(self._read_only_error)


class StorageServerWrite(StorageServerRead):
    def __init__(self, database, exclusive):
        # Write access must be in an exclusive transaction to avoid
        # "Database is locked" error in SQLite with parallel accesses:
        # https://www2.sqlite.org/cvstrac/wiki?p=DatabaseIsLocked
        self._dbs = database.session(exclusive=True)
        self._init_database()

    def _init_database(self):
        if not self._dbs.has_collection(populse_db.storage.Storage.default_collection):
            self._dbs.add_collection(
                populse_db.storage.Storage.default_collection,
                populse_db.Storage.default_field,
            )
            self._dbs[populse_db.storage.Storage.default_collection][
                populse_db.storage.Storage.default_document_id
            ] = {}

    def add_schema_collections(self, schema_to_collections):
        if not self._dbs.has_collection(populse_db.storage.Storage.schema_collection):
            self._dbs.add_collection(
                populse_db.storage.Storage.schema_collection, "name"
            )
            schema_collection = self._dbs[populse_db.storage.Storage.schema_collection]
            schema_collection.add_field("version", str)
            schema_collection[schema_to_collections["name"]] = {
                "version": schema_to_collections["version"]
            }
        collections = schema_to_collections["collections"]
        for collection_name, fields_definition in collections.items():
            if not self._dbs.has_collection(collection_name):
                primary_keys = {}
                for field_name, field_def in fields_definition.items():
                    type_str, kwargs = field_def
                    if kwargs.get("primary_key", False):
                        primary_keys[field_name] = str_to_type(type_str)
                self._dbs.add_collection(collection_name, primary_keys)
                if populse_db.storage.Storage.default_field in primary_keys:
                    self._dbs[collection_name][
                        populse_db.storage.Storage.default_document_id
                    ] = {}

            collection = self._dbs[collection_name]
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

    def add_collection(self, name, primary_key):
        collection = self._dbs.get_collection(name)
        if collection is None:
            self._dbs.add_collection(name, primary_key)
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
        collection_name,
        field_name,
        field_type,
        description,
        index,
    ):
        if collection_name is None:
            collection_name = populse_db.storage.Storage.default_collection
        collection = self._dbs.get_collection(collection_name)
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

    def set(self, path, value):
        collection, document_id, field, path = self._parse_path(path)
        if not collection:
            raise ValueError("cannot set the whole content of a database")
        if field:
            if path:
                db_value = collection.document(
                    document_id, fields=[field], as_list=True
                )[0]
                container = db_value
                for i in path[:-1]:
                    container = container[i]
                container[path[-1]] = value
                value = db_value
            collection.update_document(document_id, {field: value})
        elif document_id:
            collection[document_id] = value
        else:
            collection.delete(None)
            for document in value:
                collection.add(document)

    def delete(self, path):
        collection, document_id, field, path = self._parse_path(path)
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

    def update(self, path, value):
        collection, document_id, field, path = self._parse_path(path)
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

    def append(self, path, value):
        collection, document_id, field, path = self._parse_path(path)
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

    def clear_database(self):
        self._dbs.clear()
        self._init_database()
