from uuid import uuid4
from . import Database
from populse_db.database import str_to_type


class StorageServer:
    default_collection = "_"
    default_field = "_"
    default_document_id = "_"

    def __init__(self, *args, **kwargs):
        self.database = Database(*args, **kwargs)
        self.connections = {}

    def access_rights(self, access_token):
        return "write"

    def connect(self, access_token, schema, exclusive):
        connection_id = str(uuid4())
        access_rights = self.access_rights(access_token)
        if access_rights == "read":
            self.connections[connection_id] = StorageServerRead(
                self.database, schema, exclusive
            )
        elif access_rights == "write":
            self.connections[connection_id] = StorageServerWrite(
                self.database, schema, exclusive
            )
        else:
            raise PermissionError("invalid access token")
        return connection_id

    def disconnect(self, connection_id, rollback):
        self.connections[connection_id]._close(rollback)

    def get(self, connection_id, path):
        return self.connections[connection_id].get(path)

    def set(self, connection_id, path, value):
        self.connections[connection_id].set(path, value)

    def append(self, connection_id, path, value):
        self.connections[connection_id].append(path, value)


class StorageServerRead:
    def __init__(self, database, schema, exclusive):
        self._dbs = database.session(exclusive=exclusive)
        self._check_schema(schema, create=False)

    def _close(self, rollback):
        self._dbs.close(rollback=rollback)

    def _check_collection(self, collection_name, definition, create):
        primary_key = []
        collection_fields = {}
        for kk, vv in definition.items():
            if isinstance(vv, str):
                collection_fields[kk] = (str_to_type(vv), {})
            elif isinstance(vv, list) and len(vv) == 2:
                if isinstance(vv[0], type):
                    t = vv[0]
                else:
                    t = str_to_type(vv[0])
                kwargs = vv[1].copy()
                if kwargs.pop("primary_key"):
                    primary_key.append(kk)
                collection_fields[kk] = (t, kwargs)
        if not self._dbs.has_collection(collection_name):
            if create:
                if not primary_key:
                    raise ValueError(
                        f"invalid schema, collection {collection_name} is "
                        "missing and cannot be created without primary key"
                    )
                self._dbs.add_collection(collection_name, primary_key)
            else:
                raise LookupError(
                    f"collection {collection_name} is required in the schema but is not in the database"
                )
        collection = self._dbs[collection_name]
        if primary_key and list(collection.primary_key) != primary_key:
            raise ValueError(
                f"primary key of collection {collection_name} is {list(collection.primary_key)} in database but is {primary_key} in schema"
            )
        for n, d in collection_fields.items():
            f = collection.fields.get(n)
            t, kwargs = d
            if f:
                if f["type"] != t:
                    raise ValueError(
                        f"type of field {collection_name}.{n} is {f['type']} in database but is {t} in schema"
                    )
            else:
                collection.add_field(n, t, **kwargs)

    def _check_schema(self, schema, create):
        if not self._dbs.has_collection(StorageServer.default_collection):
            if create:
                self._dbs.add_collection(
                    StorageServer.default_collection, StorageServer.default_field
                )
            else:
                raise LookupError("database was not properly initialized")
        default_collection = self._dbs[StorageServer.default_collection]
        for k, v in schema.items():
            if isinstance(v, dict):
                # Create the collection first to set its primary key
                self._check_collection(
                    k,
                    {StorageServer.default_field: [str, {"primary_key": True}]},
                    create,
                )
                # Then call create again to add fields from schema definition
                # and raise an error if a primary key is defined.
                self._check_collection(k, v, create)
                if not self._dbs[k].has_document(StorageServer.default_document_id):
                    if create:
                        # Create an empty singleton document
                        self._dbs[k][StorageServer.default_document_id] = {}
                    else:
                        raise LookupError(
                            f"single document is missing from collection {k}"
                        )
            elif isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict):
                self._check_collection(k, v[0], create)
            elif isinstance(v, str):
                v = str_to_type(v)
                f = default_collection.fields.get(k)
                if f:
                    if f["type"] != v:
                        raise ValueError(
                            f"type of field {k} is {f['type']} in database but is {v} in schema"
                        )
                else:
                    default_collection.add_field(k, v)
            else:
                raise ValueError(f"invalid schema definition for field {k}: {v}")
        if not self._dbs[StorageServer.default_collection].has_document(
            StorageServer.default_document_id
        ):
            if create:
                self._dbs[StorageServer.default_collection][
                    StorageServer.default_document_id
                ] = {}
            else:
                raise LookupError("global document is missing from database")

    def _parse_path(self, path):
        if not path:
            return (None, None, None, None)
        if self._dbs.has_collection(path[0]):
            collection = self._dbs[path[0]]
            path = path[1:]
        else:
            collection = self._dbs[StorageServer.default_collection]
        if StorageServer.default_field in collection.primary_key:
            document_id = StorageServer.default_document_id
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

    def get(self, path):
        collection, document_id, field, path = self._parse_path(path)
        if not collection:
            raise ValueError("cannot get the whole content of a database")
        if field:
            value = collection.document(document_id, fields=[field], as_list=True)[0]
            for i in path:
                value = value[i]
            return value
        elif document_id:
            document = collection[document_id]
            if document_id == StorageServer.default_document_id:
                del document[document_id]
            return document
        else:
            return list(collection.documents())


class StorageServerWrite(StorageServerRead):
    def __init__(self, database, schema, exclusive):
        self._dbs = database.session(exclusive=exclusive)
        self._check_schema(schema, create=True)

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
