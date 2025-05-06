import importlib
import pathlib
import types
import typing
from contextlib import contextmanager

from .database import type_to_str
from .storage_api import StorageAPI


class Storage:
    default_collection = "_"
    schema_collection = "_schema"
    default_field = "_"
    default_document_id = "_"

    def __init__(
        self,
        database_file: str | pathlib.Path,
        timeout: float | None = 10000,
        create: bool = False,
        echo_sql: typing.TextIO | None = None,
    ):
        if isinstance(database_file, pathlib.Path):
            database_file = str(database_file)
        self.storage_api = StorageAPI(
            database_file, timeout=timeout, create=create, echo_sql=echo_sql
        )
        self.access_token = self.storage_api.access_token()
        self._current_data_session = None

    @contextmanager
    def data(self, exclusive=None, write=False, create=False):
        if self._current_data_session is not None:
            storage_session, is_exclusive, is_write = self._current_data_session
            if exclusive and is_exclusive is not True:
                raise RuntimeError(
                    "Impossible to get an exclusive data session because another non exclusive data session exists"
                )
            if write and not is_write:
                raise RuntimeError(
                    "Impossible to get an write data session because another read data session exists"
                )
            yield storage_session
        else:
            connection_id = self.storage_api.connect(
                self.access_token, exclusive=exclusive, write=write, create=create
            )
            if connection_id is not None:
                try:
                    storage_session = StorageSession(self.storage_api, connection_id)
                    self._current_data_session = (storage_session, exclusive, write)
                    yield storage_session
                    self._current_data_session = None
                    self.storage_api.disconnect(connection_id, rollback=False)
                except Exception:
                    self._current_data_session = None
                    self.storage_api.disconnect(connection_id, rollback=True)
                    raise
            else:
                raise RuntimeError("Failed to establish a data session.")

    @contextmanager
    def schema(self):
        connection_id = self.storage_api.connect(
            self.access_token, exclusive=True, write=True, create=True
        )
        try:
            yield SchemaSession(self.storage_api, connection_id)
            self.storage_api.disconnect(connection_id, rollback=False)
        except Exception:
            self.storage_api.disconnect(connection_id, rollback=True)
            raise

    def start_session(self, exclusive=None, write=False, create=False):
        connection_id = self.storage_api.connect(
            self.access_token, exclusive=exclusive, write=write, create=create
        )
        return StorageSession(self.storage_api, connection_id)

    def end_session(self, storage_session, rollback=False):
        self.storage_api.disconnect(storage_session._connection_id, rollback=rollback)


class SchemaSession:
    @classmethod
    def find_schema(cls, name, version_selection=None):
        module = importlib.import_module(name)

        schemas_to_collections = getattr(module, "_schemas_to_collections", None)
        if schemas_to_collections is None:
            schemas_to_collections = {}
            index = 0
            for schema_declaration in module.schemas:
                unknown_items = set(schema_declaration) - {"version", "schema"}
                if unknown_items:
                    raise ValueError(
                        f"invalid item(s) in schema {module.__name__} (list index={index}): {','.join(unknown_items)}"
                    )
                version = schema_declaration.get("version")
                if version is None:
                    raise ValueError(
                        f"version missing in schema {module.__name__} (list index={index})"
                    )
                sversion = version.split(".")
                if len(sversion) != 3:
                    raise ValueError(
                        f"invalid version in schema {module.__name__} (list index={index}): {version}"
                    )
                schema_content = schema_declaration.get("schema")
                if schema_content is None:
                    raise ValueError(
                        f"schema missing in schema {module.__name__} (list index={index})"
                    )
                try:
                    collections = cls._parse_schema_content(schema_content)
                except Exception as e:
                    raise ValueError(
                        f"invalid schema definition {module.__name__} (list index={index})"
                    ) from e
                schema_to_collections = {
                    "name": module.__name__,
                    "version": version,
                    "collections": collections,
                }
                if version in schemas_to_collections:
                    raise ValueError(
                        f"two schemas with version {version} defined in {module.__name__}"
                    )
                schemas_to_collections[version] = schema_to_collections
                if None not in schemas_to_collections:
                    schemas_to_collections[None] = schema_to_collections
                short_version = ".".join(sversion[:2])
                if short_version not in schemas_to_collections:
                    schemas_to_collections[short_version] = schema_to_collections
                index += 1
            module._schemas_to_collections = schemas_to_collections
        return schemas_to_collections.get(version_selection)

    @classmethod
    def _parse_field(cls, name, definition):
        error = True
        if isinstance(definition, type | types.GenericAlias):
            type_str = type_to_str(definition)
            kwargs = {}
            error = False
        elif isinstance(definition, str):
            type_str = definition
            kwargs = {}
            error = False
        elif isinstance(definition, list) and len(definition) == 2:
            if isinstance(definition[1], dict):
                if isinstance(definition[0], type | types.GenericAlias):
                    type_str = type_to_str(definition[0])
                    kwargs = definition[1]
                    error = False
                elif isinstance(definition[0], str):
                    type_str = definition[0]
                    kwargs = definition[1]
                    error = False
        if error:
            raise ValueError(f'invalid definition for field "{name}"')
        return [type_str, kwargs]

    @classmethod
    def _parse_schema_content(cls, schema_content):
        if not isinstance(schema_content, dict):
            raise ValueError("schema must be a dict")
        collections = {}
        for k, v in schema_content.items():
            if isinstance(v, dict):
                fields = {kk: cls._parse_field(kk, vv) for kk, vv in v.items()}
                for name, d in fields.items():
                    _, kwargs = d
                    if name == Storage.default_field:
                        raise ValueError(f'invalid field name: "{name}')
                    if "primary_key" in kwargs:
                        raise ValueError(
                            f'primary key not allowed for unique document "{k}" in field "{name}"'
                        )
                fields[Storage.default_field] = ["str", {"primary_key": True}]
                collections[k] = fields
            elif isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict):
                fields = {kk: cls._parse_field(kk, vv) for kk, vv in v[0].items()}
                primary = False
                for name, d in fields.items():
                    type, kwargs = d
                    if name == Storage.default_field:
                        raise ValueError(f'invalid field name: "{name}')
                    if "primary_key" in kwargs:
                        primary = True
                if not primary:
                    raise ValueError(f'no primary key defined for collection "{k}"')
                collections[k] = fields
            else:
                type_str, kwargs = cls._parse_field(k, v)
                fields = collections.setdefault(Storage.default_collection, {})
                if Storage.default_field not in fields:
                    fields[Storage.default_field] = ["str", {"primary_key": True}]
                fields[k] = [type_str, kwargs]
        return collections

    def __init__(self, server, connection_id):
        super().__setattr__("_storage_api", server)
        super().__setattr__("_connection_id", connection_id)

    def add_schema(self, name, version=None):
        schema_to_collections = self.find_schema(name, version)
        if not schema_to_collections:
            raise ValueError(f"cannot find schema {name} with version {version}")
        self._storage_api.add_schema_collections(
            self._connection_id, schema_to_collections
        )

    def add_collection(self, name, primary_key):
        # Make primary_key json compatible
        if isinstance(primary_key, dict):
            primary_key = dict(
                (k, (v if isinstance(v, str) else type_to_str(v)))
                for k, v in primary_key.items()
            )
        self._storage_api.add_collection(self._connection_id, name, primary_key)

    def add_field(
        self, collection_name, field_name, field_type, description=None, index=False
    ):
        if isinstance(field_type, type | types.GenericAlias):
            field_type = type_to_str(field_type)
        self._storage_api.add_field(
            self._connection_id,
            collection_name,
            field_name,
            field_type,
            description,
            index,
        )

    def remove_field(self, collection_name, field_name):
        """
        Removes a specified field from a collection in the storage system.

        Args:
            collection_name (str): The name of the collection containing
                                   the field.
            field_name (str): The name of the field to be removed.

        This method delegates the operation to the storage API, ensuring
        the field is removed from the specified collection within the
        active connection.

        """
        self._storage_api.remove_field(self._connection_id, collection_name, field_name)

    def clear_database(self):
        return self._storage_api.clear_database(self._connection_id)

    @contextmanager
    def data(self):
        yield StorageSession(self._storage_api, self._connection_id)


class StorageSession:
    def __init__(self, storage_api, connection_id, path=None):
        super().__setattr__("_storage_api", storage_api)
        super().__setattr__("_connection_id", connection_id)
        super().__setattr__("_path", path or [])

    def __getitem__(self, key):
        return self.__class__(
            self._storage_api, self._connection_id, self._path + [key]
        )

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        self._storage_api.set(self._connection_id, self._path + [key], value)

    def __setattr__(self, key, value):
        self[key] = value

    def __delitem__(self, key):
        self._storage_api.delete(self._connection_id, self._path + [key])

    def __delattr__(self, key):
        del self[key]

    def primary_key(self):
        return self._storage_api.primary_key(self._connection_id, self._path)

    def set(self, value):
        self._storage_api.set(self._connection_id, self._path, value)

    def update(self, value):
        self._storage_api.update(self._connection_id, self._path, value)

    def get(self, default=None, fields=None, as_list=False, distinct=False):
        return self._storage_api.get(
            self._connection_id,
            self._path,
            default=default,
            fields=fields,
            as_list=as_list,
            distinct=distinct,
        )

    def count(self, query=None):
        return self._storage_api.count(self._connection_id, self._path, query=query)

    def append(self, value):
        return self._storage_api.append(self._connection_id, self._path, value)

    def distinct_values(self, field):
        return self._storage_api.distinct_values(self._connection_id, self._path, field)

    def search(self, query=None, fields=None, as_list=None, distinct=False, **kwargs):
        if kwargs and query:
            raise ValueError("Cannot combine query and equality research")
        if kwargs:
            query = " AND ".join(f'{{{k}}}=="{v}"' for k, v in kwargs.items())
        if isinstance(fields, tuple):
            fields = list(fields)
        return self._storage_api.search(
            self._connection_id,
            self._path,
            query,
            fields=fields,
            as_list=as_list,
            distinct=distinct,
        )

    def search_and_delete(self, query=None, **kwargs):
        if kwargs and query:
            raise ValueError("Cannot combine query and equality research")
        if kwargs:
            query = " AND ".join(f'{{{k}}}=="{v}"' for k, v in kwargs.items())
        return self._storage_api.search_and_delete(
            self._connection_id, self._path, query
        )

    def has_collection(self, collection):
        return self._storage_api.has_collection(
            self._connection_id, self._path, collection
        )

    def collection_names(self):
        return self._storage_api.collection_names(
            self._connection_id,
            self._path,
        )

    def keys(self):
        return self._storage_api.keys(
            self._connection_id,
            self._path,
        )
