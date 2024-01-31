import importlib
from contextlib import contextmanager

from .database import type_to_str
from .storage_server import StorageClient


class Storage:
    default_collection = "_"
    schema_collection = "_schema"
    default_field = "_"
    default_document_id = "_"

    def __init__(self, *args, **kwargs):
        self.server = StorageClient(*args, **kwargs)

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
        if isinstance(definition, type):
            type_str = type_to_str(definition)
            kwargs = {}
            error = False
        elif isinstance(definition, str):
            type_str = definition
            kwargs = {}
            error = False
        elif isinstance(definition, list) and len(definition) == 2:
            if isinstance(definition[1], dict):
                if isinstance(definition[0], type):
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

    @classmethod
    def _check_schema(cls, schema):
        result = {}
        for k, v in schema.items():
            if isinstance(v, dict):
                v = {
                    k: (type_to_str(i) if isinstance(i, type) else i)
                    for k, i in v.items()
                }
            elif isinstance(v, list):
                if len(v) == 1 and isinstance(v[0], dict):
                    v = [cls._check_schema(v[0])]
                elif (
                    len(v) == 2
                    and isinstance(v[0], (str, type))
                    and isinstance(v[1], dict)
                ):
                    if isinstance(v[0], type):
                        v = [type_to_str(v[0]), v[1]]
                else:
                    raise ValueError("invalid schema definition for field {k}: {v}")
            if isinstance(v, type):
                v = type_to_str(v)
            result[k] = v
        return result

    def add_schema(self, name, version=None):
        schema_to_collections = self.find_schema(name, version)
        if not schema_to_collections:
            raise ValueError(f"cannot find schema {name} with version {version}")
        with self.session(write=True, exclusive=True) as dbs:
            dbs._server.add_schema_collections(
                dbs._connection_id, schema_to_collections
            )

    @contextmanager
    def session(self, exclusive=False, write=False):
        token = self.server.access_rights("TODO")
        connection_id = self.server.connect(token, exclusive=exclusive, write=write)
        try:
            yield StorageSession(self.server, connection_id)
            self.server.disconnect(connection_id, rollback=False)
        except Exception:
            self.server.disconnect(connection_id, rollback=True)
            raise


class StorageSession:
    def __init__(self, server, connection_id, path=[]):
        super().__setattr__("_server", server)
        super().__setattr__("_connection_id", connection_id)
        super().__setattr__("_path", path)

    def __getitem__(self, key):
        return self.__class__(self._server, self._connection_id, self._path + [key])

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        self._server.set(self._connection_id, self._path + [key], value)

    def __setattr__(self, key, value):
        self[key] = value

    def set(self, value):
        self._server.set(self._connection_id, self._path, value)

    def get(self):
        return self._server.get(self._connection_id, self._path)

    def append(self, value):
        return self._server.append(self._connection_id, self._path, value)

    def search(self, query):
        return self._server.search(self._connection_id, self._path, query)

    def distinct_values(self, field):
        return self._server.distinct_values(self._connection_id, self._path, field)

    def add_schema(self, name, version_selection=None):
        if self._path:
            raise ValueError("add_schema is only allowed on database session")
        schema_info = Storage.find_schema(name, version_selection)
        if not schema_info:
            raise ValueError(
                f"cannot find schema {name} with version {version_selection}"
            )
        return self._server.add_schema(self._connection_id, schema_info)

    # def ensure_schema(self, schema, version=None):
    #     if self._path:
    #         raise ValueError("ensure_schema is only allowed on database session")
    #     return self._server.ensure_schema(self._connection_id, schema, version)

    # def schema_names(self):
    #     if self._path:
    #         raise ValueError("schema_names is only allowed on database session")
    #     return self._server.schema_names(self._connection_id)

    # def schema(self, schema_name):
    #     if self._path:
    #         raise ValueError("schema is only allowed on database session")
    #     return self._server.schema(self._connection_id, schema_name)
