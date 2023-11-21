from contextlib import contextmanager
from populse_db.database import str_to_type


class Storage:
    default_collection = "_"
    default_field = "_"
    default_document_id = "_"

    def __init__(self, *args, **kwargs):
        self.database = Database(*args, **kwargs)

    @property
    @contextmanager
    def data(self):
        try:
            session = self.database.begin_session(exclusive=False)
            yield StorageGlobal(session)
            self.database.end_session(rollback=False)
        except Exception:
            self.database.end_session(rollback=True)
            raise

    @property
    @contextmanager
    def data_exclusive(self):
        try:
            session = self.database.begin_session(exclusive=True)
            yield StorageGlobal(session)
            self.database.end_session(rollback=False)
        except Exception:
            self.end_session(rollback=True)
            raise

    @classmethod
    def update_schema(cls, result, other):
        for k, v in other.items():
            mine = result.get(k)
            if mine is None:
                if isinstance(v, dict):
                    v = v.copy()
                elif isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict):
                    v = [v[0].copy()]
                result[k] = v
            else:
                if isinstance(v, dict):
                    result[k].update(v)
                elif isinstance(v, list):
                    if v and isinstance(v[0], dict):
                        result[k][0].update(v)
                else:
                    result[k] = v

    def get_schema(self):
        schema = {}
        for cls in reversed(self.__class__.__mro__):
            s = getattr(cls, "schema", None)
            if s:
                self.update_schema(schema, s)
        return schema

    def _create_collection(self, dbs, collection_name, definition):
        primary_key = []
        collection_fields = {}
        for kk, vv in definition.items():
            if isinstance(vv, type):
                collection_fields[kk] = (vv, {})
            elif isinstance(vv, str):
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
        if not dbs.has_collection(collection_name):
            if not primary_key:
                raise ValueError(
                    f"invalid schema, collection {collection_name} must have at least a primary key"
                )
            dbs.add_collection(collection_name, primary_key)
        collection = dbs[collection_name]
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

    def create(self):
        schema = self.get_schema()
        with self.database.exclusive as dbs:
            if not dbs.has_collection(self.default_collection):
                dbs.add_collection(self.default_collection, self.default_field)
            default_collection = dbs[self.default_collection]
            for k, v in schema.items():
                if isinstance(v, dict):
                    # Create the collection first to set its primary key
                    self._create_collection(
                        dbs, k, {self.default_field: [str, {"primary_key": True}]}
                    )
                    # Then call create again to add fields from schema definition
                    # and raise an error if a primary key is defined.
                    self._create_collection(dbs, k, v)
                    # Create an empty singleton document
                    dbs[k][Storage.default_document_id] = {}
                elif isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict):
                    self._create_collection(dbs, k, v[0])
                elif isinstance(v, (type, str)):
                    if isinstance(v, str):
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
            dbs[self.default_collection][self.default_document_id] = {}


class StorageGlobal:
    def __init__(self, db):
        super().__setattr__("_db", db)

    def __getitem__(self, key):
        if self._db.has_collection(key):
            collection = self._db[key]
            if collection:
                if Storage.default_field in collection.primary_key:
                    return StorageDocument(self._db, key, Storage.default_document_id)
                return StorageCollection(self._db, key)
        return StorageDocumentField(
            self._db,
            Storage.default_collection,
            Storage.default_document_id,
            key,
        )

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        self[key].set(value)

    def __setattr__(self, key, value):
        self[key] = value


class StorageCollection:
    def __init__(self, db, collection):
        super().__setattr__("_db", db)
        super().__setattr__("_collection", collection)

    def __getitem__(self, key):
        if Storage.default_field in self._db[self._collection].primary_key:
            return StorageDocumentField(
                self._db, self._collection, Storage.default_document_id, key
            )
        else:
            return StorageDocument(self._db, self._collection, key)

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        self[key].set(value)

    def __setattr__(self, key, value):
        self[key] = value

    def append(self, document, replace=False):
        self._db[self._collection].add(document, replace=replace)

    def __iter__(self):
        return self._db[self._collection].documents()

    def get(self):
        return list(self)

    def set(self, value):
        collection = self._db[self._collection]
        collection.delete(None)
        for document in value:
            collection.add(document)


class StorageDocument:
    def __init__(self, db, collection, document_id):
        super().__setattr__("_db", db)
        super().__setattr__("_collection", collection)
        super().__setattr__("_document_id", document_id)

    def __getitem__(self, key):
        return StorageDocumentField(self._db, self._collection, self._document_id, key)

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        self[key].set(value)

    def __setattr__(self, key, value):
        self[key] = value

    def get(self):
        return self._db[self._collection][self._document_id]

    def set(self, value):
        self._db[self._collection][self._document_id] = value


class StorageDocumentField:
    def __init__(self, db, collection, document_id, field, path=()):
        super().__setattr__("_db", db)
        super().__setattr__("_collection", collection)
        super().__setattr__("_document_id", document_id)
        super().__setattr__("_field", field)
        super().__setattr__("_path", path)

    def __getitem__(self, key):
        return StorageDocumentField(
            self._db, self._collection, self._document_id, self._path + (key,)
        )

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        self[key].set(value)

    def __setattr__(self, key, value):
        self[key] = value

    def set(self, value):
        if self._path:
            db_value = self._db[self._collection].document(
                self._document_id, fields=[self._field], as_list=True
            )[0]
            container = db_value
            for i in self._path[:-1]:
                container = container[i]
            container[self._path[-1]] = value
        else:
            db_value = value
        self._db[self._collection].update_document(
            self._document_id, {self._field: db_value}
        )

    def get(self):
        db_value = self._db[self._collection].document(
            self._document_id, fields=[self._field], as_list=True
        )[0]
        if self._path:
            value = db_value
            for i in self._path:
                value = value[i]
        else:
            value = db_value
        return value


if __name__ == "__main__":
    import os
    from datetime import datetime
    from populse_db import Database

    from pprint import pprint

    # A schema can be defined in a class deriving from
    # Storage
    class MyStorage(Storage):
        schema = {
            # A global value
            "last_update": datetime,
            # A single document (i.e. not in a collection)
            "dataset": {
                "directory": str,
                "schema": str,
            },
            # A collection of metadata associated to a path.
            "metadata": [
                {
                    "path": [str, {"primary_key": True}],
                    "subject": str,
                    "time_point": str,
                    "history": list[str],  # contains a list of execution_id
                }
            ],
            # A collection of executions to track data provenance
            # "execution": [{}],
            "snapshots": [
                {
                    "image": str,
                    "top": "list[int]",
                    "size": list[float],
                    "subject": ["str", {"primary_key": True}],
                    "time_point": ["str", {"primary_key": True}],
                    "software": "str",
                    "data_type": "str",
                    "side": "str",
                }
            ],
        }

    if os.path.exists("/tmp/test.sqlite"):
        os.remove("/tmp/test.sqlite")
    store = MyStorage("/tmp/test.sqlite")
    pprint(store.get_schema())
    store.create()
    store.create()
    with store.database as dbs:
        for collection in dbs.collections():
            print(collection.name)
            print(
                "  fields:",
                dict((f["name"], f["type"]) for f in collection.fields.values()),
            )
            print("  primary key:", collection.primary_key)
            print("  catchall:", collection.catchall_column)

    with store.data as d:
        d.last_update = datetime.now()
        print(d.last_update.get())
        d.dataset = {}
        d.dataset.directory.set("/somewhere")
        print(d.dataset.directory.get())

        d.snapshots = []
        d.snapshots.append(
            {
                "orientation": "coronal",
                "top": [0.0, 0.0],
                "size": [872.0, 887.0],
                "dataset": "database_brainvisa",
                "software": "morphologist",
                "time_point": "M0",
                "data_type": "greywhite",
                "side": "both",
                "image": "/home/yann/dev/snapcheck/database_brainvisa/snapshots/morphologist/M0/greywhite/snapshot_greywhite_0001017COG_M0.png",
                "subject": "0001017COG",
            },
        )
        pprint(d.snapshots.get())
