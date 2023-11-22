from contextlib import contextmanager
from .database import type_to_str
from .storage_server import StorageServer


class Storage:
    def __init__(self, *args, **kwargs):
        self.server = StorageServer(*args, **kwargs)

    @contextmanager
    def session(self, exclusive=False):
        token = self.server.access_rights("TODO")
        connection_id = self.server.connect(
            token, self.get_schema(), exclusive=exclusive
        )
        try:
            yield StorageSession(self.server, connection_id)
            self.server.disconnect(connection_id, rollback=False)
        except Exception:
            self.server.disconnect(connection_id, rollback=True)
            raise

    @classmethod
    def update_schema(cls, result, other):
        for k, v in other.items():
            mine = result.get(k)
            if mine is None:
                if isinstance(v, dict):
                    v = {
                        k: (type_to_str(i) if isinstance(i, type) else i)
                        for k, i in v.items()
                    }
                elif isinstance(v, list):
                    if len(v) == 1 and isinstance(v[0], dict):
                        inner = {}
                        cls.update_schema(inner, v[0])
                        v = [inner]
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
            else:
                if isinstance(v, dict):
                    result[k].update(v)
                elif isinstance(v, list):
                    if v and isinstance(v[0], dict):
                        result[k][0].update(v)
                else:
                    if isinstance(v, type):
                        v = type_to_str(v)
                    result[k] = v

    def get_schema(self):
        schema = {}
        for cls in reversed(self.__class__.__mro__):
            s = getattr(cls, "schema", None)
            if s:
                self.update_schema(schema, s)
        return schema


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


if __name__ == "__main__":
    import json
    import os
    from datetime import datetime

    from pprint import pprint

    with open("/home/yann/dev/snapcheck/database_brainvisa/snapshots.js") as f:
        f.readline()
        snapshots = json.loads(f.read(11560057))
    select = {}
    for s in snapshots:
        select[(s["subject"], s["time_point"])] = s
    snapshots = list(select.values())
    del select

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
            "execution": [
                {
                    "execution_id": ["str", {"primary_key": True}],
                    "start_time": "datetime",
                    "end_time": datetime,
                    "status": str,
                    "capsul_executable": str,
                    "capsul_parameters": dict,
                    "software": str,
                    "software_module": str,
                    "software_version": str,
                }
            ],
            # A collection of snapshots requir
            "snapshots": [
                {
                    "subject": ["str", {"primary_key": True}],
                    "time_point": ["str", {"primary_key": True}],
                    "image": str,
                    "top": "list[int]",
                    "size": list[float],
                    "execution": "str",
                    "data_type": "str",
                    "side": "str",
                }
            ],
        }

    if os.path.exists("/tmp/test.sqlite"):
        os.remove("/tmp/test.sqlite")

    # store = Storage("/tmp/test.sqlite")
    # with store.session(exclusive=True) as d:
    #     d.dataset = {}
    #     d.snapshots = []
    store = MyStorage("/tmp/test.sqlite")

    pprint(store.get_schema())

    with store.session() as d:
        # Set a global value
        d.last_update = datetime.now()
        d.last_update.set(datetime.now())

        # Read a global value
        print(d.last_update.get())

        # Set single document fields
        d.dataset.directory = "/somewhere"
        d.dataset.schema.set("BIDS")
        # Following field is not in schema
        d.dataset.my_data = {"creation_date": datetime.now(), "manager": "me"}
        pprint(d.dataset.directory.get())

        # Adds many documents in a collection
        for snapshot in snapshots:
            d.snapshots.append(snapshot)

        try:
            # Select one document from a collection
            pprint(d.snapshots["0001292COG", "M0"].get())

            # Select one document field from a collection
            pprint(d.snapshots["0001292COG", "M0"].image.get())
        except TypeError:
            # Acces via primary_key values cannot work without schema
            pass

    with store.server.database as dbs:
        for collection in dbs.collections():
            print("=" * 40)
            print(collection.name)
            print("=" * 40)
            for f in collection.fields.values():
                print(f["name"], ":", f["type"])
            print()
            print("primary key:", list(collection.primary_key))
            print("catchall:", collection.catchall_column)
            print("-" * 40)
            columns = list(collection.fields)
            columns.append(collection.catchall_column)
            for row in dbs.sqlite.execute(
                f"SELECT {','.join('['+i+']' for i in columns)} FROM [{collection.name}]"
            ):
                print(row)
