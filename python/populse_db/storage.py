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

    def search(self, query):
        return self._server.search(self._connection_id, self._path, query)

    def distinct_values(self, field):
        return self._server.distinct_values(self._connection_id, self._path, field)
