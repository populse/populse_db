from contextlib import contextmanager
from populse_db import Database

class PyjbitDB:
    def __init__(self, *args, **kwargs):
        self.database = Database(*args, **kwargs)

    def session(self, *args, **kwargs):
        return PyjbitSession(self.database.session(*args, **kwargs))

    def begin_session(self, *args, **kwargs):
        return PyjbitSession(self.database.begin_session(*args, **kwargs))

    def end_session(self, *args, **kwargs):
        self.database.end_session(*args, **kwargs)

    def __enter__(self):
        return self.begin_session(exclusive=False)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_session(rollback=(exc_type is not None))
    
    @property
    @contextmanager
    def exclusive(self):
        try:
            session = self.begin_session(exclusive=True)
            yield session
            self.end_session(rollback=False)
        except Exception:
            self.end_session(rollback=True)
            raise
        ...

class PyjbitSession:
    def __init__(self, dbs):
        self.dbs = dbs
    
    @property
    def data(self):
        return PyjbitData(self.dbs)
    
    @property
    def schema(self):
        return PyjbitSchema(self.dbs)


class PyjbitData:
    def __init__(self, dbs):
        self._dbs = dbs
        self._collection_path = ()
        self._inner_path = ()

    

    def __getitem__(self, key):
        result = self.__class__(self._dbs)
        new_path = self._collection_path + self._inner_path + (key,)
        if new_path in self._session._tables:
            result._table_path = new_path
        else:
            result._table_path = self._table_path
            result._inner_path = self._inner_path + (key,)
        return result



    def __getattr__(self, key):
        return self[key]


class PyjbitData(PyjbitLocator):
    def __call__(self):
        result = self._session._tables
        for index in self._inner_path:
            result = result[index]
        return result


    def __setitem__(self, key, value):
        container = self._session._tables.get(self._table_path + (key,))
        if container is not None:
            if isinstance(value, dict):
                container.clear()
                container.update(value)
            else:
                raise TypeError(f'Expected dict, got {type(value)}')
        else:
            container = self._session._tables[self._table_path]
            for index in self._inner_path:
                container = container[index]
            container[key] = value


    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        else:
            self[key] = value


class PyjbitSchema(PyjbitLocator):
    def __enter__(self):
        return object()
    
    def __exit__(self, x, y, z):
        ...


if __name__ == '__main__':
    from populse_db import Database

    with Database('/tmp/test.sqlite') as dbs:
        if not dbs.has_collection('_'):
    from pprint import pprint
    pjbs = PyjbitSession()

    with pjbs.schema.snapshots as s:
        s.subject(str, primary_key=True)
        s.time_point(str, primary_key=True)

    pjbs.schema.snapshots
    pjbs.data.x = {'this is': 'x'}
    pjbs.data.y = {'this is': 'y'}

    pprint(pjbs._tables)