import functools
import json
import os
import sqlite3
from datetime import date, datetime, time

import dateutil

from ..database import (
    DatabaseCollection,
    DatabaseSession,
    json_decode,
    json_dumps,
    json_encode,
    populse_db_table,
    str_to_type,
    type_to_sqlite,
    type_to_str,
)
from ..filter import FilterToSQL, filter_parser

"""
SQLite3 implementation of populse_db engine.

A populse_db engine is created when a DatabaseSession object is created
(typically within a "with" statement)
"""

if tuple(int(i) for i in sqlite3.sqlite_version.split(".")) < (3, 38, 0):
    raise NotImplementedError(
        f"populse_db requires a SQLite version > 3.38.0 but current version is {sqlite3.sqlite_version}"
    )
sqlite3.register_adapter(datetime, lambda d: d.isoformat())
sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(time, lambda d: d.isoformat())
sqlite3.register_converter("bool", lambda b: bool(int(b)))
sqlite3.register_converter("datetime", lambda b: dateutil.parser.parse(b))
sqlite3.register_converter("date", lambda b: dateutil.parser.parse(b).date())
sqlite3.register_converter("time", lambda b: dateutil.parser.parse(b).time())


def create_sqlite_session_factory(url):
    if url.path:
        sqlite_file = url.path
    elif url.netloc:
        sqlite_file = url.netloc
    result = functools.partial(sqlite_session_factory, sqlite_file)
    return result


def sqlite_session_factory(sqlite_file, *args, create=False, **kwargs):
    if not create and not os.path.exists(sqlite_file):
        return None
    return SQLiteSession(sqlite_file, *args, **kwargs)


class ParsedFilter(str):
    pass


class SQLiteSession(DatabaseSession):
    database_exceptions = (
        sqlite3.OperationalError,
        sqlite3.IntegrityError,
    )

    def __init__(self, sqlite_file, exclusive=False, timeout=None, echo_sql=None):
        self.echo_sql = echo_sql
        self.sqlite = sqlite3.connect(
            sqlite_file,
            isolation_level=None,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        self.exclusive = exclusive
        if timeout:
            self.sqlite.execute(f"PRAGMA busy_timeout={timeout}")
        self.sqlite.executescript(
            "PRAGMA synchronous=OFF;"
            "PRAGMA case_sensitive_like=ON;"
            "PRAGMA foreign_keys=ON;"
            f"BEGIN {('EXCLUSIVE' if self.exclusive else 'DEFERRED')};"
        )
        self._collection_cache = {}
        # Iterate on all collections to put them in cache
        all(self)

    def close(self, rollback=False):
        if rollback:
            self.sqlite.rollback()
        else:
            self.sqlite.commit()
        self.sqlite.close()

    def has_collection(self, name):
        return name in self._collection_cache

    def get_collection(self, name):
        return self._collection_cache.get(name)

    def __getitem__(self, collection_name):
        result = self._collection_cache.get(collection_name)
        if result is None:
            result = SQLiteCollection(self, collection_name)
            self._collection_cache[collection_name] = result
        return result

    def execute(self, sql, data=None):
        try:
            if data:
                result = self.sqlite.execute(sql, data)
                if self.echo_sql:
                    print(sql, data, file=self.echo_sql)
                return result
            else:
                result = self.sqlite.execute(sql)
                if self.echo_sql:
                    print(sql, file=self.echo_sql)
                return result
        except sqlite3.OperationalError as e:
            raise sqlite3.OperationalError(f"Error in SQL request: {sql}") from e

    def commit(self):
        self.sqlite.commit()
        self.sqlite.execute(f"BEGIN {('EXCLUSIVE' if self.exclusive else 'DEFERRED')}")

    def rollback(self):
        self.sqlite.rollback()
        self.sqlite.execute(f"BEGIN {('EXCLUSIVE' if self.exclusive else 'DEFERRED')}")

    def settings(self, category, key, default=None):
        try:
            sql = f"SELECT _json FROM [{populse_db_table}] WHERE category=? and key=?"
            cur = self.execute(sql, [category, key])
        except sqlite3.OperationalError:
            return default
        j = cur.fetchone()
        if j:
            return json.loads(j[0])
        return default

    def set_settings(self, category, key, value):
        sql = f"INSERT OR REPLACE INTO {populse_db_table} (category, key, _json) VALUES (?,?,?)"
        data = [category, key, json_dumps(value)]
        retry = False
        try:
            self.execute(sql, data)
        except sqlite3.OperationalError:
            retry = True
        if retry:
            sql2 = (
                "CREATE TABLE IF NOT EXISTS "
                f"[{populse_db_table}] ("
                "category TEXT NOT NULL,"
                "key TEXT NOT NULL,"
                "_json TEXT,"
                "PRIMARY KEY (category, key))"
            )
            self.execute(sql2)
            self.execute(sql, data)

    def clear(self):
        """
        Erase the whole database content.
        """
        sql = 'SELECT name FROM sqlite_master WHERE type = "table"'
        tables = [i[0] for i in self.execute(sql)]
        for table in tables:
            sql = f"DROP TABLE [{table}]"
            self.execute(sql)
        self._collection_cache = {}

    def add_collection(
        self,
        name,
        primary_key=DatabaseSession.default_primary_key,
        catchall_column="_catchall",
    ):
        if isinstance(primary_key, str):
            dict_primary_key = {primary_key: "str"}
        elif isinstance(primary_key, list | tuple):
            dict_primary_key = {i: "str" for i in primary_key}
        else:
            dict_primary_key = {
                k: (
                    type_to_sqlite(str_to_type(v))
                    if isinstance(v, str)
                    else type_to_str(v)
                )
                for k, v in primary_key.items()
            }
        sql = (
            f"CREATE TABLE [{name}] ("
            f"{','.join(f'[{n}] {t} NOT NULL' for n, t in dict_primary_key.items())},"
            f"{catchall_column} dict,"
            f"PRIMARY KEY ({','.join(f'[{i}]' for i in dict_primary_key.keys())}))"
        )
        self.execute(sql)
        # Accessing the collection to put it in cache
        self[name]

    def remove_collection(self, name):
        sql = f"DROP TABLE [{name}]"
        self.execute(sql)
        self._collection_cache.pop(name, None)

    def __iter__(self):
        sql = "SELECT name FROM sqlite_master WHERE type='table'"
        for row in self.execute(sql):
            table = row[0]
            if table == populse_db_table:
                continue
            yield self[table]


class SQLiteCollection(DatabaseCollection):
    _column_encodings = {
        list: (
            lambda l: (None if l is None else json_dumps(l)),  # noqa: E741
            lambda l: (None if l is None else json.loads(l)),  # noqa: E741
        ),
        dict: (
            lambda d: (None if d is None else json_dumps(d)),
            lambda d: (None if d is None else json.loads(d)),
        ),
    }

    def __init__(self, session, name):
        super().__init__(session, name)
        settings = self.session.settings("collection", name, {})
        sql = f"pragma table_info([{self.name}])"
        bad_table = True
        catchall_column_found = False
        for row in self.session.execute(sql):
            bad_table = False
            if row[1] == self.catchall_column:
                catchall_column_found = True
                continue
            column_type_str = row[2].lower()
            column_type = str_to_type(column_type_str)
            main_type = getattr(column_type, "__origin__", None) or column_type
            encoding = self._column_encodings.get(main_type)
            if row[5]:
                self.primary_key[row[1]] = column_type
            field = {
                "collection": self.name,
                "name": row[1],
                "primary_key": bool(row[5]),
                "type": column_type,
                "encoding": encoding,
            }
            field_settings = settings.get("fields", {}).get(row[1], {})
            field.update(field_settings)
            if field_settings.get("bad_json", False):
                self.bad_json_fields.add(row[1])
            self.fields[row[1]] = field
        if bad_table:
            raise ValueError(f"No such database table: {name}")
        if self.catchall_column and not catchall_column_found:
            raise ValueError(f"table {name} must have a column {self.catchall_column}")

    def add_field(
        self, name, field_type, description=None, index=False, bad_json=False
    ):
        if isinstance(field_type, str):
            field_type = str_to_type(field_type)
        sql = f"ALTER TABLE [{self.name}] ADD COLUMN [{name}] {type_to_sqlite(field_type)}"
        self.session.execute(sql)
        if index:
            sql = f"CREATE INDEX [{self.name}_{name}] ON [{self.name}] ([{name}])"
            self.session.execute(sql)
        settings = self.settings()
        settings.setdefault("fields", {})[name] = {
            "description": description,
            "index": index,
            "bad_json": bad_json,
        }
        self.set_settings(settings)
        field = {
            "collection": self.name,
            "name": name,
            "primary_key": False,
            "type": field_type,
            "description": description,
            "index": index,
            "bad_json": bad_json,
            "encoding": self._column_encodings.get(
                getattr(field_type, "__origin__", None) or field_type
            ),
        }
        self.fields[name] = field
        if bad_json:
            self.bad_json_fields.add(name)

    def remove_field(self, name):
        """
        Removes a specified field from the table and updates associated
        metadata.

        Args:
            name (str): The name of the field to remove.

        Raises:
            ValueError: If attempting to remove a primary key field.
            NotImplementedError: If the SQLite version is below 3.35.0,
                                 which does not support removing columns.
        """
        if name in self.primary_key:
            raise ValueError("Cannot remove a key field")

        sql = f"ALTER TABLE [{self.name}] DROP COLUMN [{name}]"
        self.session.execute(sql)
        # Update metadata
        settings = self.settings()
        fields = settings.setdefault("fields", {})
        fields.pop(name, None)
        self.set_settings(settings)
        self.fields.pop(name, None)
        self.bad_json_fields.discard(name)

    def has_document(self, document_id):
        document_id = self.document_id(document_id)
        sql = f"SELECT count(*) FROM [{self.name}] WHERE {' AND '.join(f'[{i}] = ?' for i in self.primary_key)}"
        return next(self.session.execute(sql, document_id))[0] != 0

    def _documents(self, where, where_data, fields, as_list, distinct):
        json_decode_columns = []
        if fields:
            columns = []
            catchall_fields = False
            for field in fields:
                if field in self.fields:
                    columns.append(f"[{field}]")
                else:
                    json_decode_columns.append(len(columns))
                    columns.append(f"[{self.catchall_column}] -> '$.{field}'")
        else:
            fields = self.fields
            columns = [f"[{i}]" for i in fields]
            catchall_fields = bool(self.catchall_column)
            if catchall_fields:
                columns.append(f"[{self.catchall_column}] -> '$'")
                if as_list:
                    raise ValueError(
                        f"as_list=True cannot be used on {self.name} without a fields list because two documents can have different fields"
                    )

        sql = f"SELECT {('DISTINCT ' if distinct else '')}{','.join(columns)} FROM [{self.name}]"
        if where:
            sql += f" WHERE {where}"
        cur = self.session.execute(sql, where_data)
        for row in cur:
            for i in json_decode_columns:
                if row[i] is None:
                    continue
                if isinstance(row, tuple):
                    row = list(row)
                row[i] = json.loads(row[i])
            if catchall_fields:
                if row[-1] is not None:
                    catchall = json.loads(row[-1])
                    if isinstance(catchall_fields, set):
                        catchall = {i: catchall[i] for i in catchall_fields}
                else:
                    catchall = {}
                row = row[:-1]
            else:
                catchall = {}
            if columns[-1] == self.catchall_column:
                columns = columns[:-1]
            document = json_decode(catchall)
            if isinstance(document, dict):
                document.update(zip(fields, row, strict=True))
                for field, value in document.items():
                    encoding = self.fields.get(field, {}).get("encoding")
                    if encoding:
                        encode, decode = encoding
                        value = decode(value)
                    if field in self.bad_json_fields:
                        value = json_decode(value)
                    document[field] = value
                if as_list:
                    yield [document[i] for i in fields]
                else:
                    yield document
            else:
                yield document

    def count(self, filter=None):
        where = self.parse_filter(filter)
        sql = f"SELECT COUNT(*) FROM [{self.name}]"
        if where:
            sql += f" WHERE {where}"
        return self.session.execute(sql, None).fetchone()[0]

    def document(self, document_id, fields=None, as_list=False):
        document_id = self.document_id(document_id)
        where = f"{' AND '.join(f'[{i}] = ?' for i in self.primary_key)}"
        try:
            return next(self._documents(where, document_id, fields, as_list, False))
        except StopIteration:
            return None

    def documents(self, fields=None, as_list=False, distinct=False):
        yield from self._documents(None, None, fields, as_list, distinct)

    def add(self, document, replace=False):
        document_id = tuple(document.get(i) for i in self.primary_key)
        self._set_document(document_id, document, replace=replace)

    def __setitem__(self, document_id, document):
        document_id = self.document_id(document_id)
        self._set_document(document_id, document, replace=True)

    def _dict_to_sql_update(self, document):
        columns = []
        data = []
        catchall_column = None
        catchall_data = None
        catchall = ...
        if isinstance(document, dict):
            catchall = {}
            for field, value in document.items():
                if field in self.primary_key:
                    continue
                if field in self.bad_json_fields:
                    value = json_encode(value)
                if field in self.fields:
                    columns.append(field)
                    data.append(self._encode_column_value(field, value))
                else:
                    catchall[field] = value
        else:
            catchall_column = self.catchall_column
            catchall_data = document
        if catchall is not ...:
            if not self.catchall_column:
                raise ValueError(
                    f"Collection {self.name} cannot store this value: {catchall}"
                )
            bad_json = False
            try:
                catchall_data = catchall
                json_dumps(catchall)
            except TypeError:
                if isinstance(catchall, dict):
                    bad_json = True
                else:
                    raise
            if bad_json:
                jsons = {}
                for field, value in catchall.items():
                    bad_json = False
                    try:
                        j = value
                        json_dumps(value)
                    except TypeError:
                        bad_json = True
                    if bad_json:
                        self.add_field(field, dict, bad_json=True)
                        column_value = self._encode_column_value(field, value)
                        columns.append(field)
                        data.append(column_value)
                    else:
                        jsons[field] = j
                if jsons:
                    catchall_column = self.catchall_column
                    catchall_data = jsons

            else:
                catchall_column = self.catchall_column
        return columns, data, catchall_column, catchall_data

    def _set_document(self, document_id, document, replace):
        columns, data, catchall_column, catchall_data = self._dict_to_sql_update(
            document
        )
        catchall_data = json.dumps(json_encode(catchall_data))
        columns = [i for i in self.primary_key] + columns
        data = [i for i in document_id] + data
        if catchall_column:
            columns.append(catchall_column)
            data.append(catchall_data)
        if replace:
            replace = " OR REPLACE"
        else:
            replace = ""
        sql = f"INSERT{replace} INTO [{self.name}] ({','.join(f'[{i}]' for i in columns)}) values ({','.join('?' for i in data)})"
        self.session.execute(sql, data)

    def update_document(self, document_id, partial_document):
        document_id = self.document_id(document_id)
        if not all(
            y is None or x == y
            for x, y in zip(
                document_id,
                (partial_document.get(i) for i in self.primary_key),
                strict=True,
            )
        ):
            raise ValueError("Modification of a document's primary key is not allowed")
        columns, data, catchall_column, catchall_data = self._dict_to_sql_update(
            partial_document
        )

        if catchall_column and catchall_data:
            catchall_update = [
                f'[{catchall_column}]=json_set(IFNULL([{catchall_column}],"{{}}"),{",".join("?,json(?)" for i in catchall_data)})'
            ]
            for k, v in catchall_data.items():
                data.append(f"$.{k}")
                data.append(json.dumps(v))
        else:
            catchall_update = []
        where = " AND ".join(f"[{i}]=?" for i in self.primary_key)
        data = data + [i for i in document_id]
        affectations = [f"[{i}]=?" for i in columns] + catchall_update
        if not affectations:
            return
        sql = f"UPDATE [{self.name}] SET {','.join(affectations)} WHERE {where}"
        cur = self.session.execute(sql, data)
        if not cur.rowcount:
            raise ValueError(f"Document with key {document_id} does not exist")

    def __delitem__(self, document_id):
        document_id = self.document_id(document_id)
        sql = f"DELETE FROM [{self.name}] WHERE {' AND '.join(f'[{i}] = ?' for i in self.primary_key)}"
        self.session.execute(sql, document_id)

    def parse_filter(self, filter):
        if filter is None or isinstance(filter, ParsedFilter):
            return filter
        tree = filter_parser().parse(filter)
        where_filter = FilterToSQL(self).transform(tree)
        if where_filter is None:
            return None
        else:
            return ParsedFilter(" ".join(where_filter))

    def filter(self, filter, fields=None, as_list=False, distinct=False):
        parsed_filter = self.parse_filter(filter)
        yield from self._documents(
            parsed_filter, None, fields=fields, as_list=as_list, distinct=distinct
        )

    def delete(self, filter):
        where = self.parse_filter(filter)
        sql = f"DELETE FROM [{self.name}]"
        if where:
            sql += f" WHERE {where}"
        cur = self.session.execute(sql)
        return cur.rowcount
