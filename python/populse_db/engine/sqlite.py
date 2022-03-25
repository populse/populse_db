from datetime import time, date, datetime
import dateutil
import json
import sqlite3

from ..database import DatabaseSession, str_to_type, type_to_str
from ..filter import FilterToSQL, filter_parser

'''
SQLite3 implementation of populse_db engine.

A populse_db engine is created when a DatabaseSession object is created 
(typically within a "with" statement)
'''

class ParsedFilter(str):
    pass

class SQLiteSession(DatabaseSession):
    @staticmethod
    def parse_url(url):
        if url.path:
            args = (url.path,)
        elif url.netloc:
            args = (url.netloc,)
        return args, {}
    
    def __init__(self, sqlite_file):
        self.sqlite = sqlite3.connect(sqlite_file)
        self._collection_cache = {}

    def __getitem__(self, collection_name):
        result = self._collection_cache.get(collection_name)
        if result is None:
            result = SQLiteCollection(self, collection_name)
            self._collection_cache[collection_name] = result
        return result
    
    def execute(self, sql, data=None):
        print('!sql!', sql, data)
        if data:
            return self.sqlite.execute(sql, data)
        else:
            return self.sqlite.execute(sql)

    def get_settings(self, category, key, default=None):
        try:
            sql = f'SELECT _json FROM [{self.populse_db_table}] WHERE category=? and key=?'
            cur = self.execute(sql, [category, key])
        except sqlite3.OperationalError:
            return default
        j = cur.fetchone()
        if j:
            return json.loads(j[0])
        return default

    def set_settings(self, category, key, value):
        sql = f'INSERT OR REPLACE INTO {self.populse_db_table} (category, key, _json) VALUES (?,?,?)'
        data = [category, key, json_dumps(value)]
        retry = False
        try:
            self.execute(sql, data)
        except sqlite3.OperationalError:
            retry = True
        if retry:
            sql2 = ('CREATE TABLE IF NOT EXISTS '
                f'[{self.populse_db_table}] ('
                'category TEXT NOT NULL,'
                'key TEXT NOT NULL,'
                '_json TEXT,'
                'PRIMARY KEY (category, key))')
            self.execute(sql2)
            self.execute(sql, data)

    def clear(self):
        """
        Erase the whole database content.
        """

        sql = f'SELECT name FROM sqlite_schema'
        tables = [i[0] for i in self.execute(sql)]
        for table in tables:
            sql = f'DROP TABLE {table}'
            self.execute(sql)
        self._collection_cache = {}
    
    def add_collection(self, name, primary_key, catchall_column='_catchall'):
        if isinstance(primary_key, str):
            primary_key = {primary_key: str}
        elif isinstance(primary_key, (list, tuple)):
            primary_key = dict((i,str) for i in primary_key)
        sql = (f'CREATE TABLE [{name}] ('
            f'{",".join(f"[{n}] {type_to_str(t)} NOT NULL" for n, t in primary_key.items())},'
            f'{catchall_column} dict,'
            f'PRIMARY KEY ({",".join(f"[{i}]" for i in primary_key.keys())}))')
        self.execute(sql)

    def remove_collection(self, name):
        sql = f'DROP TABLE [{name}]'
        self.execute(sql)
        self._collection_cache.pop(name, None)
    
    def __iter__(self):
        sql = f'SELECT name FROM sqlite_schema'
        for row in self.execute(sql):
            table = row[0]
            if table == self.populse_db_table:
                continue
            yield self[table]

def json_dumps(value):
    return json.dumps(value, separators=(',', ':'))

_json_encodings = {
    datetime: lambda d: f'{d.isoformat()}ℹdatetimeℹ',
    date: lambda d: f'{d.isoformat()}ℹdateℹ',
    time: lambda d: f'{d.isoformat()}ℹtimeℹ',
    list: lambda l: [json_encode(i) for i in l],
    dict: lambda d: dict((k, json_encode(v)) for k, v in d.items()),
}

_json_decodings = {
    'datetime': lambda s: dateutil.parser.parse(s),
    'date': lambda s: dateutil.parser.parse(s).date(),
    'time': lambda s: dateutil.parser.parse(s).time(),
}

def json_encode(value):
    global _json_encodings

    type_ = type(value)
    encode = _json_encodings.get(type_)
    if encode is not None:
        return encode(value)
    return encode(value)

def json_decode(value):
    global _json_decodings

    if isinstance(value, list):
        return [json_decode(i) for i in value]
    elif isinstance(value, dict):
        return dict((k, json_decode(v)) for k, v in value.items())
    elif isinstance(value, str):
        if value.endswith('ℹ'):
            l = value[:-1].rsplit('ℹ', 1)
            if len(l) == 2:
                encoded_value, decoding_name = l
                decode = _json_decodings.get(decoding_name)
                if decode is None:
                    raise ValueError(f'Invalid JSON encoding type for value "{value}"')
                return decode(encoded_value)
    return value

class SQLiteCollection:
    _column_encodings = {
        datetime: (
            lambda d: d.isoformat(),
            lambda s: dateutil.parser.parse(s)
        ),
        date: (
            lambda d: d.isoformat(),
            lambda s: dateutil.parser.parse(s).date()
        ),
        time: (
            lambda d: d.isoformat(),
            lambda s: dateutil.parser.parse(s).time()
        ),
        list: (
            lambda l: json_dumps(l),
            lambda l: json.loads(l),
        ),
        dict: (
            lambda d: json_dumps(d),
            lambda d: json.loads(d),
        ),
    }

    def __init__(self, session, name):
        self.session = session
        settings = self.session.get_settings('collection', name, {})
        self.catchall_column = settings.get('catchall_column', '_catchall')
        self.name = name
        self.primary_key = {}
        self.bad_json_fields = set()
        self.fields = {}
        sql = f'pragma table_info({self.name})'
        bad_table = True
        catchall_column_found = False
        for row in self.session.execute(sql):
            bad_table = False
            if row[1] == self.catchall_column:
                catchall_column_found = True
                continue
            column_type_str = row[2].lower()
            column_type = str_to_type(column_type_str)
            main_type = getattr(column_type, '__origin__', None) or column_type
            encoding = self._column_encodings.get(main_type)
            if row[5]:
                self.primary_key[row[1]] = column_type
            field = {
                'name': row[1],
                'primary_key': bool(row[5]),
                'type': column_type,
                'encoding': encoding
            }
            field_settings = settings.get('fields', {}).get(row[1], {})
            field.update(field_settings)
            if field_settings.get('bad_json', False):
                self.bad_json_fields.add(row[1])
            self.fields[row[1]] = field
        if bad_table:
            raise ValueError(f'No such database table: {name}')
        if self.catchall_column and not catchall_column_found:
            raise ValueError(f'table {name} must have a column {self.catchall_column}')

    def settings(self):
        return self.session.get_settings('collection', self.name, {})
    
    def set_settings(self, settings):
        self.session.set_settings('collection', self.name, settings)

    def document_id(self, document_id):
        if isinstance(document_id, str):
            document_id = (document_id,)
        if len(document_id) != len(self.primary_key):
            raise KeyError(f'key for table {self.name} requires {len(self.primary_key)} value(s), {len(document_id)} given')
        return document_id
    
    def update_settings(self, **kwargs):
        settings = self.settings()
        settings.update(kwargs)
        self.set_settings(settings)

    def add_field(self, name, field_type, description=None,
                  index=False, bad_json=False):
        sql = f'ALTER TABLE [{self.name}] ADD COLUMN [{name}] {type_to_str(field_type)}'
        self.session.execute(sql)
        if index:
            sql = 'CREATE INDEX [{self.name}_{name}] ON [{self.name}] ([{name}])'
            self.session.execute(sql)
        settings = self.settings()
        settings.setdefault('fields', {})[name] = {
            'description': description,
            'index': index
        }
        self.set_settings(settings)
        field = {
            'name': name,
            'primary_key': False,
            'type': field_type,
            'description': description,
            'index': index,
            'bad_json': bad_json,
            'encoding': self._column_encodings.get(getattr(field_type, '__origin__', None) or field_type),
        }
        self.fields[name] = field
        if bad_json:
            self.bad_json_fields.add(name)
    
    def remove_field(self, name):
        if name in self.primary_key:
            raise ValueError('Cannot remove a key field')
        sql = f'ALTER TABLE [{self.name}] DROP COLUMN [{name}]'
        self.session.execute(sql)
        settings = self.settings()
        settings.setdefault('fields', {}).pop(name, None)
        self.set_settings(settings)
        self.fields.pop(name, None)
        self.bad_json_fields.discard(name)

    def field(self,name):
        return self.field[name]
    
    def fields(self):
        return self.fields.values()

    def update_document(self, document_id, partial_document):
        document_id = self.document_id(document_id)
        #TODO can be optimized
        document = self[document_id]
        document.update(partial_document)
        document[document_id] = document
    
    def has_document(self, document_id):
        document_id = self.document_id(document_id)
        sql = f'SELECT count(*) FROM [{self.name}] WHERE {" AND ".join(f"[{i}] = ?" for i in self.primary_key)}'
        return next(self.session.execute(sql, document_id))[0] != 0

    
    def _documents(self, fields, as_list, where, where_data):
        if fields:
            columns = []
            catchall_fields = set()
            for field in fields:
                if field in self.fields:
                    columns.append(field)
                else:
                    catchall_fields.add(field)
        else:
            columns = list(self.fields)
            catchall_fields = bool(self.catchall_column)        
        if catchall_fields:
            if not self.catchall_column and isinstance(catchall_fields, set):
                raise ValueError(f'Collection {self.name} do not have the following fields: {",".join(catchall_fields)}')
            columns.append(self.catchall_column)

        sql = f'SELECT {",".join(f"[{i}]" for i in columns)} FROM [{self.name}]'
        if where:
            sql += f' WHERE {where}'
        cur = self.session.execute(sql, where_data)
        for row in cur:
            if catchall_fields:
                if as_list and catchall_fields is True:
                    raise ValueError(f'as_list=True cannot be used on {self.name} without a fields list because two documents can have different fields')
                catchall = json.loads(row[-1])
                if isinstance(catchall_fields, set):
                    catchall = dict((i, catchall[i]) for i in catchall_fields)
                row = row[:-1]
                columns = columns[:-1]
            else:
                catchall = {}        

            document = catchall
            document.update(zip(columns, row))
            for field, value in document.items():
                encoding = self.fields.get(field,{}).get('encoding')
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
      
    def document(self, document_id, fields, as_list):
        document_id = self.document_id(document_id)
        where = f'{" AND ".join(f"[{i}] = ?" for i in self.primary_key)}'
        return next(self._documents(fields, as_list, where, document_id))

    def documents(self, fields, as_list):
        yield from self._documents(None, None, fields, as_list)
    
    def add(self, document):
        document_id = tuple(document.get(i) for i in self.primary_key)
        self._set_document(document_id, document, replace=False)

    def __setitem__(self, document_id, document):
        document_id = self.document_id(document_id)
        self._set_document(document_id, document, replace=True)

    def _encode_column_value(self, field, value):
        encoding  = self.fields.get(field,{}).get('encoding')
        if encoding:
            encode, decode = encoding
            try:
                column_value = encode(value)
            except TypeError:
                # Error with JSON encoding
                column_value = ...
            if column_value is ...:
                column_value = encode(json_encode(value))
                self.bad_json_fields.add(field)
                settings = self.settings()
                settings.setdefault('fields', {}).setdefault(field,{})['bad_json'] = True
                self.set_settings(settings)
            return column_value
        return value

    def _set_document(self, document_id, document, replace):
        columns = [i for i in self.primary_key]
        data = [i for i in document_id]
        catchall = {}
        for field, value in document.items():
            if field in self.primary_key:
                continue
            if field in self.bad_json_fields:
                value = json_encode(value)
            if field in self.fields:
                columns.append(field)
                data.append(self._encode_column_value(column_value))
            else:
                catchall[field] = value
        if catchall:
            if not self.catchall_column:
                raise ValueError(f'Collection {self.name} cannot store the following unknown fields: {", ".join(catchall)}')
            bad_json = False
            try:
                data.append(json_dumps(catchall))
            except TypeError:
                bad_json = True
            if bad_json:
                jsons = []
                for field, value in catchall.items():
                    bad_json = False
                    try:
                        j = json_dumps(value)
                    except TypeError:
                        bad_json = True
                    if bad_json:
                        t = type(value)
                        self.add_field(field, t,
                            bad_json=t not in (time, date, datetime))
                        column_value = self._encode_column_value(field, value)
                        columns.append(field)
                        data.append(column_value)
                    else:
                        jsons.append((f'"{field}"', j))
                if jsons:
                    columns.append(self.catchall_column)
                    data.append(f'{{{",".join(f"{i}:{j}" for i, j in jsons)}}}')

            else:
                columns.append(self.catchall_column)

        if replace:
            replace = ' OR REPLACE'
        else:
            replace = ''
        sql = f'INSERT{replace} INTO [{self.name}] ({",".join(f"[{i}]" for i in columns)}) values ({",".join("?" for i in data)})'
        self.session.execute(sql, data)

    def __getitem__(self, document_id):
        return self.document(document_id)
    
    def __delitem__(self, document_id):
        document_id = self.document_id(document_id)
        sql = f'DELETE FROM [{self.name}] WHERE {" AND ".join(f"[{i}] = ?" for i in self.primary_key)}'
        self.session.execute(sql, document_id)
    
    def parse_filter(self, filter):
        if filter is None or isinstance(filter, ParsedFilter):
            return filter
        tree = filter_parser().parse(filter)
        where_filter = FilterToSQL(self).transform(tree)
        if where_filter is None:
            return None
        else:
            return ParsedFilter(' '.join(where_filter))
    

    def filter(self, filter, fields=None, as_list=False):
        parsed_filter = self.parse_filter(filter)
        yield from self._select_documents(parsed_filter, None, fields=fields, as_list=as_list)
