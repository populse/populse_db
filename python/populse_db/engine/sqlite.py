from datetime import time, date, datetime
from turtle import update
from xml.dom.minidom import Document
import dateutil
import json
import sqlite3

from ..database import str_to_type, type_to_str
from ..filter import FilterToSQL, filter_parser

'''
SQLite3 implementation of populse_db engine.

A populse_db engine is created when a DatabaseSession object is created 
(typically within a "with" statement)
'''

class ParsedFilter(str):
    pass

class SQLiteSession:
    populse_db_table = 'populse_db'

    def parse_url(self, url):
        if url.path:
            args = (url.path,)
        elif url.netloc:
            args = (url.netloc,)
        return args, {}
    
    def __init__(self, sqlite_file):
        self.sqlite = sqlite3.connect(sqlite_file)
        self._collection_cache = {}

    def __getitem__(self, collection_name):
        return SQLiteCollection(self, collection_name)
    
    def execute(self, *args, **kwargs):
        return self.sqlite.execute(*args, **kwargs)

    def get_settings(self, category, key, default=None):
        try:
            sql = f'SELECT _json FROM [{self.populsedb_table}] WHERE category=? and key=?'
            cur = self.execute(sql, [category, key])
        except sqlite3.OperationalError:
            return default
        if cur.rowcount:
            return json.loads(next(cur)[0])
        return default

    def set_settings(self, category, key, value):
        sql = f'INSERT OR REPLACE INTO {self.populsedb_table} (category, key, _json) VALUES (?,?,?)'
        data = [category, key, json.dumps(value)]
        retry = False
        try:
            self.execute(sql, data)
        except sqlite3.OperationalError:
            retry = True
        if retry:
            sql2 = ('CREATE TABLE IF NOT EXISTS '
                f'[{self.jsondb_table}] ('
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
    
    def add_collection(self, name, primary_key):
        if isinstance(primary_key, str):
            primary_key = {primary_key: str}
        elif isinstance(primary_key, (list, tuple)):
            primary_key = dict((i,str) for i in primary_key)
        sql = (f'CREATE TABLE [{name}] ('
            f'{",".join(f"[{n}] {type_to_str(t)} NOT NULL" for n, t in primary_key.items())},'
            f'{self.catchall_column} dict,'
            f'PRIMARY KEY ({",".join(primary_key.keys())}))')
        print('!sql!', sql)
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
    datetime: lambda d: f'{d.isoformat()}ℹdatetime',
    date: lambda d: f'{d.isoformat()}ℹdate',
    time: lambda d: f'{d.isoformat()}ℹtime',
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
        if value.startswith('ℹ'):
            l = value[1:].rsplit('ℹ', 1)
            if len(l) == 2:
                encoded_value, decoding_name = l
                decode = _json_decodings.get(decoding_name)
                if decode is None:
                    raise ValueError(f'Invalid JSON encoding type for value "{value}"')
                return decode(encoded_value)
    return value

class SQLiteCollection:
    _column_encodings = {
        'datetime': (
            lambda d: d.isoformat(),
            lambda s: dateutil.parser.parse(s)
        ),
        'date': (
            lambda d: d.isoformat(),
            lambda s: dateutil.parser.parse(s).date()
        ),
        'time': (
            lambda d: d.isoformat(),
            lambda s: dateutil.parser.parse(s).time()
        ),
        'list': (
            lambda l: json_dumps(l),
            lambda l: json.loads(l),
        ),
        'dict': (
            lambda d: json_dumps(d),
            lambda d: json.loads(d),
        ),
    }

    def __new__(cls, session, name):
        result = session._collection_cache.get(name)
        if result is not None:
            return result
        return super().__new__(cls)

    def __init__(self, session, name):
        self.session = session
        settings = self.session.get_settings('session', name, {})
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
            main_type = column_type_str.split('[',1)[0]
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
        return self.session.get_settings('collection', self.name)
    
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

    def add_field(self, name, field_type, description,
                  index):
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
            'index': index
        }
        self.fields[name] = field
    
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

        sql = f'SELECT {",".join(f"[{i}]" for i in self.columns)} FROM [{self.name}]'
        if where:
            sql += f' WHERE {where}'
        cur = self.sqlite.execute(sql, where, where_data)
        if cur.rowcount == 0:
            raise ValueError(f'No such collection: {self.name}')
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
                if field in self.bad_json:
                    value = json_decode(value)
                encoding = self.fields.get(field,{}).get('encoding')
                if encoding:
                    encode, decode = encoding
                    value = decode(value)
                document[field] = value
            if as_list:
                yield [document[i] for i in fields]
            else:
                yield document
      
    def document(self, document_id, fields, as_list):
        document_id = self.document_id(document_id)
        where += f' WHERE {" AND ".join(f"[{i}] = ?" for i in self.primary_key)}'
        return next(self._documents(where, tuple(self.primary_key), fields, as_list))

    def documents(self, fields, as_list):
        yield from self._documents(None, None, fields, as_list)
    
    def add(self, document):
        document_id = tuple(document.get(i) for i in self.primary_key)
        self._set_document(document_id, document, replace=False)

    def __setitem__(self, document_id, document):
        document_id = self.document_id(document_id)
        self._set_document(document_id, document, replace=True)

    def _set_document(self, document_id, document, replace):
        columns = [i for i in self.primary_key]
        data = [i for i in document_id]
        catchall = {}
        for field, value in document.items():
            if field in self.primary_key:
                continue
            if field in self.bad_json_field:
                value = json_encode(value)
            field_dict = self.fields.get(field)
            if field_dict:
                columns.append(field)
                encoding  = field_dict.get('encoding')
                if encoding:
                    encode, decode = encoding
                    try:
                        column_value = encode(value)
                    except TypeError:
                        # Error with JSON encoding
                        column_value = ...
                    if column_value is ...:
                        column_value = encode(json_encode(value))
                        self.bad_json_field.add(field)
                        settings = self.settings()
                        settings.setdefault('fields', {}).setdefault(field,{})['bad_json'] = True
                        self.set_settings(settings)
                    data.append(column_value)
                else:
                    data.append(value)
            else:
                catchall[field] = value
        if catchall:
            if not self.catchall_column:
                raise ValueError(f'Collection {self.name} cannot store the following unknown fields: {", ".join(catchall)}')
            columns.append(self.catchall_column)
            data.append(json.dumps(catchall))

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
        yield from self._select_documents(where, None, fields=fields, as_list=as_list)


# class FilterToSqliteQuery(FilterToQuery):
#     '''
#     Implements required methods to produce a SQLite query given a document
#     selection filter. This class returns either None (all documents are 
#     selected) or an SQL WHERE clause (without the WHERE keyword) as a list 
#     of string (that must be joined with spaces). This WHERE clause is useable 
#     with a SELECT from the table containing the collection documents. Using a 
#     list for combining strings is supposed to be more efficient (especially 
#     for long queries).
#     '''
    
#     def __init__(self, engine, collection):
#         '''
#         Create a parser for a givent engine and collection
#         '''
#         FilterToQuery.__init__(self, engine, collection)
#         self.table = self.engine.collection_table[collection]

#     def get_column(self, field):
#         '''
#         :return: The SQL representation of a field object.
#         '''
#         return self.engine.field_column[self.collection][field.field_name]

#     _python_to_sql = {
#         pdb.FIELD_TYPE_DATE: lambda x: x.isoformat(),
#         pdb.FIELD_TYPE_DATETIME: lambda x: x.isoformat(),
#         pdb.FIELD_TYPE_TIME: lambda x: x.isoformat(),
#         pdb.FIELD_TYPE_BOOLEAN: lambda x: (1 if x else 0),
#     }
#     _python_to_sql = {
#         type(None): lambda x: 'NULL',
#         type(''): lambda x: "'{0}'".format(x),
#         type(u''): lambda x: "'{0}'".format(x),
#         int: lambda x: str(x),
#         float: lambda x: str(x),
#         datetime.time: lambda x: "'{0}'".format(x.isoformat()),
#         datetime.datetime: lambda x: "'{0}'".format(x.isoformat()),
#         datetime.date: lambda x: "'{0}'".format(x.isoformat()),
#         bool: lambda x: ('1' if x else '0'),
#     }

#     def get_column_value(self, python_value):
#         '''
#         Converts a Python value to a value suitable to put in a database column
#         '''
#         if isinstance(python_value, list):
#             c = '(%s)' % ','.join(self.get_column_value(i) for i in python_value)
#             return c
#         return self._python_to_sql[type(python_value)](python_value)

#     def build_condition_all(self):
#         return None

#     def build_condition_literal_in_list_field(self, value, list_field):
#         cvalue = self.get_column_value(value)
#         list_column = self.get_column(list_field)
#         list_table = 'list_%s_%s' % (self.table, list_column)
#         primary_key_column = self.engine.primary_key(self.collection)
#         pk_column = self.engine.field_column[
#             self.collection][primary_key_column]

#         where = ('[{0}] IS NOT NULL AND '
#                  '{1} IN (SELECT value FROM {2} '
#                  'WHERE list_id = [{3}])').format(list_column,
#                                                   cvalue,
#                                                   list_table,
#                                                   pk_column)
#         return [where]

#     def build_condition_field_in_list_field(self, field, list_field):
#         column = self.get_column(field)
#         list_column = self.get_column(list_field)
#         list_table = 'list_%s_%s' % (self.table, list_column)
#         primary_key_column = self.engine.primary_key(self.collection)
#         pk_column = self.engine.field_column[self.collection][primary_key_column]

#         where = ('[{0}] IS NOT NULL AND '
#                  '[{1}] IN (SELECT value FROM {2} '
#                  'WHERE list_id = [{3}])').format(list_column,
#                                                   column,
#                                                   list_table,
#                                                   pk_column)
#         return [where]

#     def build_condition_field_in_list(self, field, list_value):
#         column = self.get_column(field)
#         if None in list_value:
#             list_value.remove(None)
#             where = '[{0}] IS NULL OR [{0}] IN {1}'.format(column,
#                 self.get_column_value(list_value))
#         else:
#             where = '[{0}] IN {1}'.format(column,
#                 self.get_column_value(list_value))
#         return [where]

#     sql_operators = {
#         '==': 'IS',
#         '!=': 'IS NOT',
#         'ilike': 'LIKE',
#     }    
    
#     no_list_operator = {'>', '<', '>=', '<=', 'like', 'ilike'}
    
#     def build_condition_field_op_field(self, left_field, operator_str, right_field):
#         if operator_str == 'ilike':
#             field_pattern = 'UPPER([%s])'
#         else:
#             field_pattern = '[%s]'
#         sql_operator = self.sql_operators.get(operator_str, operator_str)
#         where = '%s %s %s' % (field_pattern % self.get_column(left_field),
#                               sql_operator,
#                               field_pattern % self.get_column(right_field))
#         return [where]
    
#     def build_condition_field_op_value(self, field, operator_str, value):
#         if isinstance(value, list):
#             if operator_str in self.no_list_operator:
#                 raise ValueError('operator %s cannot be used with value of list type' % operator_str)
#             value = self.engine.list_hash(value)
#         if operator_str == 'ilike':
#             field_pattern = 'UPPER([%s])'
#             if isinstance(value, six.string_types):
#                 value = value.upper()
#         else:
#             field_pattern = '[%s]'
#         sql_operator = self.sql_operators.get(operator_str, operator_str)
#         where = '%s %s %s' % (field_pattern % self.get_column(field),
#                               sql_operator,
#                               self.get_column_value(value))
#         return [where]

    
#     def build_condition_value_op_field(self, value, operator_str, field):
#         if isinstance(value, list):
#             if operator_str in self.no_list_operator:
#                 raise ValueError('operator %s cannot be used with value of list type' % operator_str)
#             value = self.list_hash(value)
#         if operator_str == 'ilike':
#             field_pattern = 'UPPER([%s])'
#             if isinstance(value, six.string_types):
#                 value = value.upper()
#         else:
#             field_pattern = '[%s]'
#         sql_operator = self.sql_operators.get(operator_str, operator_str)
#         where = '%s %s %s' % (self.get_column_value(value),
#                               sql_operator,
#                               field_pattern % self.get_column(field))
#         return [where]

#     def build_condition_negation(self, condition):
#         if condition is None:
#             return ['0']
#         return ['NOT', '(' ] + condition + [')']
    
#     def build_condition_combine_conditions(self, left_condition, operator_str, right_condition):
#         return ['('] + left_condition + [')', operator_str, '('] + right_condition + [')']
