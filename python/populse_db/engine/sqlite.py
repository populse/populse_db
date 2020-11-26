import datetime
import hashlib
import json
import os.path as osp
import re
import six
import sqlite3
import threading
import uuid

import populse_db.database as pdb
from populse_db.engine import Engine
from populse_db.filter import FilterToQuery, filter_parser

import dateutil

'''
SQLite3 implementation of populse_db engine.

A populse_db engine is created when a DatabaseSession object is created 
(typically within a "with" statement)
'''

# Table names
FIELD_TABLE = '_field'
COLLECTION_TABLE = '_collection'
                
class SQLiteEngine(Engine):
    
    _global_lock = threading.RLock()
    _database_locks = {}
    
    def __init__(self, database):
        self._enter_recursion_count = 0
        with self._global_lock:
            self.connection = sqlite3.connect(database, 
                                            isolation_level=None,
                                            check_same_thread=False)
            self.cursor = None
            if database == ':memory:':
                self._global_lock_id = None
                self.lock = threading.RLock()
            else:
                self._global_lock_id = osp.normpath(osp.realpath(osp.abspath(database)))
                self.lock, lock_count = self._database_locks.get(self._global_lock_id) or (None, None)
                if self.lock is None:
                    self.lock = threading.RLock()
                    self._database_locks[self._global_lock_id] = (self.lock, 1)
                else:
                    self._database_locks[self._global_lock_id] = (self.lock, lock_count + 1)

    def __del__(self):
        if self._global_lock_id:
            with self._global_lock:
                self.lock, lock_count = self._database_locks.get(self._global_lock_id)
                lock_count -= 1
                if lock_count:
                    self._database_locks[self._global_lock_id] = (self.lock, lock_count)
                else:
                    del self._database_locks[self._global_lock_id]
    
    def name_to_sql(self, name):
        """
        Transforms the name into a valid and unique SQLite table/column name.
        Since all names are quoted in SQL with '[]', there is no restriction
        on character that can be used. However, case is insignificant in
        SQLite. Therefore, all upper case characters are prefixed with '!'.

        :param name: Name (str)

        :return: Valid and unique table/column name
        """
        return re.sub('([A-Z])', r'!\1', name)

    def __enter__(self):
        if self._enter_recursion_count == 0:
            self.lock.acquire()
            self.cursor = self.connection.cursor()
            self.cursor.execute('PRAGMA synchronous=OFF')
            self.cursor.execute('PRAGMA case_sensitive_like=ON')
            self.cursor.execute('PRAGMA foreign_keys=ON')
            self.cursor.execute('BEGIN DEFERRED')
            
            if not self.has_table(COLLECTION_TABLE):
                sql = '''CREATE TABLE [{0}] (
                    collection_name TEXT,
                    field_name TEXT,
                    field_type TEXT CHECK(field_type IN ({1})) NOT NULL,
                    description TEXT,
                    has_index BOOLEAN NOT NULL,
                    column TEXT NOT NULL,
                    PRIMARY KEY (field_name, collection_name))
                '''.format(FIELD_TABLE,
                        ','.join("'%s'" % i for i in (pdb.FIELD_TYPE_STRING,
                                                    pdb.FIELD_TYPE_INTEGER, 
                                                    pdb.FIELD_TYPE_FLOAT, 
                                                    pdb.FIELD_TYPE_BOOLEAN,
                                                    pdb.FIELD_TYPE_DATE,
                                                    pdb.FIELD_TYPE_DATETIME,
                                                    pdb.FIELD_TYPE_TIME,
                                                    pdb.FIELD_TYPE_JSON,
                                                    pdb.FIELD_TYPE_LIST_STRING,
                                                    pdb.FIELD_TYPE_LIST_INTEGER,
                                                    pdb.FIELD_TYPE_LIST_FLOAT,
                                                    pdb.FIELD_TYPE_LIST_BOOLEAN,
                                                    pdb.FIELD_TYPE_LIST_DATE,
                                                    pdb.FIELD_TYPE_LIST_DATETIME,
                                                    pdb.FIELD_TYPE_LIST_TIME,
                                                    pdb.FIELD_TYPE_LIST_JSON)))
                self.cursor.execute(sql)
                
                sql = '''CREATE TABLE [{0}] (
                    collection_name TEXT PRIMARY KEY,
                    primary_key TEXT NOT NULL,
                    table_name TEXT NOT NULL)
                '''.format(COLLECTION_TABLE)
                self.cursor.execute(sql)
            
            
            self.collection_primary_key = {}
            self.collection_table = {}
            self.table_row = {}
            self.table_document = {}
            for table in (COLLECTION_TABLE, FIELD_TABLE):
                sql = 'PRAGMA table_info([%s])' % table
                self.cursor.execute(sql)
                columns = [i[1] for i in self.cursor]
                self.table_row[table] = pdb.list_with_keys(table, columns)
            sql = 'SELECT collection_name, table_name, primary_key FROM [%s]' % COLLECTION_TABLE
            self.cursor.execute(sql)
            for i in self.cursor.fetchall():
                collection, table, primary_key = i
                self.collection_primary_key[collection] = primary_key
                self.collection_table[collection] = table
                sql = "SELECT field_name, column FROM [%s] WHERE collection_name = '%s'" % (FIELD_TABLE, collection)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                fields = [i[0] for i in rows]
                columns = [i[1] for i in rows]
                self.table_row[table] = pdb.list_with_keys(table, columns)
                self.table_document[table] = pdb.list_with_keys(table, fields)
            
            sql = 'SELECT collection_name, field_name, field_type, column FROM [%s]' % FIELD_TABLE
            self.cursor.execute(sql)
            self.field_column = {}
            self.field_type = {}
            for collection, field, field_type, column in self.cursor:
                self.field_column.setdefault(collection, {})[field] = column
                self.field_type.setdefault(collection, {})[field] = field_type
        
        self._enter_recursion_count += 1
        return self
    
    def commit(self):
        try:
            self.cursor.execute('COMMIT')
        except sqlite3.OperationalError as e:
            if 'no transaction is active' not in str(e):
                raise
    
    def rollback(self):
        self.cursor.execute('ROLLBACK')
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter_recursion_count -= 1
        if self._enter_recursion_count == 0:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
            self.cursor = None
            self.lock.release()
    
    type_to_sql = {
        pdb.FIELD_TYPE_INTEGER: 'INT',
        pdb.FIELD_TYPE_FLOAT: 'REAL',
        pdb.FIELD_TYPE_BOOLEAN: 'BOOLEAN',
        pdb.FIELD_TYPE_DATE: 'TEXT',
        pdb.FIELD_TYPE_DATETIME: 'TEXT',
        pdb.FIELD_TYPE_TIME: 'TEXT',
        pdb.FIELD_TYPE_STRING: 'TEXT',
        pdb.FIELD_TYPE_JSON: 'STRING',
    }
    
    def sql_type(self, type):
        if type.startswith('list_'):
            return 'TEXT'
        else:
            return self.type_to_sql[type]
    
    @staticmethod
    def list_hash(list):
        if list is None:
            return None
        if not list:
            return ''
        m = hashlib.md5()
        for i in list:
            m.update(six.text_type(i).encode('utf8'))
        hash = m.hexdigest()
        return hash
    
    def clear(self):
        tables = [FIELD_TABLE, COLLECTION_TABLE]
        tables += [i.table_name for i in self.collections()]
        tables += ['list_%s_%s' % (self.collection_table[i.collection_name],
                                   self.field_column[i.collection_name][i.field_name])
                   for i in self.fields() if i.field_type.startswith('list_')]
        for table in tables:
            sql = 'DROP TABLE [%s]' % table
            self.cursor.execute(sql)
        self.collection_table = {}
        self.table_row = {}
        self.table_document = {}
        self.field_column = {}
        self.field_type = {}
        
    def has_table(self, table):
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'" % table)
        return self.cursor.fetchone()[0] == 1
    
    def has_collection(self, collection):
        return collection in self.collection_table

    def add_collection(self, collection, primary_key):
        table_name = self.name_to_sql(collection)
        pk_column = self.name_to_sql(primary_key)
        sql = 'CREATE TABLE [%s] ([%s] TEXT PRIMARY KEY)' % (table_name, pk_column)
        try:
            self.cursor.execute(sql)
        except sqlite3.OperationalError as e:
            raise ValueError(str(e))
        self.table_row[table_name] = pdb.list_with_keys(table_name, [pk_column])
        self.table_document[table_name] = pdb.list_with_keys(table_name, [primary_key])
        
        sql = 'INSERT INTO [%s] (collection_name, primary_key, table_name) VALUES (?, ?, ?)' % COLLECTION_TABLE
        self.cursor.execute(sql, [collection, primary_key, table_name])
        self.collection_table[collection] = table_name
        self.collection_primary_key[collection] = primary_key
        
        sql = 'INSERT INTO [%s] (field_name, collection_name, field_type, description, has_index, column) VALUES (?, ?, ?, ?, 1, ?)' % FIELD_TABLE
        self.cursor.execute(sql, [primary_key, collection, pdb.FIELD_TYPE_STRING,
                                  'Primary_key of the document collection %s' % collection,
                                  pk_column])
        self.field_column[collection] = {primary_key: pk_column}
        self.field_type[collection] = {primary_key: pdb.FIELD_TYPE_STRING}
        
    def collection(self, collection):
        row_class = self.table_row[COLLECTION_TABLE]
        sql = 'SELECT %s FROM [%s] WHERE collection_name = ?' % (
            ','.join('[%s]' % i for i in row_class._key_indices),
            COLLECTION_TABLE)
        self.cursor.execute(sql, [collection])
        row = self.cursor.fetchone()
        if row is not None:
            return row_class(*row)
        return None
    
    def primary_key(self, collection):
        return self.collection_primary_key[collection]
    
    def remove_collection(self, collection):
        table = self.collection_table[collection]
        
        sql = 'DELETE FROM [%s] WHERE collection_name = ?' % FIELD_TABLE
        self.cursor.execute(sql, [collection])

        sql = 'DELETE FROM [%s] WHERE collection_name = ?' % COLLECTION_TABLE
        self.cursor.execute(sql, [collection])
        del self.collection_table[collection]
        del self.collection_primary_key[collection]
        del self.table_row[table]
        del self.table_document[table]
        del self.field_column[collection]
        del self.field_type[collection]
        
        sql = 'DROP TABLE [%s]' % table
        self.cursor.execute(sql)

    def collections(self):
        row_class = self.table_row[COLLECTION_TABLE]
        sql = 'SELECT %s FROM [%s]' % (
            ','.join('[%s]' % i for i in row_class._key_indices),
            COLLECTION_TABLE)
        return [self.table_row[COLLECTION_TABLE](*i) for i in self.cursor.execute(sql)]

    def add_field(self, collection, field, type, description, index):
        table = self.collection_table[collection]
        column = self.name_to_sql(field)
        sql = 'ALTER TABLE [%s] ADD COLUMN [%s] %s' % (table, column, self.sql_type(type))
        table_row = self.table_row[table]
        table_row._append_key(column)
        table_doc = self.table_document[table]
        table_doc._append_key(field)
        
        self.cursor.execute(sql)
        sql = 'INSERT INTO [%s] (field_name, collection_name, field_type, description, has_index, column) VALUES (?, ?, ?, ?, ?, ?)' % FIELD_TABLE
        self.cursor.execute(sql, [field,
                                  collection,
                                  type,
                                  description,
                                  (1 if index else 0),
                                  column])
        self.field_column.setdefault(collection, {})[field] = column
        self.field_type.setdefault(collection, {})[field] = type
        if index:
            sql = 'CREATE INDEX [{0}_{1}] ON [{0}] ([{1}])'.format(table, column)
            self.cursor.execute(sql)
        if type.startswith('list_'):
            sql = 'CREATE TABLE [list_{0}_{1}] (list_id TEXT NOT NULL, i INT, value {2})'.format(table, column, self.sql_type(type[5:]))
            self.cursor.execute(sql)
            sql = 'CREATE INDEX [list_{0}_{1}_id] ON [list_{0}_{1}] (list_id)'.format(table, column)
            self.cursor.execute(sql)
            sql = 'CREATE INDEX [list_{0}_{1}_i] ON [list_{0}_{1}] (i ASC)'.format(table, column)
            self.cursor.execute(sql)
    
    def add_document(self, collection, document, create_missing_fields):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        document_id = document[primary_key]
        lists = []
        column_values = {}
        for field, value in document.items():
            field_type = self.field_type[collection].get(field)
            if field_type is None:
                if not create_missing_fields:
                    raise ValueError('Collection {0} has no field {1}'
                                    .format(collection, field))
                try:
                    field_type = pdb.python_value_type(value)
                    if field_type is None:
                        raise KeyError
                except KeyError:
                    raise ValueError('Collection {0} has no field {1} and it '
                                    'cannot be created from a value of type {2}'
                                    .format(collection,
                                            field,
                                            type(value)))
                self.add_field(collection, field, field_type,
                            description=None, index=False)
            column = self.field_column[collection][field]
            if isinstance(value, list):
                list_table = 'list_%s_%s' % (table, column)
                list_id = document_id
                column_values[column] = self.list_hash(value)
                sql = 'INSERT INTO [%s] (list_id, i, value) VALUES (?, ?, ?)' % list_table
                sql_params = [
                    [list_id,
                     i,
                     self.python_to_column(field_type[5:], value[i])]
                     for i in range(len(value))]
                lists.append((sql, sql_params))
            else:
                column_values[column] = self.python_to_column(field_type, value)
        
        sql = 'INSERT INTO [%s] (%s) VALUES (%s)' % (
            table,
            ','.join('[%s]' % i for i in column_values.keys()),
            ','.join('?' for i in column_values))
        try:
            self.cursor.execute(sql, list(column_values.values()))
        except sqlite3.IntegrityError as e:
            raise ValueError(str(e))
        for sql, sql_params in lists:
            self.cursor.executemany(sql, sql_params)
            
    def has_field(self, collection, field):
        return self.field_column.get(collection, {}).get(field) is not None
    
    def field(self, collection, field):
        row_class = self.table_row[FIELD_TABLE]
        sql = 'SELECT %s FROM [%s] WHERE collection_name = ? AND field_name = ?' % (
            ','.join('[%s]' % i for i in row_class._key_indices),
            FIELD_TABLE)
        self.cursor.execute(sql, [collection, field])
        row = self.cursor.fetchone()
        if row is not None:
            return row_class(*row)
        return None

    def fields(self, collection=None):
        row_class = self.table_row[FIELD_TABLE]
        sql = 'SELECT %s FROM [%s]' % (
            ','.join('[%s]' % i for i in row_class._key_indices),
            FIELD_TABLE)
        if collection is None:
            data = []
        else:
            sql += ' WHERE collection_name = ?'
            data = [collection]
        self.cursor.execute(sql, data)
        for row in self.cursor.fetchall():
            yield row_class(*row)
    
    def remove_fields(self, collection, fields):
        table = self.collection_table[collection]
        exclude_fields = set(fields)
        new_columns = []
        indices = []
        for field in self.fields(collection):
            if field.field_name not in exclude_fields:
                new_columns.append((field.column,
                                    self.sql_type(field.field_type)))
                if field.has_index:
                    indices.append(field.column)
            else:
                self.table_row[table]._delete_key(field.column)
                self.table_document[table]._delete_key(field.field_name)
                del self.field_column[collection][field.field_name]
                sql = 'DELETE FROM [%s] WHERE collection_name = ? AND field_name = ?' % FIELD_TABLE
                self.cursor.execute(sql, [collection, field.field_name])
                if field.field_type.startswith('list_'):
                    column = field.column
                    list_table = 'list_%s_%s' % (table, column)
                    sql = 'DROP TABLE [%s]' % list_table
                    self.cursor.execute(sql)
        tmp_table = '_' + str(uuid.uuid4())
        sql = 'CREATE TABLE [%s] (%s)' % (tmp_table,
                                          ','.join('[%s] %s' % (i, j) for i, j in new_columns))
        self.cursor.execute(sql)
        for column in indices:
            sql = 'CREATE INDEX [{0}_{1}] ON [{0}] ([{1}])'.format(tmp_table, column)
            self.cursor.execute(sql)
        self.cursor.execute('PRAGMA table_info([%s])' % table)
        sql = 'INSERT INTO [%s] SELECT %s FROM [%s]' % (
            tmp_table,
            ','.join('[%s]' % i[0] for i in new_columns),
            table)
        self.cursor.execute(sql)
        sql = 'PRAGMA foreign_keys=OFF'
        self.cursor.execute(sql)
        sql = 'DROP TABLE [%s]' % table
        self.cursor.execute(sql)
        sql = 'ALTER TABLE [%s] RENAME TO [%s]' % (tmp_table, table)
        self.cursor.execute(sql)
        sql = 'PRAGMA foreign_keys=ON'
        self.cursor.execute(sql)
    
    # Some types (e.g. time, date and datetime) cannot be
    # serialized/deserialized into string with repr/ast.literal_eval.
    # This is a problem for storing the corresponding list_columns in
    # database. For the list types with this problem, we record in the
    # following dictionaries the functions that must be used to serialize
    # (in _list_item_to_string) and deserialize (in _string_to_list_item)
    # the list items.
    _python_to_sql_data = {
        pdb.FIELD_TYPE_DATE: lambda x: x.isoformat() if x is not None else x,
        pdb.FIELD_TYPE_DATETIME:
            lambda x: x.isoformat() if x is not None else x,
        pdb.FIELD_TYPE_TIME: lambda x: x.isoformat() if x is not None else x,
        pdb.FIELD_TYPE_BOOLEAN: lambda x: (1 if x else 0),
    }

    _sql_to_python = {
        pdb.FIELD_TYPE_DATE: lambda x: dateutil.parser.parse(x).date(),
        pdb.FIELD_TYPE_DATETIME: lambda x: dateutil.parser.parse(x),
        pdb.FIELD_TYPE_TIME: lambda x: dateutil.parser.parse(x).time(),
        pdb.FIELD_TYPE_BOOLEAN: lambda x: bool(x),
        pdb.FIELD_TYPE_JSON: lambda x: json.loads(x),
    }

    @staticmethod
    def python_to_column(field_type, value):
        """
        Converts a python value into a suitable value to put in a
        database column.
        """
        converter = SQLiteEngine._python_to_sql_data.get(field_type)
        if converter is not None:
            return converter(value)
        elif isinstance(value, dict):
            return json.dumps(value)
        else:
            return value

    @staticmethod
    def column_to_python(field_type, value):
        if value is None:
            return None
        converter = SQLiteEngine._sql_to_python.get(field_type)
        if converter is not None:
            return converter(value)
        else:
            return value

    def has_document(self, collection, document_id):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        pk_column = self.field_column[collection][primary_key]
        sql = 'SELECT COUNT(*) FROM [%s] WHERE [%s] = ?' % (table, pk_column)
        self.cursor.execute(sql, [document_id])
        r = self.cursor.fetchone()
        return bool(r[0])


    def _select_documents(self, collection, where, where_data,
                          fields=None, as_list=False):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        pk_column = self.field_column[collection][primary_key]
        row_class = self.table_row[table]
        if fields:
            selected_fields = fields
            columns = [self.field_column[collection][i] for i in fields]
        else:
            selected_fields = list(self.table_document[table].keys())
            columns = list(row_class._key_indices)
        sql = 'SELECT %s FROM [%s]' % (
            ','.join('[%s]' % i for i in [pk_column] + columns),
            table)
        if where:
            sql += ' WHERE %s' % where
        self.cursor.execute(sql, where_data)
        for row in self.cursor.fetchall():
            document_id = row[0]
            values = []
            for field, sql_value in zip(selected_fields, row[1:]):
                field_type = self.field_type[collection][field]
                if field_type.startswith('list_'):
                    item_type = field_type[5:]
                    column = self.field_column[collection][field]
                    list_hash = sql_value
                    if list_hash is None:
                        values.append(None)
                    else:
                        sql = 'SELECT value FROM [list_{0}_{1}] WHERE list_id = ? ORDER BY i'.format(table, column)
                        self.cursor.execute(sql, [document_id])
                        values.append([self.column_to_python(item_type,i[0]) for i in self.cursor])
                else:
                    values.append(self.column_to_python(field_type, sql_value))
            if as_list:
                yield values
            else:
                if fields:
                    result = pdb.DictList(selected_fields, values)
                else:
                    result = self.table_document[table](*values)
                yield result

    def document(self, collection, document_id,
                 fields=None, as_list=False):
        primary_key = self.collection_primary_key[collection]
        pk_column = self.field_column[collection][primary_key]
        where = '[%s] = ?' % pk_column
        where_data = [document_id]
        
        try:
            return next(self._select_documents(collection, where, where_data,
                                               fields=fields, as_list=as_list))
        except StopIteration:
            return None
    
    def has_value(self, collection, document_id, field):
        table = self.collection_table.get(collection)
        if table is not None:
            primary_key = self.collection_primary_key[collection]
            pk_column = self.field_column[collection][primary_key]
            column = self.field_column[collection].get(field)
            if column is not None:
                sql = 'SELECT [%s] FROM [%s] WHERE [%s] = ?' % (column, table,
                                                                pk_column)
                self.cursor.execute(sql, [document_id])
                row = self.cursor.fetchone()
                if row:
                    return row[0] != None
        return False
        
    def set_values(self, collection, document_id, values):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        if primary_key in values:
            raise ValueError('Cannot modify document id "%s" of collection %s' % (primary_key, collection))
        pk_column = self.field_column[collection][primary_key]
        column_values = []
        columns = []
        for field, value in values.items():
            column = self.field_column[collection][field]
            columns.append(column)
            field_type = self.field_type[collection][field]
            if field_type.startswith('list_'):
                list_table = 'list_%s_%s' % (table, column)
                column_values.append(self.list_hash(value))
                sql = 'DELETE FROM [%s] WHERE list_id = ?' % list_table
                self.cursor.execute(sql, [document_id])
                sql = 'INSERT INTO [%s] (list_id, i, value) VALUES (?, ?, ?)' % list_table
                if value is None:
                    value = []
                sql_params = [[document_id, 
                            i,
                            self.python_to_column(field_type[5:], value[i])]
                            for i in range(len(value))]
                self.cursor.executemany(sql, sql_params)
                # column_value = repr(value)
            else:
                column_values.append(self.python_to_column(field_type, value))

        sql = 'UPDATE [%s] SET %s WHERE [%s] = ?' % (
            table,
            ', '.join(['[%s] = ?' % c for c in columns]),
            pk_column)
        self.cursor.execute(sql, column_values + [document_id])

    def remove_value(self, collection, document_id, field):
        table = self.collection_table[collection]
        column = self.field_column[collection][field]
        primary_key = self.collection_primary_key[collection]
        pk_column = self.field_column[collection][primary_key]
        field_type = self.field_type[collection][field]
        if field_type.startswith('list_'):
            list_table = 'list_%s_%s' % (table, column)
            sql = 'DELETE FROM [%s] WHERE list_id = ?' % list_table
            self.cursor.execute(sql, [document_id])
        
        sql = 'UPDATE [%s] SET [%s] = NULL WHERE [%s] = ?' % (
            table,
            column,
            pk_column)
        self.cursor.execute(sql, [document_id])
        

    def remove_document(self, collection, document_id):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        pk_column = self.field_column[collection][primary_key]
        document = self.document(collection, document_id)
        for field in self.fields(collection):
            if field.field_type.startswith('list_') and document[field.field_name]:
                self.remove_value(collection, document_id, field.field_name)
        sql = 'DELETE FROM [%s] WHERE [%s] = ?' % (
            table,
            pk_column)
        self.cursor.execute(sql, [document_id])
        
    def parse_filter(self, collection, filter):
        """
        Given a filter string, return a internal query representation that
        can be used with filter_documents() to select documents


        :param collection: the collection for which the filter is intended 
               (str, must be existing)
        
        :param filter: the selection string using the populse_db selection
                       language.

        """
        if filter is None:
            query = None
        else:
            tree = filter_parser().parse(filter)
            query = FilterToSqliteQuery(self, collection).transform(tree)
        return (collection, query)


    def filter_documents(self, parsed_filter, fields=None, as_list=False):
        collection, where_filter = parsed_filter
        if where_filter is None:
            where = None
        else:
            where = ' '.join(where_filter)
        where_data = []
        for doc in self._select_documents(collection, where, where_data,
                                          fields=fields, as_list=as_list):
            yield doc


class FilterToSqliteQuery(FilterToQuery):
    '''
    Implements required methods to produce a SQLite query given a document
    selection filter. This class returns either None (all documents are 
    selected) or an SQL WHERE clause (without the WHERE keyword) as a list 
    of string (that must be joined with spaces). This WHERE clause is useable 
    with a SELECT from the table containing the collection documents. Using a 
    list for combining strings is supposed to be more efficient (especially 
    for long queries).
    '''
    
    def __init__(self, engine, collection):
        '''
        Create a parser for a givent engine and collection
        '''
        FilterToQuery.__init__(self, engine, collection)
        self.table = self.engine.collection_table[collection]

    def get_column(self, field):
        '''
        :return: The SQL representation of a field object.
        '''
        return self.engine.field_column[self.collection][field.field_name]

    _python_to_sql = {
        pdb.FIELD_TYPE_DATE: lambda x: x.isoformat(),
        pdb.FIELD_TYPE_DATETIME: lambda x: x.isoformat(),
        pdb.FIELD_TYPE_TIME: lambda x: x.isoformat(),
        pdb.FIELD_TYPE_BOOLEAN: lambda x: (1 if x else 0),
    }
    _python_to_sql = {
        type(None): lambda x: 'NULL',
        type(''): lambda x: "'{0}'".format(x),
        type(u''): lambda x: "'{0}'".format(x),
        int: lambda x: str(x),
        float: lambda x: str(x),
        datetime.time: lambda x: "'{0}'".format(x.isoformat()),
        datetime.datetime: lambda x: "'{0}'".format(x.isoformat()),
        datetime.date: lambda x: "'{0}'".format(x.isoformat()),
        bool: lambda x: ('1' if x else '0'),
    }

    def get_column_value(self, python_value):
        '''
        Converts a Python value to a value suitable to put in a database column
        '''
        if isinstance(python_value, list):
            c = '(%s)' % ','.join(self.get_column_value(i) for i in python_value)
            return c
        return self._python_to_sql[type(python_value)](python_value)

    def build_condition_all(self):
        return None

    def build_condition_literal_in_list_field(self, value, list_field):
        cvalue = self.get_column_value(value)
        list_column = self.get_column(list_field)
        list_table = 'list_%s_%s' % (self.table, list_column)
        primary_key_column = self.engine.primary_key(self.collection)
        pk_column = self.engine.field_column[
            self.collection][primary_key_column]

        where = ('[{0}] IS NOT NULL AND '
                 '{1} IN (SELECT value FROM {2} '
                 'WHERE list_id = [{3}])').format(list_column,
                                                  cvalue,
                                                  list_table,
                                                  pk_column)
        return [where]

    def build_condition_field_in_list_field(self, field, list_field):
        column = self.get_column(field)
        list_column = self.get_column(list_field)
        list_table = 'list_%s_%s' % (self.table, list_column)
        primary_key_column = self.engine.primary_key(self.collection)
        pk_column = self.engine.field_column[self.collection][primary_key_column]

        where = ('[{0}] IS NOT NULL AND '
                 '[{1}] IN (SELECT value FROM {2} '
                 'WHERE list_id = [{3}])').format(list_column,
                                                  column,
                                                  list_table,
                                                  pk_column)
        return [where]

    def build_condition_field_in_list(self, field, list_value):
        column = self.get_column(field)
        if None in list_value:
            list_value.remove(None)
            where = '[{0}] IS NULL OR [{0}] IN {1}'.format(column,
                self.get_column_value(list_value))
        else:
            where = '[{0}] IN {1}'.format(column,
                self.get_column_value(list_value))
        return [where]

    sql_operators = {
        '==': 'IS',
        '!=': 'IS NOT',
        'ilike': 'LIKE',
    }    
    
    no_list_operator = {'>', '<', '>=', '<=', 'like', 'ilike'}
    
    def build_condition_field_op_field(self, left_field, operator_str, right_field):
        if operator_str == 'ilike':
            field_pattern = 'UPPER([%s])'
        else:
            field_pattern = '[%s]'
        sql_operator = self.sql_operators.get(operator_str, operator_str)
        where = '%s %s %s' % (field_pattern % self.get_column(left_field),
                              sql_operator,
                              field_pattern % self.get_column(right_field))
        return [where]
    
    def build_condition_field_op_value(self, field, operator_str, value):
        if isinstance(value, list):
            if operator_str in self.no_list_operator:
                raise ValueError('operator %s cannot be used with value of list type' % operator_str)
            value = self.engine.list_hash(value)
        if operator_str == 'ilike':
            field_pattern = 'UPPER([%s])'
            if isinstance(value, six.string_types):
                value = value.upper()
        else:
            field_pattern = '[%s]'
        sql_operator = self.sql_operators.get(operator_str, operator_str)
        where = '%s %s %s' % (field_pattern % self.get_column(field),
                              sql_operator,
                              self.get_column_value(value))
        return [where]

    
    def build_condition_value_op_field(self, value, operator_str, field):
        if isinstance(value, list):
            if operator_str in self.no_list_operator:
                raise ValueError('operator %s cannot be used with value of list type' % operator_str)
            value = self.list_hash(value)
        if operator_str == 'ilike':
            field_pattern = 'UPPER([%s])'
            if isinstance(value, six.string_types):
                value = value.upper()
        else:
            field_pattern = '[%s]'
        sql_operator = self.sql_operators.get(operator_str, operator_str)
        where = '%s %s %s' % (self.get_column_value(value),
                              sql_operator,
                              field_pattern % self.get_column(field))
        return [where]

    def build_condition_negation(self, condition):
        if condition is None:
            return ['0']
        return ['NOT', '(' ] + condition + [')']
    
    def build_condition_combine_conditions(self, left_condition, operator_str, right_condition):
        return ['('] + left_condition + [')', operator_str, '('] + right_condition + [')']
