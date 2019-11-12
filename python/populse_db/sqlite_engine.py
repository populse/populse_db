import datetime
import hashlib
import json
import re
import six
import sqlite3

import populse_db.database as pdb
from populse_db.filter import FilterToQuery, filter_parser

import dateutil

# Table names
FIELD_TABLE = 'field'
COLLECTION_TABLE = 'collection'
                
class SQLiteEngine:
    def __init__(self, database):
        self.connection = sqlite3.connect(database)
        self.cursor = None
           
    _valid_identifier = re.compile(r'^[_A-Za-z][a-zA-Z0-9_]*$')
    
    def name_to_sql(self, name):
        """
        Transforms the name into a valid and unique table/column name, by hashing it with md5

        :param name: Name (str)

        :return: Valid and unique (hashed) table/column name
        """
        if self._valid_identifier.match(name):
            return name
        else:
            return '__' + hashlib.md5(name.encode('utf-8')).hexdigest()

    def __enter__(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA case_sensitive_like=ON')
        self.cursor.execute('PRAGMA foreign_keys=ON')
        
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
            self.table_row[table] = pdb.row_class(table, columns)
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
            self.table_row[table] = pdb.row_class(table, columns)
            self.table_document[table] = pdb.row_class(table, fields)
        
        sql = 'SELECT collection_name, field_name, field_type, column FROM [%s]' % FIELD_TABLE
        self.cursor.execute(sql)
        self.field_column = {}
        self.field_type = {}
        for collection, field, field_type, column in self.cursor:
            self.field_column.setdefault(collection, {})[field] = column
            self.field_type.setdefault(collection, {})[field] = field_type
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor = None
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
    
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
            return 'INT'
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
                                   self.field_column[i.collection_name][i.name])
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

    def add_collection(self, name, primary_key):
        table_name = self.name_to_sql(name)
        pk_column = self.name_to_sql(primary_key)
        sql = 'CREATE TABLE [%s] ([%s] TEXT PRIMARY KEY)' % (table_name, pk_column)
        try:
            self.cursor.execute(sql)
        except sqlite3.OperationalError as e:
            raise ValueError(str(e))
        self.table_row[table_name] = pdb.row_class(table_name, [pk_column])
        self.table_document[table_name] = pdb.row_class(table_name, [primary_key])
        
        sql = 'INSERT INTO [%s] (collection_name, primary_key, table_name) VALUES (?, ?, ?)' % COLLECTION_TABLE
        self.cursor.execute(sql, [name, primary_key, table_name])
        self.collection_table[name] = table_name
        self.collection_primary_key[name] = primary_key
        
        sql = 'INSERT INTO [%s] (field_name, collection_name, field_type, description, has_index, column) VALUES (?, ?, ?, ?, 1, ?)' % FIELD_TABLE
        self.cursor.execute(sql, [primary_key, name, pdb.FIELD_TYPE_STRING,
                                  'Primary_key of the document collection %s' % name,
                                  pk_column])
        self.field_column[name] = {primary_key: pk_column}
        self.field_type[name] = {primary_key: pdb.FIELD_TYPE_STRING}
        
    def collection(self, name):
        sql = 'SELECT * FROM [%s] WHERE collection_name = ?' % COLLECTION_TABLE
        self.cursor.execute(sql, [name])
        row = self.cursor.fetchone()
        if row is not None:
            return self.table_row[COLLECTION_TABLE](*row)
        return None
    
    def primary_key(self, collection):
        return self.collection_primary_key[collection]
    
    def remove_collection(self, name):
        table = self.collection_table[name]
        
        sql = 'DELETE FROM [%s] WHERE collection_name = ?' % FIELD_TABLE
        self.cursor.execute(sql, [name])

        sql = 'DELETE FROM [%s] WHERE collection_name = ?' % COLLECTION_TABLE
        self.cursor.execute(sql, [name])
        del self.collection_table[name]
        del self.collection_primary_key[name]
        del self.table_row[table]
        del self.table_document[table]
        del self.field_column[name]
        del self.field_type[name]
        
        sql = 'DROP TABLE [%s]' % table
        self.cursor.execute(sql)

    def collections(self):        
        sql = 'SELECT * FROM [%s]' % COLLECTION_TABLE
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
            sql = 'CREATE TABLE [list_{0}_{1}] (list_id TEXT, i INT, value {2})'.format(table, column, self.sql_type(type[5:]))
            self.cursor.execute(sql)
            sql = 'CREATE INDEX [list_{0}_{1}_id] ON [list_{0}_{1}] (list_id)'.format(table, column)
            self.cursor.execute(sql)
            sql = 'CREATE INDEX [list_{0}_{1}_i] ON [list_{0}_{1}] (i ASC)'.format(table, column)
            self.cursor.execute(sql)
    
    def add_document(self, collection, document, create_missing_fields):
        table = self.collection_table[collection]
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
                list_id = self.list_hash(value)
                column_values[column] = list_id
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
            for params in sql_params:
                self.cursor.execute(sql, params)
            
    def has_field(self, collection, field):
        return self.field_column.get(collection, {}).get(field) is not None
    
    def field(self, collection, field):
        sql = 'SELECT * FROM [%s] WHERE collection_name = ? AND field_name = ?' % FIELD_TABLE
        self.cursor.execute(sql, [collection, field])
        row = self.cursor.fetchone()
        if row is not None:
            return self.table_row[FIELD_TABLE](*row)
        return None

    def fields(self, collection=None):
        sql = 'SELECT * FROM [%s]' % FIELD_TABLE
        if collection is None:
            data = []
        else:
            sql += ' WHERE collection_name = ?'
            data = [collection]
        self.cursor.execute(sql, data)
        row_class = self.table_row[FIELD_TABLE]
        for row in self.cursor:
            yield row_class(*row)
    
    # Some types (e.g. time, date and datetime) cannot be
    # serialized/deserialized into string with repr/ast.literal_eval.
    # This is a problem for storing the corresponding list_columns in
    # database. For the list types with this problem, we record in the
    # following dictionaries the functions that must be used to serialize
    # (in _list_item_to_string) and deserialize (in _string_to_list_item)
    # the list items.
    _python_to_sql_data = {
        pdb.FIELD_TYPE_DATE: lambda x: x.isoformat(),
        pdb.FIELD_TYPE_DATETIME: lambda x: x.isoformat(),
        pdb.FIELD_TYPE_TIME: lambda x: x.isoformat(),
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

    def has_document(self, collection, document):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        sql = 'SELECT COUNT(*) FROM [%s] WHERE [%s] = ?' % (table, primary_key)
        self.cursor.execute(sql, [document])
        return bool(self.cursor.fetchone()[0])


    def _select_documents(self, collection, where, where_data):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        sql = 'SELECT * FROM [%s]' % table
        if where:
            sql += ' WHERE %s' % where
        self.cursor.execute(sql, where_data)
        for row in self.cursor.fetchall():
            result = self.table_document[table](*row)
            values = []
            for field in result:
                field_type = self.field_type[collection][field]
                if field_type.startswith('list_'):
                    item_type = field_type[5:]
                    column = self.field_column[collection][field]
                    list_index = result[field]
                    if list_index is None:
                        values.append(None)
                    else:
                        sql = 'SELECT value FROM list_{0}_{1} WHERE list_id = ? ORDER BY i'.format(table, column)
                        self.cursor.execute(sql, [list_index])
                        values.append([self.column_to_python(item_type,i[0]) for i in self.cursor])
                else:
                    values.append(self.column_to_python(field_type, result[field]))
            result._values = values
            yield result
    
    def document(self, collection, document):
        primary_key = self.collection_primary_key[collection]
        where = '[%s] = ?' % primary_key
        where_data = [document]
        
        try:
            return self._select_documents(collection, where, where_data).__next__()
        except StopIteration:
            return None
    
    def has_value(self, collection, document_id, field):
        table = self.collection_table.get(collection)
        if table is not None:
            primary_key = self.collection_primary_key[collection]
            column = self.field_column[collection].get(field)
            if column is not None:
                sql = 'SELECT [%s] FROM [%s] WHERE [%s] = ?' % (column, table, primary_key)
                self.cursor.execute(sql, [document_id])
                row = self.cursor.fetchone()
                if row:
                    return row[0] is not None
        return False
        
    def set_value(self, collection, document_id, field, value):
        table = self.collection_table[collection]
        column = self.field_column[collection][field]
        primary_key = self.collection_primary_key[collection]
        field_type = self.field_type[collection][field]
        if field_type.startswith('list_'):
            list_table = 'list_%s_%s' % (table, column)
            list_id = self.list_hash(value)
            column_value = list_id
            sql = 'INSERT INTO [%s] (list_id, i, value) VALUES (?, ?, ?)' % list_table
            sql_params = [[list_id, 
                          i,
                          self.python_to_column(field_type[5:], value[i])]
                          for i in range(len(value))]
            for p in sql_params:
                self.cursor.execute(sql, p)
        else:
            column_value = self.python_to_column(field_type, value)
        
        sql = 'UPDATE [%s] SET [%s] = ? WHERE [%s] = ?' % (
            table,
            column,
            primary_key)
        try:
            self.cursor.execute(sql, [column_value, document_id])
        except sqlite3.IntegrityError as e:
            raise ValueError(str(e))

    def remove_value(self, collection, document_id, field):
        table = self.collection_table[collection]
        column = self.field_column[collection][field]
        primary_key = self.collection_primary_key[collection]
        field_type = self.field_type[collection][field]
        if field_type.startswith('list_'):
            list_table = 'list_%s_%s' % (table, column)
            sql = 'SELECT [%s] FROM [%s] WHERE [%s] = ?' % (column, table, primary_key)
            self.cursor.execute(sql, [document_id])
            list_id = self.cursor.fetchone()
            if list_id:
                list_id = list_id[0]
                sql = 'DELETE FROM [%s] WHERE list_id = ? LIMIT 1' % list_table
                self.cursor.execute(sql, [list_id])
        
        sql = 'UPDATE [%s] SET [%s] = NULL WHERE [%s] = ?' % (
            table,
            column,
            primary_key)
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
        tree = filter_parser().parse(filter)
        query = FilterToSqliteQuery(self, collection).transform(tree)
        return query

    def filter_documents(self, collection, where_filter):
        if where_filter is None:
            where = None
        else:
            where = ' '.join(where_filter)
        where_data = []
        for doc in self._select_documents(collection, where, where_data):
            yield doc


class FilterToSqliteQuery(FilterToQuery):
    def __init__(self, engine, collection):
        super(FilterToSqliteQuery, self).__init__(engine, collection)
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
        '''
        Builds a condition checking if a constant value is in a list field
        '''
        cvalue = self.get_column_value(value)
        list_column = self.get_column(list_field)
        list_table = 'list_%s_%s' % (self.table, list_column)
    
        where = ('[{0}] IS NOT NULL AND '
                 '{1} IN (SELECT value FROM {2} '
                 'WHERE list_id = [{0}])').format(list_column,
                                                  cvalue,
                                                  list_table)
        return [where]

    def build_condition_field_in_list_field(self, field, list_field):
        '''
        Builds a condition checking if a field value is in another
        list field value
        '''
        column = self.get_column(field)
        list_column = self.get_column(list_field)
        list_table = 'list_%s_%s' % (self.table, list_column)
    
        where = ('[{0}] IS NOT NULL AND '
                 '[{1}] IN (SELECT value FROM {2} '
                 'WHERE list_id = [{0}])').format(list_column,
                                                  column,
                                                  list_table)
        return [where]

    def build_condition_field_in_list(self, field, list_value):
        '''
        Builds a condition checking if a field value is a
        constant list value
        '''
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
        return ['NOT', '(' ] + condition + [')']
    
    def build_condition_combine_conditions(self, left_condition, operator_str, right_condition):
        return ['('] + left_condition + [')', operator_str, '('] + right_condition + [')']
