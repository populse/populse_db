import hashlib
import json
import re
import sqlite3

import dateutil

import populse_db.database as pdb

# Table names
FIELD_TABLE = "field"
COLLECTION_TABLE = "collection"



class Row:
    _key_indices = {}
    
    def __init__(self, *args, **kwargs):
        self._values = [None] * len(self._key_indices)
        i = 0
        for value in args:
            self._values[i] = value
            i += 1
        for key, value in kwargs.items():
            self._values[self._key_indices[key]] = value
    
    def __iter__(self):
        return iter(self._key_indices)
    
    def __getattr__(self, name):
        try:
            return self._values[self._key_indices[name]]
        except KeyError:
            raise AttributeError(repr(name))

    def __getitem__(self, name_or_index):
        if isinstance(name_or_index, str):
            return self._values[self._key_indices[name_or_index]]
        else:
            return self._values[name_or_index]
    
    @classmethod
    def _append_key(cls, key):
        cls._key_indices[key] = len(cls._key_indices)
    
    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, ','.join('%s = %s' % (k, repr(self._values[i])) for k, i in self._key_indices.items()))
    
    def _dict(self):
        return dict((i, self[i]) for i in self._key_indices if self[i] is not None)

def row_class(name, keys):
    return type(name, (Row,), {'_key_indices': dict(zip(keys, 
                                                    range(len(keys))))})        
                
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
        
        
        sql = 'SELECT collection_name, primary_key FROM [%s]' % COLLECTION_TABLE
        self.cursor.execute(sql)
        self.collection_primary_key = dict(self.cursor)
        self.collection_table = {}
        self.table_row = {}
        for table in (COLLECTION_TABLE, FIELD_TABLE):
            sql = 'PRAGMA table_info([%s])' % table
            self.cursor.execute(sql)
            columns = [i[1] for i in self.cursor]
            self.table_row[table] = row_class(table, columns)
        for i in self.cursor:
            collection, table = i
            self.collection_table[collection] = table
            sql = 'PRAGMA table_info([%s])' % table
            self.cursor.execute(sql)
            columns = [i[1] for i in self.cursor()]
            self.table_row[table] = row_class(table, columns)
        
        sql = 'SELECT collection_name, field_name, column FROM [%s]' % FIELD_TABLE
        self.cursor.execute(sql)
        self.field_column = {}
        for collection, field, column in self.cursor:
            self.field_column.setdefault(collection, {})[field] = column
        
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
    
    
    def delete_tables(self):
        for table in [FIELD_TABLE, COLLECTION_TABLE] + [i[5] for i in self.collections()]:
            sql = 'DROP TABLE [%s]' % table
            self.cursor.execute(sql)
        
    def has_table(self, table):
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'" % table)
        return self.cursor.fetchone()[0] == 1
    
    def has_collection(self, collection):
        return collection in self.collection_table

    def add_collection(self, name, primary_key):
        table_name = self.name_to_sql(name)
        pk_column = self.name_to_sql(primary_key)
        sql = 'CREATE TABLE [%s] ([%s] TEXT PRIMARY KEY)' % (table_name, pk_column)
        self.cursor.execute(sql)
        self.table_row[table_name] = row_class(table_name, [pk_column])
        
        sql = 'INSERT INTO [%s] (collection_name, primary_key, table_name) VALUES (?, ?, ?)' % COLLECTION_TABLE
        self.cursor.execute(sql, [name, primary_key, table_name])
        self.collection_table[name] = table_name
        self.collection_primary_key[name] = primary_key
        
        sql = 'INSERT INTO [%s] (field_name, collection_name, field_type, description, has_index, column) VALUES (?, ?, ?, ?, 1, ?)' % FIELD_TABLE
        self.cursor.execute(sql, [primary_key, name, pdb.FIELD_TYPE_STRING,
                                  'Primary_key of the document collection %s' % name,
                                  pk_column])
        self.field_column[name] = {primary_key: pk_column}
        
    def collection(self, name):
        sql = 'SELECT * FROM [%s] WHERE collection_name = ?' % COLLECTION_TABLE
        self.cursor.execute(sql, [name])
        row = self.cursor.fetchone()
        if row is not None:
            return self.table_row[COLLECTION_TABLE](*row)
        return None
        
    def remove_collection(self, name):
        table = self.collection_table[name]
        
        sql = 'DELETE FROM [%s] WHERE collection_name = ?' % FIELD_TABLE
        self.cursor.execute(sql, [name])

        sql = 'DELETE FROM [%s] WHERE collection_name = ?' % COLLECTION_TABLE
        self.cursor.execute(sql, [name])
        del self.collection_table[name]
        del self.collection_primary_key[name]
        del self.table_row[table]
        
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
        
        self.cursor.execute(sql)
        sql = 'INSERT INTO [%s] (field_name, collection_name, field_type, description, has_index, column) VALUES (?, ?, ?, ?, ?, ?)' % FIELD_TABLE
        self.cursor.execute(sql, [field,
                                  collection,
                                  type,
                                  description,
                                  (1 if index else 0),
                                  column])
        self.field_column.setdefault(collection, {})[field] = column
        if index:
            sql = 'CREATE INDEX [%s] ([%s])'% (table, column)
            cursor.execute(sql)
        if type.startswith('list_'):
            sql = 'CREATE TABLE [list_{0}_{1}] (list_id INT, i INT, value {2})'.format(table, column, self.sql_type(type[5:]))
            self.cursor.execute(sql)
            sql = 'CREATE INDEX [list_{0}_{1}_id] ON [list_{0}_{1}] (list_id)'.format(table, column)
            self.cursor.execute(sql)
            sql = 'CREATE INDEX [list_{0}_{1}_i] ON [list_{0}_{1}] (i ASC)'.format(table, column)
            self.cursor.execute(sql)
    
    def add_document(self, collection, document, create_missing_fields):
        table = self.collection_table[collection]
        lists = []
        column_values = {}
        field_types = dict((field.field_name, field.field_type) 
                           for field in self.fields(collection))
        for field, value in document.items():
            field_type = field_types.get(field)
            if field_type is None:
                if not create_missing_fields:
                    raise ValueError('Collection {0} has no field {1}'
                                    .format(collection, field))
                try:
                    field_type = pdb.python_value_type(value)
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
                sql = 'SELECT max(list_id) FROM [%s]' % list_table
                self.cursor.execute(sql)
                list_id = self.cursor.fetchone()[0]
                if list_id is None:
                    list_id = 0
                else:
                    list_id += 1
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
        self.cursor.execute(sql, list(column_values.values()))
        for sql, sql_params in lists:
            for params in sql_params:
                self.cursor.execute(sql, params)
        
        
    #def ensure_field_for_value(self, collection, field , value, create):
        #field_row = self.field(collection, field)
        #if field_row is None:
            #if not create:
                #raise ValueError('Collection {0} has no field {1}'
                                 #.format(collection, field))
            #try:
                #field_type = pdb.python_value_type(value)
            #except KeyError:
                #raise ValueError('Collection {0} has no field {1} and it '
                                 #'cannot be created from a value of type {2}'
                                 #.format(collection,
                                         #field,
                                         #type(value)))
            #self.add_field(collection, field, field_type,
                           #description=None, index=False)
            #return field_type
        #return field_row.field_type
    
    def field(self, collection, field):
        sql = 'SELECT * FROM [%s] WHERE collection_name = ? AND field_name = ?' % FIELD_TABLE
        self.cursor.execute(sql, [collection, field])
        row = self.cursor.fetchone()
        if row is not None:
            return self.table_row[FIELD_TABLE](*row)
        return None

    def fields(self, collection):
        sql = 'SELECT * FROM [%s] WHERE collection_name = ?' % FIELD_TABLE
        self.cursor.execute(sql, [collection])
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
    _python_to_sql = {
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
        converter = SQLiteEngine._python_to_sql.get(field_type)
        if converter is not None:
            return converter(value)
        elif isinstance(value, dict):
            return json.dumps(value)
        else:
            return value

    @staticmethod
    def column_to_python(field_type, value):
        converter = SQLiteEngine._sql_to_python.get(field_type)
        if converter is not None:
            return converter(value)
        else:
            return value

    def document(self, collection, document):
        table = self.collection_table[collection]
        primary_key = self.collection_primary_key[collection]
        sql = 'SELECT * FROM [%s] WHERE [%s] = ?' % (table, primary_key)
        self.cursor.execute(sql, [document])
        row = self.cursor.fetchone()
        if row is not None:
            field_types = dict((field.field_name, field.field_type) 
                            for field in self.fields(collection))
            result = self.table_row[table](*row)
            values = []
            for field in result:
                field_type = field_types[field]
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
            return result
        return None
    
