import sqlite3

                
class SQLiteEngine:
    def __init__(self, database):
        self.connection = sqlite3.connect(database)
        cursor = self.connection.cursor()
        cursor.execute('PRAGMA case_sensitive_like=ON')
        cursor.execute('PRAGMA foreign_keys=ON')

        with self.cursor as cursor:
            if not cursor.has_table(COLLECTION_TABLE):
                cursor.create_base_tables()
                self.connection.commit()
    
    
    def sqlite_delete_engine(self):
        self.engine.close()
        self.engine = None

    def execute(self, sql, data=None):
        c = self.cnx.cursor()
        if data:
            return c.execute(sql, data)
        else:
            return c.execute(sql)

    def cursor(self):
        return SQLiteCursor(self.connection)

class SQLiteEngineCursor:
    def __init__(self, connection):
        self.cursor = connection.cursor
    
    def create_base_tables(self)
        sql = '''CREATE TABLE {0} (
            field_name TEXT PRIMARY KEY,
            collection_name TEXT PRIMARY KEY,
            field_type ENUM ({1}) NOT NULL,
            decription TEXT)
        '''.format(FIELD_TABLE,
                ','.join("'%s'" % i for i in (FIELD_TYPE_STRING,
                                                FIELD_TYPE_INTEGER, 
                                                FIELD_TYPE_FLOAT, 
                                                FIELD_TYPE_BOOLEAN,
                                                FIELD_TYPE_DATE,
                                                FIELD_TYPE_DATETIME,
                                                FIELD_TYPE_TIME,
                                                FIELD_TYPE_JSON,
                                                FIELD_TYPE_LIST_STRING,
                                                FIELD_TYPE_LIST_INTEGER,
                                                FIELD_TYPE_LIST_FLOAT,
                                                FIELD_TYPE_LIST_BOOLEAN,
                                                FIELD_TYPE_LIST_DATE,
                                                FIELD_TYPE_LIST_DATETIME,
                                                FIELD_TYPE_LIST_TIME,
                                                FIELD_TYPE_LIST_JSON)))
        self.cursor.execute(sql)
        
        sql = '''CREATE TABLE {0} (
            collection_name TEXT PRIMARY KEY,
            primary_key TEXT NOT NULL)
        '''.format(COLLECTION_TABLE)
        self.cursor.execute(sql)
    
    def delete_tables(self):
        for table in (FIELD_TABLE, COLLECTION_TABLE):
            sql = 'DROP TABLE %s' % table
            self.cursor.execute(sql)
        
    def has_table(self, table):
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'" % table)
        return self.cursor.fetchone()[0] == 1

    def add_collection(name, primary_key, table_name, pk_column):
        sql = 'CREATE TABLE %s (%s TEXT PRIMARY KEY)' % (table_name, pk_column)
        self.cursor.execute(sql)
        
        sql = 'INSERT INTO %s (collection_name, primary_key) VALUES (?, ?)' % COLLECTION_TABLE
        self.cursor.execute(sql, [name, primary_key])
        

        sql = 'INSERT INTO %s (field_name, collection_name) VALUES (?, ?)' % FIELD_TABLE
        self.cursor.execute(sql, [primary_key, name, FIELD_TYPE_STRING,
                                  'Primary_key of the document collection %s' % name])
