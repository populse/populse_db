from . import database
from . import filter
from urllib.parse import urlparse

from .engine.sqlite import SQLiteSession

class Database(object):
    """
    A Database is the entrypoin to populse_db. It parse, check and store
    the URL required to create a database. I return a database engine specific
    session object via a with statement.

    Example:

        from populse_db import database

        db = Database('sqlite:///:memory:')
        with db as dbs:
            db.add_collection('my_collection', key='id')
            db['my_collection']['my_document'] = {
                'a key': 'a value',
                'another key': 'another value'
            }

    methods:
        - __enter__: Creates a DatabaseSession instance
        - __exit__: Release resource used by the DatabaseSession
    """

    def __init__(self, database_url):
        """Initialization of the database

        :param database_url: Database engine

            The engine is constructed this way: dialect://user:password@host/dbname[?key=value..]

            The dialect can be sqlite or postgresql

            For sqlite databases, the file can be not existing yet, it will be created in this case

            Examples:
                    - "sqlite:///foo.db"
                    - "postgresql://scott:tiger@localhost/test"

        :raise ValueError: - If database_url is invalid
                           - If the schema is not coherent with the API (the database is not a populse_db database)
        """

        self.url = urlparse(database_url)
        if self.url.scheme in ('', 'sqlite'):
            self.session_class = SQLiteSession
        else:
            raise ValueError(f'Invalid datbase type in database URL: {database_url}')
        self.session_parameters = self.session_class.parse_url(self.url)


    def __enter__(self):
        """
        Return a DatabaseSession instance for using the database. This is
        supposed to be called using a "with" statement:
        
        with database as session:
           session.add_document(...)
           
        Therefore __exit__ must be called to get rid of the session.
        When called recursively, the underlying database session returned
        is the same. The commit/rollback of the session is done only by the
        outermost __enter__/__exit__ pair (i.e. by the outermost with
        statement).
        """
        args, kwargs = session_parameters
        return self.session_class(*args, **kwargs)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass