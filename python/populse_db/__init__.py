import threading
from urllib.parse import urlparse

from .engine.sqlite import SQLiteSession

class Database:
    """Entrypoint of populse_db for creating :any:`DatabaseSession` object given
    an URL that identify the underlying database engine.

    Creating a :any:`Database` doesn't connect to the database engine. It just
    parses the URL (ensuring that it corresponds to a valid engine) and stores
    it. The :any:`Database` is a Python context manager that must be using a 
    ``with`` statement that connects to the database and creates a database 
    specific object that implements the API of :any:`DatabaseSession`.

    Example::

        from populse_db import database

        db = Database('sqlite:///tmp/populse_db.sqlite')
        with db as dbs:
            dbs.add_collection('my_collection', primary_key='id')
            dbs['my_collection']['my_document'] = {
                'a key': 'a value',
                'another key': 'another value'
            }

    Database modification within a ``with`` statement is done in a transaction.
    If an exception occurs before the end of the `with`, a rollback is done and
    the database is not modified.

    :any:``Database`` is a reusable context manager. It means that it is allowed
    to use it in several consecutive ``with``::

        from populse_db import database

        db = Database('sqlite:///tmp/populse_db.sqlite')
        with db as dbs:
            dbs.add_collection('my collection')
        
        with db as dbs:
            dbs['my collection']['my document'] = {}

    :any:`Database` is a reentrant context manager. It is possible to use a
    ``with`` statement within another ``with`` that already use the same 
    :any:`Database`. In that case, the same context is returned and the 
    transaction is not ended in any of the inner context, it is terminated
    when the outer context exits::

        from populse_db import database

        db = Database('sqlite:///tmp/populse_db.sqlite')
        with db as dbs1:
            # Connection to the database and transaction starts here
            dbs.add_collection('my collection')
            with db as dbs2:
                # here: dbs1 is dbs2 == True
                # No new connection to the database is done
                dbs['my collection']['my document'] = {}
            # After the inner with, connection to the database is not 
            # closed and transaction is ongoing (no commit nor rollback 
            # is done)
        # After the end of the outer with, transaction is terminated and
        # connection to the database is closed.

    On a multithreaded application, each thread using :any:`Database` context
    manager gets its own :any:`DatabaseSession` instance with its own database
    connection.

    methods:
        - __enter__: Creates a :any:`DatabaseSession` instance
        - __exit__: Release resource used by the DatabaseSession
    """

    def __init__(self, database_url):
        """Creates a :any:`Database` instance.

        :param database_url: URL defining database engine and its parameters. The
            engine URL must have the following pattern: 
            dialect://user:password@host/dbname[?key=value..].
            To date dialect can only be ``sqlite`` but ``postgresql`` is planned.

            Examples:
                    - ``sqlite:///foo.db``
                    - ``postgresql://scott:tiger@localhost/test``
        """

        self.url = urlparse(database_url)
        if self.url.scheme in ('', 'sqlite'):
            self.session_class = SQLiteSession
        else:
            raise ValueError(f'Invalid datbase type in database URL: {database_url}')
        self.session_parameters = self.session_class.parse_url(self.url)
        self.thread_local = threading.local()
        session = None
        depth = 0
        self.thread_local.populse_db = (session, depth)

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
        session, depth = self.thread_local.populse_db
        if session is None:
            args, kwargs = self.session_parameters
            session = self.session_class(*args, **kwargs)
        depth += 1
        self.thread_local.populse_db = (session, depth)
        return session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        session, depth = self.thread_local.populse_db
        depth -= 1
        if depth == 0:
            if exc_type is None:
                session.commit()
            else:
                session.rollback()
            session = None
        self.thread_local.populse_db = (session, depth)

# Import here to allow the followint import in external
# modules:
#   from populse_db import json_encode, json_decode
from .database import json_encode, json_decode
