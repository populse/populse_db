import importlib.metadata
import os
import re
import threading
from contextlib import contextmanager
from urllib.parse import urlparse

try:
    __version__ = importlib.metadata.__version__ = importlib.metadata.version(
        "populse_db"
    )
except importlib.metadata.PackageNotFoundError:
    __version__ = None


class Database:
    """Entrypoint of populse_db for creating :any:`DatabaseSession` object given
    an URL that identify the underlying database engine.

    Creating a :any:`Database` doesn't connect to the database engine. It just
    parses the URL (ensuring that it corresponds to a valid engine) and stores
    it. The :any:`Database` is a Python context manager that must be using a
    ``with`` statement that connects to the database and creates a database
    specific object that implements the API of :any:`DatabaseSession`.

    Example::

        from populse_db import Database

        db = Database('sqlite:///tmp/populse_db.sqlite')
        with db as dbs:
            dbs.add_collection('my_collection', primary_key='id')
            dbs['my_collection']['my_document'] = {
                'a key': 'a value',
                'another key': 'another value'
            }

    Database modification within a ``with`` statement is done in a transaction.
    If an exception occurs before the end of the `with`, a rollback is done and
    the database is not modified. By default, this transaction allows to read
    the database in parallel. The first database modification will acquire an
    exclusive access to the database. It is possible to request an exclusive
    session (preventing any other access to the database by other processes
    or threads) by using the `exclusive` property::

        from populse_db import Database

        db = Database('sqlite:///tmp/populse_db.sqlite')
        with db.exclusive as dbs:
            # All other access to the database are blocked
            # until the end of this "with" statement.

    :any:``Database`` is a reusable context manager. It means that it is allowed
    to use it in several consecutive ``with``::

        from populse_db import Database

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

        from populse_db import Database

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
        - __exit__: Release resource used by the :any:`DatabaseSession`
    """

    def __init__(self, database_url, timeout=None, create=False, echo_sql=None):
        """Creates a :any:`Database` instance.

        :param database_url: URL defining database engine and its parameters. The
            engine URL must have the following pattern:
            dialect://user:password@host/dbname[?key=value..].
            To date dialect can only be ``sqlite`` but ``postgresql`` is planned.

            Examples:
                    - /somewhere/a_file.popdb
                    - ``sqlite:///foo.db``
                    - ``postgresql://scott:tiger@localhost/test``
        """

        self.thread_local = threading.local()

        # Remove the drive letter for Windows paths
        if os.name == "nt" and re.match(r"^[a-zA-Z]:\\", database_url):
            database_url = database_url[2:]

        self.url = urlparse(database_url)
        self.create = create
        self.echo_sql = echo_sql

        if timeout:
            self.timeout = int(timeout)

        else:
            self.timeout = None

        if self.url.scheme in ("", "sqlite"):
            # sqlite3 module (used in .engine.sqlite) is optional
            # because it does not exist in Pyodide distribution (i.e.
            # PyScript).
            from .engine.sqlite import create_sqlite_session_factory
            self.session_factory = create_sqlite_session_factory(self.url)

        else:
            raise ValueError(f"Invalid database type in database URL: {database_url}")

    def session(self, exclusive=False, create=None):
        if create is None:
            create = self.create
        return self.session_factory(
            exclusive=exclusive,
            timeout=self.timeout,
            echo_sql=self.echo_sql,
            create=create,
        )

    def begin_session(self, exclusive, create=None):
        if create is None:
            create = self.create
        session_depth = getattr(self.thread_local, "populse_db", None)
        if session_depth is None:
            session = self.session(exclusive=exclusive, create=create)
            depth = 0
        else:
            session, depth = session_depth
        depth += 1
        self.thread_local.populse_db = (session, depth)
        return session

    def end_session(self, rollback):
        session, depth = self.thread_local.populse_db
        depth -= 1
        if depth == 0:
            session.close(rollback=rollback)
            del self.thread_local.populse_db
        else:
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
        return self.begin_session(exclusive=False, create=self.create)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_session(rollback=(exc_type is not None))

    @property
    @contextmanager
    def exclusive(self, create=None):
        if create is None:
            create = self.create
        try:
            session = self.begin_session(exclusive=True, create=create)
            yield session
            self.end_session(rollback=False)
        except Exception:
            self.end_session(rollback=True)
            raise


# Import here to allow the following import in external
# modules:
from .database import json_decode, json_encode  # noqa: F401, E402
from .storage import Storage  # noqa: F401, E402
