# KNOWN BUG under Python 3: When the following line is executed,
# populse_db.database is added to sys.modules. As a result, future calls to
# 'from populse_db.database import ...' will succeed, even if the populse_db
# package cannot be imported (e.g. if importing populse_db.filter fails due to
# lark-parser missing).
from . import database
from . import filter
