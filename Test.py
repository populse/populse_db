import os
from model.DatabaseModel import createDatabase

path = os.path.relpath(os.path.join(".", "test.db"))
os.remove(path)
createDatabase(path)