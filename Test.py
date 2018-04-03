import os
from model.DatabaseModel import createDatabase
from database.Database import Database
import unittest

class TestDatabaseMethods(unittest.TestCase):

    def test_databasecreation(self):
        """
        Tests the database creation
        """
        path = os.path.relpath(os.path.join(".", "test.db"))
        createDatabase(path)
        self.assertTrue(os.path.exists(path))
        os.remove(path)

    def test_databaseconstructor(self):
        """
        Tests the database constructor
        """
        path = os.path.relpath(os.path.join(".", "test.db"))

        # Testing without the database file existing
        if os.path.exists(path):
            os.remove(path)
        Database(path)
        self.assertTrue(os.path.exists(path))

        # Testing with the database file existing
        os.remove(path)
        createDatabase(path)
        Database(path)
        self.assertTrue(os.path.exists(path))

if __name__ == '__main__':
    unittest.main()