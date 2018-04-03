import os
from model.DatabaseModel import createDatabase, TAG_ORIGIN_RAW, TAG_TYPE_STRING, Tag
from database.Database import Database
import unittest

class TestDatabaseMethods(unittest.TestCase):

    def test_databasecreation(self):
        """
        Tests the database creation
        """

        path = os.path.relpath(os.path.join(".", "test.db"))
        if os.path.exists(path):
            os.remove(path)
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
        os.remove(path)

    def test_addtag(self):
        """
        Tests the method adding a tag
        """

        path = os.path.relpath(os.path.join(".", "test.db"))
        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        tag = database.get_tag("PatientName")
        self.assertEqual(tag.name, "PatientName")
        self.assertEqual(tag.visible, True)
        self.assertEqual(tag.origin, TAG_ORIGIN_RAW)
        self.assertEqual(tag.type, TAG_TYPE_STRING)
        self.assertEqual(tag.unit, None)
        self.assertEqual(tag.default_value, None)
        self.assertEqual(tag.description, "Name of the patient")
        os.remove(path)

    def test_gettag(self):
        """
        Tests the method giving the Tag object of a tag
        """

        path = os.path.relpath(os.path.join(".", "test.db"))
        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        tag = database.get_tag("PatientName")
        self.assertNotEqual(tag, None)
        self.assertIsInstance(tag, Tag)
        tag = database.get_tag("Test")
        self.assertEqual(tag, None)
        os.remove(path)

    def test_removetag(self):
        """
        Tests the method removing a tag
        """

        path = os.path.relpath(os.path.join(".", "test.db"))
        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.remove_tag("PatientName")
        tag = database.get_tag("PatientName")
        self.assertEqual(tag, None)

if __name__ == '__main__':
    unittest.main()