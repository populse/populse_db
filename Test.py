import os
from model.DatabaseModel import createDatabase, TAG_ORIGIN_RAW, TAG_TYPE_STRING
from database.Database import Database
import unittest
import shutil

path = os.path.relpath(os.path.join(".", "test.db"))

class TestDatabaseMethods(unittest.TestCase):

    def test_databasecreation(self):
        """
        Tests the database creation
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        createDatabase(path)
        self.assertTrue(os.path.exists(path))

    def test_databaseconstructor(self):
        """
        Tests the database constructor
        """
        global path

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

    def test_addtag(self):
        """
        Tests the method adding a tag
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        tag = database.get_tag("PatientName")
        self.assertEqual(tag.name, "PatientName")
        self.assertTrue(tag.visible)
        self.assertEqual(tag.origin, TAG_ORIGIN_RAW)
        self.assertEqual(tag.type, TAG_TYPE_STRING)
        self.assertIsNone(tag.unit)
        self.assertIsNone(tag.default_value)
        self.assertEqual(tag.description, "Name of the patient")

    def test_gettag(self):
        """
        Tests the method giving the Tag object of a tag
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        tag = database.get_tag("PatientName")
        self.assertIsInstance(tag, database.classes["tag"])
        tag = database.get_tag("Test")
        self.assertIsNone(tag)

    def test_removetag(self):
        """
        Tests the method removing a tag
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.remove_tag("PatientName")
        tag = database.get_tag("PatientName")
        self.assertIsNone(tag)

    def test_addscan(self):
        """
        Tests the method adding a scan
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")

    def test_getcurrentvalue(self):
        """
        Tests the method giving the current value, given a tag and a scan
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)
        database.add_value("scan1", "PatientName", "test")
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

    def test_addvalue(self):
        """
        Tests the method adding a value
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_value("scan1", "PatientName", "test")
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

if __name__ == '__main__':
    unittest.main(exit=False)
    os.remove(path)