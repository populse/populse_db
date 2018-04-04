import os
from model.DatabaseModel import createDatabase, TAG_ORIGIN_RAW, TAG_TYPE_STRING, TAG_TYPE_FLOAT
from database.Database import Database
import unittest
import shutil

path = os.path.relpath(os.path.join(".", "test.db"))

class TestDatabaseMethods(unittest.TestCase):

    def test_database_creation(self):
        """
        Tests the database creation
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        createDatabase(path)
        self.assertTrue(os.path.exists(path))

    def test_database_constructor(self):
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

    def test_add_tag(self):
        """
        Tests the method adding a tag
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        tag = database.get_tag("PatientName")

        # Checking the tag properties
        self.assertEqual(tag.name, "PatientName")
        self.assertTrue(tag.visible)
        self.assertEqual(tag.origin, TAG_ORIGIN_RAW)
        self.assertEqual(tag.type, TAG_TYPE_STRING)
        self.assertIsNone(tag.unit)
        self.assertIsNone(tag.default_value)
        self.assertEqual(tag.description, "Name of the patient")

        # TODO Testing tag table creation

    def test_get_tag(self):
        """
        Tests the method giving the Tag object of a tag
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing that the tag is returned if it exists
        tag = database.get_tag("PatientName")
        self.assertIsInstance(tag, database.classes["tag"])

        # Testing that None is returned if the tag does not exist
        tag = database.get_tag("Test")
        self.assertIsNone(tag)

    def test_remove_tag(self):
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

        # TODO Testing tag table removal

    def test_add_scan(self):
        """
        Tests the method adding a scan
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")
        scan = database.get_scan("scan1")
        self.assertIsInstance(scan, database.classes["path"])

    def test_get_current_value(self):
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

        # Testing that None is returned if the value does not exist
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        database.add_value("scan1", "PatientName", "test")

        # Testing that the value is returned if it exists
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

    def test_get_initial_value(self):
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

        # Testing that None is returned if the value does not exist
        value = database.get_initial_value("scan1", "PatientName")
        self.assertIsNone(value)

        database.add_value("scan1", "PatientName", "test")

        # Testing that the value is returned if it exists
        value = database.get_initial_value("scan1", "PatientName")
        self.assertEqual(value, "test")

    def test_add_value(self):
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
        database.add_tag("SequenceName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, None)
        database.add_tag("BandWidth", True, TAG_ORIGIN_RAW, TAG_TYPE_FLOAT, None, None, None)

        # Adding values
        database.add_value("scan1", "PatientName", "test")
        database.add_value("scan2", "PatientName", "value")
        database.add_value("scan1", "SequenceName", "seq")

        # Testing not crashing when the tag does not exist
        database.add_value("scan1", "NotExisting", "none")

        # Testing not crashing when the scan does not exist
        database.add_value("scan3", "SequenceName", "none")

        # Testing not crashing when both do not exist
        database.add_value("scan3", "NotExisting", "none")

        # Testing values actually added
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        value = database.get_current_value("scan2", "PatientName")
        self.assertEqual(value, "value")
        value = database.get_current_value("scan1", "SequenceName")
        self.assertEqual(value, "seq")

        # Test value override
        database.add_value("scan1", "PatientName", "test2")
        database.save_modifications()
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

    def test_save_modifications(self):
        """
        Tests the method saving the modifications
        """

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_value("scan1", "PatientName", "test")
        shutil.copy(path, os.path.join(".", "test_origin.db"))
        shutil.copy(database.temp_file, os.path.join(".", "test_temp.db"))
        database.save_modifications()
        shutil.copy(path, os.path.join(".", "test_origin_after_commit.db"))

        # Testing that the original file is empty
        database = Database(os.path.join(".", "test_origin.db"))
        tag = database.get_tag("PatientName")
        self.assertIsNone(tag)
        value = database.get_current_value("scan2", "PatientName")
        self.assertIsNone(value)

        # Testing that the temporary file was updated
        database = Database(os.path.join(".", "test_temp.db"))
        tag = database.get_tag("PatientName")
        self.assertIsInstance(tag, database.classes["tag"])
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing that the temporary file was updated
        database = Database(os.path.join(".", "test_origin_after_commit.db"))
        tag = database.get_tag("PatientName")
        self.assertIsInstance(tag, database.classes["tag"])
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing that the save_modifications method uptades the original file
        os.remove(os.path.join(".", "test_origin.db"))
        os.remove(os.path.join(".", "test_temp.db"))
        os.remove(os.path.join(".", "test_origin_after_commit.db"))

    def test_remove_scan(self):
        """
        Tests the method that removes a scan
        """

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_value("scan1", "PatientName", "test")
        value = database.get_current_value("scan1", "PatientName")
        database.remove_scan("scan1")

        # Testing that the scan is removed from Path table
        scan = database.get_scan("scan1")
        self.assertIsNone(scan)

        # Testing that the values associated are removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

    def test_get_scan(self):
        """
        Tests the method adding a scan
        """
        global path

        if os.path.exists(path):
            os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def753")

        # Testing that a scan is returned if it exists
        scan = database.get_scan("scan1")
        self.assertIsInstance(scan, database.classes["path"])

        # Testing that None is returned if the scan does not exist
        scan = database.get_scan("scan3")
        self.assertIsNone(scan)

if __name__ == '__main__':
    unittest.main(exit=False)
    os.remove(path)