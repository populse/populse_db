import os
import shutil
import unittest
import tempfile
from datetime import datetime

from populse_db.Database import Database
from populse_db.DatabaseModel import createDatabase, TAG_ORIGIN_RAW, TAG_TYPE_STRING, TAG_TYPE_FLOAT, TAG_UNIT_MHZ, \
    TAG_TYPE_INTEGER, TAG_TYPE_TIME, TAG_TYPE_DATETIME, \
    TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT

temp_folder = tempfile.mkdtemp()
path = os.path.relpath(os.path.join(temp_folder, "test.db"))


class TestDatabaseMethods(unittest.TestCase):

    def test_database_creation(self):
        """
        Tests the database creation
        """
        global path

        # Testing the creation of the database file
        os.remove(path)
        createDatabase(path)
        self.assertTrue(os.path.exists(path))

    def test_database_constructor(self):
        """
        Tests the database constructor
        """
        global path

        # Testing without the database file existing
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

        # Testing with a first tag
        os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Checking the tag properties
        tag = database.get_tag("PatientName")
        self.assertEqual(tag.name, "PatientName")
        self.assertTrue(tag.visible)
        self.assertEqual(tag.origin, TAG_ORIGIN_RAW)
        self.assertEqual(tag.type, TAG_TYPE_STRING)
        self.assertIsNone(tag.unit)
        self.assertIsNone(tag.default_value)
        self.assertEqual(tag.description, "Name of the patient")

        # Testing with a tag that already exists
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing with all tag types
        database.add_tag("BandWidth", True, TAG_ORIGIN_RAW, TAG_TYPE_FLOAT, TAG_UNIT_MHZ, None, None)
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("AcquisitionTime", True, TAG_ORIGIN_RAW, TAG_TYPE_TIME, None, None, None)
        database.add_tag("AcquisitionDate", True, TAG_ORIGIN_RAW, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Testing with wrong parameters
        database.add_tag(None, True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag("PatientName", None, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag("PatientName", True, "wrong_origin", TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, "wrong_type", None, None, None)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, "invalid_unit", None, None)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, 1, None)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, 1.5)

        # TODO Testing tag table or creation

    def test_remove_tag(self):
        """
        Tests the method removing a tag
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("SequenceName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Adding scans
        database.add_scan("scan1", "checksum")
        database.add_scan("scan2", "checksum")

        # Adding values
        database.add_value("scan1", "PatientName", "Guerbet", "Guerbet")
        database.add_value("scan1", "SequenceName", "RARE", "RARE")
        database.add_value("scan1", "Dataset dimensions", [1, 2], [1, 2])

        # Removing tag
        database.remove_tag("PatientName")
        database.remove_tag("Dataset dimensions")

        # Testing that the tag does not exist anymore
        tag = database.get_tag("PatientName")
        self.assertIsNone(tag)
        tag = database.get_tag("Dataset dimensions")
        self.assertIsNone(tag)

        # Testing that the tag values are removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", "SequenceName")
        self.assertEqual(value, "RARE")
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertIsNone(value)

        # Testing with a tag not existing
        database.remove_tag("NotExisting")
        database.remove_tag("Dataset dimension")

        # Testing with wrong parameter
        database.remove_tag(1)
        database.remove_tag(None)

        # TODO Testing tag table or column removal

    def test_get_tag(self):
        """
        Tests the method giving the Tag table object of a tag
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding a tag
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing that the tag is returned if it exists
        tag = database.get_tag("PatientName")
        self.assertIsInstance(tag, database.classes["tag"])

        # Testing that None is returned if the tag does not exist
        tag = database.get_tag("Test")
        self.assertIsNone(tag)

    def test_is_tag_list(self):
        """
        Tests the method telling if the tag has a list type or not
        """

        global path

        os.remove(path)
        database = Database(path)

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Testing that the correct boolean is returned
        tag_list = database.is_tag_list("PatientName")
        self.assertFalse(tag_list)
        tag_list = database.is_tag_list("Test")
        self.assertFalse(tag_list)
        tag_list = database.is_tag_list("Dataset dimensions")
        self.assertTrue(tag_list)

    def test_get_current_value(self):
        """
        Tests the method giving the current value, given a tag and a scan
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scans
        database.add_scan("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Grids spacing", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_FLOAT, None, None, None)

        # Adding values
        database.add_value("scan1", "PatientName", "test", "test")
        database.add_value("scan1", "Bits per voxel", 10, 10)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        database.add_value("scan1", "Grids spacing", [0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])
        database.add_value("scan2", "Grids spacing", [0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])

        # Testing that the value is returned if it exists
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 10)
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Grids spacing")
        self.assertEqual(value, [0.234375, 0.234375, 0.4])

        # Testing when not existing
        value = database.get_current_value("scan3", "PatientName")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", "NotExisting")
        self.assertIsNone(value)
        value = database.get_current_value("scan3", "NotExisting")
        self.assertIsNone(value)
        value = database.get_current_value("scan2", "Grids spacing")
        self.assertIsNone(value)

        # Testing with wrong parameters
        value = database.get_current_value(1, "Grids spacing")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", None)
        self.assertIsNone(value)
        value = database.get_current_value(3.5, None)
        self.assertIsNone(value)

    def test_get_initial_value(self):
        """
        Tests the method giving the initial value, given a tag and a scan
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scans
        database.add_scan("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag("Grids spacing", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_FLOAT, None, None, None)

        # Adding values
        database.add_value("scan1", "PatientName", "test", "test")
        database.add_value("scan1", "Bits per voxel", 50, 50)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        database.add_value("scan1", "Grids spacing", [0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])

        # Testing that the value is returned if it exists
        value = database.get_initial_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        value = database.get_initial_value("scan1", "Bits per voxel")
        self.assertEqual(value, 50)
        value = database.get_initial_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        value = database.get_initial_value("scan1", "Grids spacing")
        self.assertEqual(value, [0.234375, 0.234375, 0.4])

        # Testing when not existing
        value = database.get_initial_value("scan3", "PatientName")
        self.assertIsNone(value)
        value = database.get_initial_value("scan1", "NotExisting")
        self.assertIsNone(value)
        value = database.get_initial_value("scan3", "NotExisting")
        self.assertIsNone(value)

        # Testing with wrong parameters
        value = database.get_initial_value(1, "Grids spacing")
        self.assertIsNone(value)
        value = database.get_initial_value("scan1", None)
        self.assertIsNone(value)
        value = database.get_initial_value(3.5, None)
        self.assertIsNone(value)

    def test_is_value_modified(self):
        """
        Tests the method telling if the value has been modified or not
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Adding tag
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding a value
        database.add_value("scan1", "PatientName", "test", "test")

        # Testing that the value has not been modified
        is_modified = database.is_value_modified("scan1", "PatientName")
        self.assertFalse(is_modified)

        # Value modified
        database.set_value("scan1", "PatientName", "test2")

        # Testing that the value has been modified
        is_modified = database.is_value_modified("scan1", "PatientName")
        self.assertTrue(is_modified)

        # Testing with values not existing
        is_modified = database.is_value_modified("scan2", "PatientName")
        self.assertFalse(is_modified)
        is_modified = database.is_value_modified("scan1", "NotExisting")
        self.assertFalse(is_modified)
        is_modified = database.is_value_modified("scan2", "NotExisting")
        self.assertFalse(is_modified)

        # Testing with wrong parameters
        value = database.is_value_modified(1, "Grids spacing")
        self.assertFalse(value)
        value = database.is_value_modified("scan1", None)
        self.assertFalse(value)
        value = database.is_value_modified(3.5, None)
        self.assertFalse(value)

    def test_set_value(self):
        """
        Tests the method setting a value
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("AcquisitionDate", True, TAG_ORIGIN_RAW, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag("AcquisitionTime", True, TAG_ORIGIN_RAW, TAG_TYPE_TIME, None, None, None)

        # Adding values and changing it
        database.add_value("scan1", "PatientName", "test", "test")
        database.set_value("scan1", "PatientName", "test2")

        database.add_value("scan1", "Bits per voxel", 1, 1)
        database.set_value("scan1", "Bits per voxel", 2)

        date = datetime(2014, 2, 11, 8, 5, 7)
        database.add_value("scan1", "AcquisitionDate", date, date)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)
        date = datetime(2015, 2, 11, 8, 5, 7)
        database.set_value("scan1", "AcquisitionDate", date)

        time = datetime(2014, 2, 11, 0, 2, 20).time()
        database.add_value("scan1", "AcquisitionTime", time, time)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)
        time = datetime(2014, 2, 11, 15, 24, 20).time()
        database.set_value("scan1", "AcquisitionTime", time)

        # Testing that the values are actually set
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test2")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)
        database.set_value("scan1", "PatientName", None)
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        # Testing when not existing
        database.set_value("scan3", "PatientName", None)
        database.set_value("scan1", "NotExisting", None)
        database.set_value("scan3", "NotExisting", None)

        # Testing with wrong types
        database.set_value("scan1", "Bits per voxel", "test")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)
        database.set_value("scan1", "Bits per voxel", 35.8)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)

        # Testing with wrong parameters
        database.set_value(1, "Grids spacing", "2")
        database.set_value("scan1", None, "1")
        database.set_value(1, None, True)

    def test_reset_value(self):
        """
        Tests the method resetting a value
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Adding values and changing it
        database.add_value("scan1", "PatientName", "test", "test")
        database.set_value("scan1", "PatientName", "test2")

        database.add_value("scan1", "Bits per voxel", 5, 5)
        database.set_value("scan1", "Bits per voxel", 15)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 15)

        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.set_value("scan1", "Dataset dimensions", [1, 2, 3, 4])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [1, 2, 3, 4])

        # Reset of the values
        database.reset_value("scan1", "PatientName")
        database.reset_value("scan1", "Bits per voxel")
        database.reset_value("scan1", "Dataset dimensions")

        # Testing when not existing
        database.reset_value("scan3", "PatientName")
        database.reset_value("scan1", "NotExisting")
        database.reset_value("scan3", "NotExisting")

        # Testing that the values are actually reset
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 5)
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

        # Testing with wrong parameters
        database.reset_value(1, "Grids spacing")
        database.reset_value("scan1", None)
        database.reset_value(3.5, None)

    def test_remove_value(self):
        """
        Tests the method removing a value
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Adding values
        database.add_value("scan1", "PatientName", "test", "test")
        database.add_value("scan1", "Bits per voxel", "space_tag", "space_tag")
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

        # Removing values
        database.remove_value("scan1", "PatientName")
        database.remove_value("scan1", "Bits per voxel")
        database.remove_value("scan1", "Dataset dimensions")

        # Testing when not existing
        database.remove_value("scan3", "PatientName")
        database.remove_value("scan1", "NotExisting")
        database.remove_value("scan3", "NotExisting")

        # Testing that the values are actually removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertIsNone(value)
        value = database.get_initial_value("scan1", "Dataset dimensions")
        self.assertIsNone(value)

    def test_check_type_value(self):
        """
        Tests the method checking the validity of incoming values
        """
        global path

        os.remove(path)

        database = Database(path)
        is_valid = database.check_type_value("string", TAG_TYPE_STRING)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1, TAG_TYPE_STRING)
        self.assertFalse(is_valid)
        is_valid = database.check_type_value(None, TAG_TYPE_STRING)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1, TAG_TYPE_INTEGER)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1, TAG_TYPE_FLOAT)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1.5, TAG_TYPE_FLOAT)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(None, None)
        self.assertFalse(is_valid)
        is_valid = database.check_type_value([1.5], TAG_TYPE_LIST_FLOAT)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1.5, TAG_TYPE_LIST_FLOAT)
        self.assertFalse(is_valid)
        is_valid = database.check_type_value([1.5, "test"], TAG_TYPE_LIST_FLOAT)
        self.assertFalse(is_valid)

    def test_add_value(self):
        """
        Tests the method adding a value
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scans
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def758")

        # Adding tags
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("BandWidth", True, TAG_ORIGIN_RAW, TAG_TYPE_FLOAT, None, None, None)
        database.add_tag("AcquisitionTime", True, TAG_ORIGIN_RAW, TAG_TYPE_TIME, None, None, None)
        database.add_tag("AcquisitionDate", True, TAG_ORIGIN_RAW, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag("Grids spacing", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_FLOAT, None, None, None)

        # Adding values
        database.add_value("scan1", "PatientName", "test", None)
        database.add_value("scan2", "BandWidth", 35.5, 35.5)
        database.add_value("scan1", "Bits per voxel", 1, 1)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        database.add_value("scan2", "Grids spacing", [0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])

        # Testing when not existing
        database.add_value("scan1", "NotExisting", "none", "none")
        database.add_value("scan3", "SequenceName", "none", "none")
        database.add_value("scan3", "NotExisting", "none", "none")
        database.add_value("scan1", "BandWidth", 45, 45)
        date = datetime(2014, 2, 11, 8, 5, 7)
        database.add_value("scan1", "AcquisitionDate", date, date)
        time = datetime(2014, 2, 11, 0, 2, 2).time()
        database.add_value("scan1", "AcquisitionTime", time, time)

        # Testing that the values are actually added
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        value = database.get_initial_value("scan1", "PatientName")
        self.assertIsNone(value)
        value = database.get_current_value("scan2", "BandWidth")
        self.assertEqual(value, 35.5)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 1)
        value = database.get_current_value("scan1", "BandWidth")
        self.assertEqual(value, 45)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        value = database.get_current_value("scan2", "Grids spacing")
        self.assertEqual(value, [0.234375, 0.234375, 0.4])

        # Test value override
        database.add_value("scan1", "PatientName", "test2", "test2")
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing with wrong types
        database.add_value("scan2", "Bits per voxel", "space_tag", "space_tag")
        value = database.get_current_value("scan2", "Bits per voxel")
        self.assertIsNone(value)
        database.add_value("scan2", "Bits per voxel", 35.5, 35.5)
        value = database.get_current_value("scan2", "Bits per voxel")
        self.assertIsNone(value)
        database.add_value("scan1", "BandWidth", "test", "test")
        value = database.get_current_value("scan1", "BandWidth")
        self.assertEqual(value, 45)

        # Testing with wrong parameters
        database.add_value(1, "Grids spacing", "2", "2")
        database.add_value("scan1", None, "1", "1")
        database.add_value("scan1", "PatientName", None, None)
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        database.add_value(1, None, True, False)

    def test_get_scan(self):
        """
        Tests the method giving the Path table object of a scan
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Testing that a scan is returned if it exists
        scan = database.get_scan("scan1")
        self.assertIsInstance(scan, database.classes["path"])

        # Testing that None is returned if the scan does not exist
        scan = database.get_scan("scan3")
        self.assertIsNone(scan)

        # Testing with wrong parameter
        scan = database.get_scan(None)
        self.assertIsNone(scan)
        scan = database.get_scan(1)
        self.assertIsNone(scan)

    def test_remove_scan(self):
        """
        Tests the method removing a scan
        """

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Adding tag
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding value
        database.add_value("scan1", "PatientName", "test", "test")

        # Removing scan
        database.remove_scan("scan1")

        # Testing that the scan is removed from Path table
        scan = database.get_scan("scan1")
        self.assertIsNone(scan)

        # Testing that the values associated are removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        # Testing with a scan not existing
        database.remove_scan("NotExisting")

    def test_add_scan(self):
        """
        Tests the method adding a scan
        """
        global path

        os.remove(path)
        database = Database(path)

        # Adding scan
        database.add_scan("scan1", "159abc")

        # Testing that the scan has been added
        scan = database.get_scan("scan1")
        self.assertIsInstance(scan, database.classes["path"])
        self.assertEqual(scan.checksum, "159abc")
        self.assertEqual(scan.name, "scan1")

        # Testing when trying to add a scan that already exists
        database.add_scan("scan1", "anotherChecksum")
        scan = database.get_scan("scan1")
        self.assertEqual(scan.checksum, "159abc")

if __name__ == '__main__':
    createDatabase(path)
    unittest.main(exit=False)
    os.remove(path)
    os.rmdir(os.path.dirname(path))
