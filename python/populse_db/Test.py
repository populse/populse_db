import os
import shutil
import unittest
from datetime import datetime

from populse_db.Database import Database
from populse_db.DatabaseModel import createDatabase, TAG_ORIGIN_RAW, TAG_TYPE_STRING, TAG_TYPE_FLOAT, TAG_UNIT_MHZ, \
    TAG_TYPE_INTEGER, TAG_TYPE_TIME, TAG_TYPE_DATETIME, \
    TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT

path = os.path.relpath(os.path.join(".", "test.db"))


class TestDatabaseMethods(unittest.TestCase):

    def test_database_creation(self):
        """
        Tests the database creation
        """
        global path

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

        # Testing with a tag that elready exists
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing all tag types
        database.add_tag("BandWidth", True, TAG_ORIGIN_RAW, TAG_TYPE_FLOAT, TAG_UNIT_MHZ, None, None)
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("AcquisitionTime", True, TAG_ORIGIN_RAW, TAG_TYPE_TIME, None, None, None)
        database.add_tag("AcquisitionDate", True, TAG_ORIGIN_RAW, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Testing with wrong parameters
        database.add_tag(None, None, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)

        # TODO Testing tag table creation

    def test_get_tag(self):
        """
        Tests the method giving the Tag object of a tag
        """
        global path

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

        os.remove(path)
        database = Database(path)
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("SequenceName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, None)
        database.add_scan("scan1", "checksum")
        database.add_scan("scan2", "checksum")
        database.add_value("scan1", "PatientName", "Guerbet")
        database.add_value("scan1", "SequenceName", "RARE")
        database.save_modifications()
        database.remove_tag("PatientName")

        # Testing that the tag does not exist anymore
        tag = database.get_tag("PatientName")
        self.assertIsNone(tag)

        # Testing that the values are removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", "SequenceName")
        self.assertEqual(value, "RARE")

        # Testing with a tag not existing
        database.remove_tag("NotExisting")

        # Testing with list tag
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_value("scan1", "Dataset dimensions", [1, 2])
        tag = database.get_tag("Dataset dimensions")
        self.assertIsInstance(tag, database.classes["tag"])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [1, 2])
        database.remove_tag("Dataset dimension")
        database.remove_tag("Dataset dimensions")
        tag = database.get_tag("Dataset dimensions")
        self.assertIsNone(tag)
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertIsNone(value)

        # TODO Testing tag table removal

    def test_add_scan(self):
        """
        Tests the method adding a scan
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        scan = database.get_scan("scan1")
        self.assertIsInstance(scan, database.classes["path"])

        # Testing when trying to add a scan that already exists
        database.add_scan("scan1", "anotherChecksum")
        scan = database.get_scan("scan1")
        self.assertEqual(scan.checksum, "159abc")

    def test_get_current_value(self):
        """
        Tests the method giving the current value, given a tag and a scan
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing that None is returned if the value does not exist
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        database.add_value("scan1", "PatientName", "test")

        # Testing that the value is returned if it exists
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing when not existing
        value = database.get_current_value("scan3", "PatientName")
        self.assertIsNone(value)
        value = database.get_current_value("scan1", "NotExisting")
        self.assertIsNone(value)
        value = database.get_current_value("scan3", "NotExisting")
        self.assertIsNone(value)

        # Testing with tag containing spaces
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_value("scan1", "Bits per voxel", 10)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 10)

        # Testing with list tag
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.add_tag("Grids spacing", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_FLOAT, None, None, None)
        database.add_value("scan1", "Grids spacing", [0.234375, 0.234375, 0.4])
        value = database.get_current_value("scan1", "Grids spacing")
        self.assertEqual(value, [0.234375, 0.234375, 0.4])

        # Trying with wrong params
        database.add_value("scan2", "Grids spacing", [0.234375, 0.234375, 0.4])
        value = database.get_current_value("scan2", "Grids spacing")
        self.assertIsNone(value)

    def test_get_initial_value(self):
        """
        Tests the method giving the initial value, given a tag and a scan
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing that None is returned if the value does not exist
        value = database.get_initial_value("scan1", "PatientName")
        self.assertIsNone(value)

        database.add_value("scan1", "PatientName", "test")

        # Testing that the value is returned if it exists
        value = database.get_initial_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing when not existing
        value = database.get_initial_value("scan3", "PatientName")
        self.assertIsNone(value)
        value = database.get_initial_value("scan1", "NotExisting")
        self.assertIsNone(value)
        value = database.get_initial_value("scan3", "NotExisting")
        self.assertIsNone(value)

        # Testing with tag containing spaces
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_value("scan1", "Bits per voxel", 50)
        value = database.get_initial_value("scan1", "Bits per voxel")
        self.assertEqual(value, 50)

        # Testing with list tag
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3])
        value = database.get_initial_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.add_tag("Grids spacing", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_FLOAT, None, None, None)
        database.add_value("scan1", "Grids spacing", [0.234375, 0.234375, 0.4])
        value = database.get_initial_value("scan1", "Grids spacing")
        self.assertEqual(value, [0.234375, 0.234375, 0.4])

        # Trying with wrong params
        database.add_value("scan2", "Grids spacing", [0.234375, 0.234375, 0.4])
        value = database.get_initial_value("scan2", "Grids spacing")
        self.assertIsNone(value)

    def test_add_value(self):
        """
        Tests the method adding a value
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_scan("scan2", "def758")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding the value
        database.add_value("scan1", "PatientName", "test")

        # Testing not crashing when the tag does not exist
        database.add_value("scan1", "NotExisting", "none")

        # Testing not crashing when the scan does not exist
        database.add_value("scan3", "SequenceName", "none")

        # Testing not crashing when both do not exist
        database.add_value("scan3", "NotExisting", "none")

        # Testing values actually added
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Test value override
        database.add_value("scan1", "PatientName", "test2")
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing with tag containing spaces
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_value("scan1", "Bits per voxel", 1)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 1)

        # Testing with wrong types
        database.add_value("scan2", "Bits per voxel", "space_tag")
        value = database.get_current_value("scan2", "Bits per voxel")
        self.assertIsNone(value)
        database.add_value("scan2", "Bits per voxel", 35.5)
        value = database.get_current_value("scan2", "Bits per voxel")
        self.assertIsNone(value)

        # Testing with Float tag
        database.add_tag("BandWidth", True, TAG_ORIGIN_RAW, TAG_TYPE_FLOAT, None, None, None)
        database.add_value("scan2", "BandWidth", 35.5)
        value = database.get_current_value("scan2", "BandWidth")
        self.assertEqual(value, 35.5)
        database.add_value("scan1", "BandWidth", "test")
        value = database.get_current_value("scan1", "BandWidth")
        self.assertIsNone(value)
        database.add_value("scan1", "BandWidth", 45)
        value = database.get_current_value("scan1", "BandWidth")
        self.assertEqual(value, 45)

        # Testing with datetime tag
        database.add_tag("AcquisitionDate", True, TAG_ORIGIN_RAW, TAG_TYPE_DATETIME, None, None, None)
        date = datetime(2014, 2, 11, 8, 5, 7)
        database.add_value("scan1", "AcquisitionDate", date)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)

        # Testing with time tag
        database.add_tag("AcquisitionTime", True, TAG_ORIGIN_RAW, TAG_TYPE_TIME, None, None, None)
        time = datetime(2014, 2, 11, 0, 2, 2).time()
        database.add_value("scan1", "AcquisitionTime", time)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)

        # Testing with list tag
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.add_tag("Grids spacing", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_FLOAT, None, None, None)
        database.add_value("scan2", "Grids spacing", [0.234375, 0.234375, 0.4])
        value = database.get_current_value("scan2", "Grids spacing")
        self.assertEqual(value, [0.234375, 0.234375, 0.4])

    def test_remove_value(self):
        """
        Tests the method removing a value
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding the value
        database.add_value("scan1", "PatientName", "test")

        # Removing the value
        database.remove_value("scan1", "PatientName")

        # Trying when not existing
        database.remove_value("scan3", "PatientName")
        database.remove_value("scan1", "NotExisting")
        database.remove_value("scan3", "NotExisting")

        # Testing that the value is actually removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        # Testing with spaces in tag
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_value("scan1", "Bits per voxel", "space_tag")
        database.remove_value("scan1", "Bits per voxel")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertIsNone(value)

        # Testing with list tag
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.remove_value("scan1", "Dataset dimensions")
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertIsNone(value)
        value = database.get_initial_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

    def test_save_modifications(self):
        """
        Tests the method saving the modifications
        """

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
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

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_value("scan1", "PatientName", "test")
        database.remove_scan("scan1")

        # Testing that the scan is removed from Path table
        scan = database.get_scan("scan1")
        self.assertIsNone(scan)

        # Testing that the values associated are removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        # Testing with a scan not existing
        database.remove_scan("NotExisting")

    def test_get_scan(self):
        """
        Tests the method giving the Path object of a scan
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")

        # Testing that a scan is returned if it exists
        scan = database.get_scan("scan1")
        self.assertIsInstance(scan, database.classes["path"])

        # Testing that None is returned if the scan does not exist
        scan = database.get_scan("scan3")
        self.assertIsNone(scan)

    def test_reset_value(self):
        """
        Tests the method resetting a value
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding a value and changing it
        database.add_value("scan1", "PatientName", "test")
        database.set_value("scan1", "PatientName", "test2")

        # Resetting the value
        database.reset_value("scan1", "PatientName")

        # Trying when not existing
        database.reset_value("scan3", "PatientName")
        database.reset_value("scan1", "NotExisting")
        database.reset_value("scan3", "NotExisting")

        # Testing that the value is actually resetted
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing with tag containing spaces
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_value("scan1", "Bits per voxel", 5)
        database.set_value("scan1", "Bits per voxel", 15)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 15)
        database.reset_value("scan1", "Bits per voxel")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 5)

        # Testing with list tag
        database.add_tag("Dataset dimensions", True, TAG_ORIGIN_RAW, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_value("scan1", "Dataset dimensions", [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.set_value("scan1", "Dataset dimensions", [1, 2, 3, 4])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [1, 2, 3, 4])
        database.reset_value("scan1", "Dataset dimensions")
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

    def test_set_value(self):
        """
        Tests the method setting a value
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding a value and changing it
        database.add_value("scan1", "PatientName", "test")
        database.set_value("scan1", "PatientName", "test2")

        # Trying when not existing
        database.set_value("scan3", "PatientName", None)
        database.set_value("scan1", "NotExisting", None)
        database.set_value("scan3", "NotExisting", None)

        # Testing that the value is actually resetted
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test2")

        # Testing with tag containing spaces
        database.add_tag("Bits per voxel", True, TAG_ORIGIN_RAW, TAG_TYPE_INTEGER, None, None, None)
        database.add_value("scan1", "Bits per voxel", 1)
        database.set_value("scan1", "Bits per voxel", 2)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)

        # Testing with wrong types
        database.set_value("scan1", "Bits per voxel", "test")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)
        database.set_value("scan1", "Bits per voxel", 35.8)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)

        # Testing with datetime tag
        database.add_tag("AcquisitionDate", True, TAG_ORIGIN_RAW, TAG_TYPE_DATETIME, None, None, None)
        date = datetime(2014, 2, 11, 8, 5, 7)
        database.add_value("scan1", "AcquisitionDate", date)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)
        date = datetime(2015, 2, 11, 8, 5, 7)
        database.set_value("scan1", "AcquisitionDate", date)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)

        # Testing with time tag
        database.add_tag("AcquisitionTime", True, TAG_ORIGIN_RAW, TAG_TYPE_TIME, None, None, None)
        time = datetime(2014, 2, 11, 0, 2, 20).time()
        database.add_value("scan1", "AcquisitionTime", time)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)
        time = datetime(2014, 2, 11, 15, 24, 20).time()
        database.set_value("scan1", "AcquisitionTime", time)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)

    def test_is_value_modified(self):
        """
        Tests the method telling if the value has been modified
        """
        global path

        os.remove(path)
        database = Database(path)
        database.add_scan("scan1", "159abc")
        database.add_tag("PatientName", True, TAG_ORIGIN_RAW, TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding a value and changing it
        database.add_value("scan1", "PatientName", "test")

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


if __name__ == '__main__':
    createDatabase(path)
    unittest.main(exit=False)
    os.remove(path)
