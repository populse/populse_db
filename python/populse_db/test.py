import os
import shutil
import unittest
import tempfile
from datetime import datetime

from populse_db.database import Database
from populse_db.database_model import (create_database, TAG_ORIGIN_BUILTIN,
                                       TAG_TYPE_STRING, TAG_TYPE_FLOAT,
                                       TAG_UNIT_MHZ, TAG_TYPE_INTEGER,
                                       TAG_TYPE_TIME, TAG_TYPE_DATETIME,
                                       TAG_TYPE_LIST_INTEGER,
                                       TAG_TYPE_LIST_FLOAT, PATH_TABLE)


class TestDatabaseMethods(unittest.TestCase):
    """
    Class executing the unit tests of populse_db
    """

    def setUp(self):
        """
        Called before every unit test
        Creates a temporary folder containing the database file that will be used for the test
        """

        self.temp_folder = tempfile.mkdtemp()
        self.path = os.path.join(self.temp_folder, "test.db")
        self.string_engine = 'sqlite:///' + self.path

    def tearDown(self):
        """
        Called after every unit test
        Deletes the temporary folder created for the test
        """

        shutil.rmtree(self.temp_folder)
        
        
    def test_database_creation(self):
        """
        Tests the database creation
        """

        # Testing the creation of the database file
        create_database(self.string_engine)
        self.assertTrue(os.path.exists(self.path))

    def test_database_constructor(self):
        """
        Tests the database constructor
        """

        # Testing without the database file existing
        Database(self.string_engine)
        self.assertTrue(os.path.exists(self.path))

        # Testing with the database file existing
        os.remove(self.path)
        create_database(self.string_engine)
        Database(self.string_engine)
        self.assertTrue(os.path.exists(self.path))

    def test_add_tag(self):
        """
        Tests the method adding a tag
        """

        # Testing with a first tag
        database = Database(self.string_engine)
        return_value = database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        self.assertEqual(return_value, 0)

        # Checking the tag properties
        tag = database.get_tag("PatientName")
        self.assertEqual(tag.name, "PatientName")
        self.assertEqual(tag.origin, TAG_ORIGIN_BUILTIN)
        self.assertEqual(tag.type, TAG_TYPE_STRING)
        self.assertIsNone(tag.unit)
        self.assertIsNone(tag.default_value)
        self.assertEqual(tag.description, "Name of the patient")

        # Testing with a tag that already exists
        return_value = database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,TAG_TYPE_STRING, None, None, "Name of the patient")
        self.assertEqual(return_value, 1)

        # Testing with all tag types
        database.add_tag("BandWidth", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_FLOAT, TAG_UNIT_MHZ, None, None)
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, "with space")
        database.add_tag(
            "AcquisitionTime", TAG_ORIGIN_BUILTIN, TAG_TYPE_TIME, None, None, None)
        database.add_tag(
            "AcquisitionDate", TAG_ORIGIN_BUILTIN, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)

        database.add_tag(
            "Bitspervoxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, "without space")
        self.assertEqual(database.get_tag("Bitspervoxel").description, "without space")
        self.assertEqual(database.get_tag("Bits per voxel").description, "with space")

        # Testing with wrong parameters
        return_value = database.add_tag(None, TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)
        self.assertEqual(return_value, 2)
        return_value = database.add_tag("Patient Name", True, TAG_TYPE_LIST_INTEGER, None, None, None)
        self.assertEqual(return_value, 3)
        return_value = database.add_tag("Patient Name", TAG_ORIGIN_BUILTIN, None, None, None, None)
        self.assertEqual(return_value, 4)
        return_value = database.add_tag("Patient Name", TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, "unit", None, None)
        self.assertEqual(return_value, 5)
        return_value = database.add_tag("Patient Name", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, True, None)
        self.assertEqual(return_value, 6)
        return_value = database.add_tag("Patient Name", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, 1.5)
        self.assertEqual(return_value, 7)

        # TODO Testing tag table or tag column creation

    def test_remove_tag(self):
        """
        Tests the method removing a tag
        """

        database = Database(self.string_engine)

        # Adding tags
        return_value = database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        self.assertEqual(return_value, 0)
        database.add_tag(
            "SequenceName", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, None)
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Adding scans
        database.add_path("scan1", "checksum")
        database.add_path("scan2", "checksum")

        # Adding values
        database.new_value("scan1", "PatientName", "Guerbet", "Guerbet")
        database.new_value("scan1", "SequenceName", "RARE", "RARE")
        database.new_value("scan1", "Dataset dimensions", [1, 2], [1, 2])

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
        return_value = database.remove_tag("NotExisting")
        self.assertEqual(return_value, 1)
        return_value = database.remove_tag("Dataset dimension")
        self.assertEqual(return_value, 1)

        # Testing with wrong parameter
        return_value = database.remove_tag(1)
        self.assertEqual(return_value, 2)
        return_value = database.remove_tag(None)
        self.assertEqual(return_value, 2)

        # TODO Testing tag table or tag column removal

    def test_get_tag(self):
        """
        Tests the method giving the Tag table object of a tag
        """

        database = Database(self.string_engine)

        # Adding a tag
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")

        # Testing that the tag is returned if it exists
        tag = database.get_tag("PatientName")
        self.assertIsNotNone(tag)

        # Testing that None is returned if the tag does not exist
        tag = database.get_tag("Test")
        self.assertIsNone(tag)

    def test_is_tag_list(self):
        """
        Tests the method telling if the tag has a list type or not
        """
        database = Database(self.string_engine)

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Testing that the correct boolean is returned
        tag_list = database.is_tag_list("PatientName")
        self.assertFalse(tag_list)
        #tag_list = database.is_tag_list("Test")
        #self.assertFalse(tag_list)
        tag_list = database.is_tag_list("Dataset dimensions")
        self.assertTrue(tag_list)

    def test_get_current_value(self):
        """
        Tests the method giving the current value, given a tag and a scan
        """

        database = Database(self.string_engine)

        # Adding scans
        database.add_path("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag(
            "Grids spacing", TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_FLOAT, None, None, None)

        # Adding values
        database.new_value("scan1", "PatientName", "test", "test")
        database.new_value("scan1", "Bits per voxel", 10, 10)
        database.new_value(
            "scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        database.new_value("scan1", "Grids spacing", [
                           0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])
        #database.new_value("scan2", "Grids spacing", [0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])

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

        database = Database(self.string_engine)

        # Adding scans
        database.add_path("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag(
            "Grids spacing", TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_FLOAT, None, None, None)

        # Adding values
        database.new_value("scan1", "PatientName", "test", "test")
        database.new_value("scan1", "Bits per voxel", 50, 50)
        database.new_value(
            "scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        database.new_value("scan1", "Grids spacing", [
                           0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])

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

        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "159abc")

        # Adding tag
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding a value
        database.new_value("scan1", "PatientName", "test", "test")

        # Testing that the value has not been modified
        is_modified = database.is_value_modified("scan1", "PatientName")
        self.assertFalse(is_modified)

        # Value modified
        database.set_current_value("scan1", "PatientName", "test2")

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

        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag(
            "AcquisitionDate", TAG_ORIGIN_BUILTIN, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag(
            "AcquisitionTime", TAG_ORIGIN_BUILTIN, TAG_TYPE_TIME, None, None, None)

        # Adding values and changing it
        database.new_value("scan1", "PatientName", "test", "test")
        database.set_current_value("scan1", "PatientName", "test2")

        database.new_value("scan1", "Bits per voxel", 1, 1)
        database.set_current_value("scan1", "Bits per voxel", 2)

        date = datetime(2014, 2, 11, 8, 5, 7)
        database.new_value("scan1", "AcquisitionDate", date, date)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)
        date = datetime(2015, 2, 11, 8, 5, 7)
        database.set_current_value("scan1", "AcquisitionDate", date)

        time = datetime(2014, 2, 11, 0, 2, 20).time()
        database.new_value("scan1", "AcquisitionTime", time, time)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)
        time = datetime(2014, 2, 11, 15, 24, 20).time()
        database.set_current_value("scan1", "AcquisitionTime", time)

        # Testing that the values are actually set
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test2")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)
        value = database.get_current_value("scan1", "AcquisitionDate")
        self.assertEqual(value, date)
        value = database.get_current_value("scan1", "AcquisitionTime")
        self.assertEqual(value, time)
        database.set_current_value("scan1", "PatientName", None)
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        # Testing when not existing
        return_value = database.set_current_value("scan3", "PatientName", None)
        self.assertEqual(return_value, 2)
        return_value = database.set_current_value("scan1", "NotExisting", None)
        self.assertEqual(return_value, 1)
        return_value = database.set_current_value("scan3", "NotExisting", None)
        self.assertEqual(return_value, 1)

        # Testing with wrong types
        return_value = database.set_current_value("scan1", "Bits per voxel", "test")
        self.assertEqual(return_value, 3)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)
        return_value = database.set_current_value("scan1", "Bits per voxel", 35.8)
        self.assertEqual(return_value, 3)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 2)

        # Testing with wrong parameters
        return_value = database.set_current_value(1, "Bits per voxel", "2")
        self.assertEqual(return_value, 2)
        return_value = database.set_current_value("scan1", None, "1")
        self.assertEqual(return_value, 1)
        return_value = database.set_current_value(1, None, True)
        self.assertEqual(return_value, 1)

    def test_reset_value(self):
        """
        Tests the method resetting a value
        """

        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Adding values and changing it
        database.new_value("scan1", "PatientName", "test", "test")
        database.set_current_value("scan1", "PatientName", "test2")

        database.new_value("scan1", "Bits per voxel", 5, 5)
        database.set_current_value("scan1", "Bits per voxel", 15)
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 15)

        database.new_value(
            "scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])
        database.set_current_value("scan1", "Dataset dimensions", [1, 2, 3, 4])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [1, 2, 3, 4])

        # Reset of the values
        database.reset_current_value("scan1", "PatientName")
        database.reset_current_value("scan1", "Bits per voxel")
        database.reset_current_value("scan1", "Dataset dimensions")

        # Testing when not existing
        return_value = database.reset_current_value("scan3", "PatientName")
        self.assertEqual(return_value, 2)
        return_value = database.reset_current_value("scan1", "NotExisting")
        self.assertEqual(return_value, 1)
        return_value = database.reset_current_value("scan3", "NotExisting")
        self.assertEqual(return_value, 1)

        # Testing that the values are actually reset
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        value = database.get_current_value("scan1", "Bits per voxel")
        self.assertEqual(value, 5)
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

        # Testing with wrong parameters
        return_value = database.reset_current_value(1, "Bits per voxel")
        self.assertEqual(return_value, 2)
        return_value = database.reset_current_value("scan1", None)
        self.assertEqual(return_value, 1)
        return_value = database.reset_current_value(3.5, None)
        self.assertEqual(return_value, 1)

    def test_remove_value(self):
        """
        Tests the method removing a value
        """

        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "159abc")

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)

        # Adding values
        database.new_value("scan1", "PatientName", "test", "test")
        database.new_value("scan1", "Bits per voxel", "space_tag", "space_tag")
        database.new_value(
            "scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        value = database.get_current_value("scan1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

        # Removing values
        database.remove_value("scan1", "PatientName")
        database.remove_value("scan1", "Bits per voxel")
        database.remove_value("scan1", "Dataset dimensions")

        # Testing when not existing
        return_value = database.remove_value("scan3", "PatientName")
        self.assertEqual(return_value, 2)
        return_value = database.remove_value("scan1", "NotExisting")
        self.assertEqual(return_value, 1)
        return_value = database.remove_value("scan3", "NotExisting")
        self.assertEqual(return_value, 1)

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

        database = Database(self.string_engine)
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
        is_valid = database.check_type_value(
            [1.5, "test"], TAG_TYPE_LIST_FLOAT)
        self.assertFalse(is_valid)

    def test_new_value(self):
        """
        Tests the method adding a value
        """

        database = Database(self.string_engine)

        # Adding scans
        database.add_path("scan1", "159abc")
        database.add_path("scan2", "def758")

        # Adding tags
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")
        database.add_tag(
            "Bits per voxel", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_tag(
            "BandWidth", TAG_ORIGIN_BUILTIN, TAG_TYPE_FLOAT, None, None, None)
        database.add_tag(
            "AcquisitionTime", TAG_ORIGIN_BUILTIN, TAG_TYPE_TIME, None, None, None)
        database.add_tag(
            "AcquisitionDate", TAG_ORIGIN_BUILTIN, TAG_TYPE_DATETIME, None, None, None)
        database.add_tag("Dataset dimensions",
                         TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_INTEGER, None, None, None)
        database.add_tag(
            "Grids spacing", TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_FLOAT, None, None, None)

        # Adding values
        database.new_value("scan1", "PatientName", "test", None)
        database.new_value("scan2", "BandWidth", 35.5, 35.5)
        database.new_value("scan1", "Bits per voxel", 1, 1)
        database.new_value(
            "scan1", "Dataset dimensions", [3, 28, 28, 3], [3, 28, 28, 3])
        database.new_value("scan2", "Grids spacing", [
                           0.234375, 0.234375, 0.4], [0.234375, 0.234375, 0.4])

        # Testing when not existing
        return_value = database.new_value("scan1", "NotExisting", "none", "none")
        self.assertEqual(return_value, 1)
        return_value = database.new_value("scan3", "SequenceName", "none", "none")
        self.assertEqual(return_value, 1)
        return_value = database.new_value("scan3", "NotExisting", "none", "none")
        self.assertEqual(return_value, 1)
        return_value = database.new_value("scan1", "BandWidth", 45, 45)
        self.assertEqual(return_value, 0)

        date = datetime(2014, 2, 11, 8, 5, 7)
        database.new_value("scan1", "AcquisitionDate", date, date)
        time = datetime(2014, 2, 11, 0, 2, 2).time()
        database.new_value("scan1", "AcquisitionTime", time, time)

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
        database.new_value("scan1", "PatientName", "test2", "test2")
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")

        # Testing with wrong types
        return_value = database.new_value("scan2", "Bits per voxel", "space_tag", "space_tag")
        self.assertEqual(return_value, 3)
        value = database.get_current_value("scan2", "Bits per voxel")
        self.assertIsNone(value)
        return_value = database.new_value("scan2", "Bits per voxel", 35, 35.5)
        self.assertEqual(return_value, 4)
        value = database.get_current_value("scan2", "Bits per voxel")
        self.assertIsNone(value)
        return_value = database.new_value("scan1", "BandWidth", "test", "test")
        self.assertEqual(return_value, 3)
        value = database.get_current_value("scan1", "BandWidth")
        self.assertEqual(value, 45)

        # Testing with wrong parameters
        return_value = database.new_value(1, "Grids spacing", "2", "2")
        self.assertEqual(return_value, 2)
        return_value = database.new_value("scan1", None, "1", "1")
        self.assertEqual(return_value, 1)
        return_value = database.new_value("scan1", "PatientName", None, None)
        self.assertEqual(return_value, 5)
        value = database.get_current_value("scan1", "PatientName")
        self.assertEqual(value, "test")
        database.new_value(1, None, True, False)

    def test_get_path(self):
        """
        Tests the method giving the Path table object of a scan
        """

        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "159abc")

        # Testing that a scan is returned if it exists
        scan = database.get_path("scan1")
        self.assertIsInstance(scan, database.table_classes[PATH_TABLE])

        # Testing that None is returned if the scan does not exist
        scan = database.get_path("scan3")
        self.assertIsNone(scan)

        # Testing with wrong parameter
        scan = database.get_path(None)
        self.assertIsNone(scan)
        scan = database.get_path(1)
        self.assertIsNone(scan)

    def test_remove_path(self):
        """
        Tests the method removing a scan
        """
        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "159abc")

        # Adding tag
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN,
                         TAG_TYPE_STRING, None, None, "Name of the patient")

        # Adding value
        database.new_value("scan1", "PatientName", "test", "test")

        # Removing scan
        database.remove_path("scan1")

        # Testing that the scan is removed from Path table
        scan = database.get_path("scan1")
        self.assertIsNone(scan)

        # Testing that the values associated are removed
        value = database.get_current_value("scan1", "PatientName")
        self.assertIsNone(value)

        # Testing with a scan not existing
        return_value = database.remove_path("NotExisting")
        self.assertEqual(return_value, 1)

    def test_add_path(self):
        """
        Tests the method adding a scan
        """

        database = Database(self.string_engine)

        # Adding scan
        database.add_path("scan1", "scan","159abc")

        # Testing that the scan has been added
        scan = database.get_path("scan1")
        self.assertIsInstance(scan, database.table_classes[PATH_TABLE])
        self.assertEqual(scan.checksum, "159abc")
        self.assertEqual(scan.name, "scan1")

        # Testing when trying to add a scan that already exists
        return_value = database.add_path("scan1", "scan", "anotherChecksum")
        self.assertEqual(return_value, 1)
        scan = database.get_path("scan1")
        self.assertEqual(scan.checksum, "159abc")

        # Testing without checksum
        database.add_path("scan_without_checksum", "scan")

        database.save_modifications()
        scan = database.get_path("scan_without_checksum")
        self.assertIsNone(scan.checksum)

        # Testing with invalid parameters
        return_value = database.add_path("scan_wrong_checksum", "scan", True)
        self.assertEqual(return_value, 2)
        return_value = database.add_path(True, "checksum")
        self.assertEqual(return_value, 3)
        return_value = database.add_path("scan", 5, "checksum")
        self.assertEqual(return_value, 4)
        return_value = database.add_path("scan", "Scan", "checksum", False)
        self.assertEqual(return_value, 5)

        # Testing with inheritance
        return_value = database.add_path("scan_inherit", "Scan", "checksum", ["scan1"])
        self.assertEqual(return_value, 0)

        return_value = database.add_path("scan_inherit_2", "Scan", "checksum", ["scan_not_existing"])
        self.assertEqual(return_value, 5)

    def test_get_paths_matching_search(self):
        """
        Tests the method returning the list of paths matching the search(str)
        """

        database = Database(self.string_engine)

        # Testing with wrong parameters
        return_list = database.get_paths_matching_search(1, [])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_search("search", 1)
        self.assertEqual(return_list, [])
        database.add_path("scan1", "scan")
        return_list = database.get_paths_matching_search("search", ["tag_not_existing"])
        self.assertEqual(return_list, [])

        database.add_path("scan2", "scan")
        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, None)
        database.new_value("scan1", "PatientName", "Guerbet1", "Guerbet")
        database.new_value("scan2", "PatientName", "Guerbet2", "Guerbet")
        return_list = database.get_paths_matching_search("search", ["PatientName"])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_search("scan", ["PatientName"])
        self.assertEqual(return_list, ["scan1", "scan2"])
        return_list = database.get_paths_matching_search("Guerbet", ["PatientName"])
        self.assertEqual(return_list, ["scan1", "scan2"])
        return_list = database.get_paths_matching_search("Guerbet1", ["PatientName"])
        self.assertEqual(return_list, ["scan1"])
        return_list = database.get_paths_matching_search("Guerbet2", ["PatientName"])
        self.assertEqual(return_list, ["scan2"])

    def test_get_paths_matching_advanced_search(self):
        """
        Tests the method returning the list of paths matching the advanced search
        """

        database = Database(self.string_engine)

        return_list = database.get_paths_matching_advanced_search([], [], [], [], [], [])
        self.assertEqual(return_list, [])

        # Testing with wrong parameters
        return_list = database.get_paths_matching_advanced_search(1, [], [], [], [], [])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search(["AND"], [], ["="], ["Guerbet"], [""], [])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["PatientName"], ["wrong_condition"], ["Guerbet"], [""], [])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["PatientName"], ["wrong_condition"], ["Guerbet"],["wrong_not"], [])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["PatientName"], ["BETWEEN"], ["Guerbet"],["NOT"], [])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["PatientName"], ["BETWEEN"], ["Guerbet"],["NOT"], 1)
        self.assertEqual(return_list, [])

        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, None)
        database.add_tag("SequenceName", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, None)
        database.add_tag("BandWidth", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_path("scan1", "scan")
        database.add_path("scan2", "scan")
        database.add_path("scan3", "scan")
        database.new_value("scan1", "PatientName", "Guerbet", "Guerbet")
        database.new_value("scan2", "SequenceName", "RARE", "RARE")
        database.new_value("scan3", "BandWidth", 50000, 50000)
        return_list = database.get_paths_matching_advanced_search([], ["PatientName"], ["="], ["Guerbet"], [""], ["scan1", "scan2", "scan3"])
        self.assertEqual(return_list, ["scan1"])
        return_list = database.get_paths_matching_advanced_search([], ["PatientName"], ["="], ["Guerbet"], ["NOT"], ["scan1", "scan2", "scan3"])
        self.assertTrue("scan2" in return_list)
        self.assertTrue("scan3" in return_list)
        self.assertEqual(len(return_list), 2)
        return_list = database.get_paths_matching_advanced_search([], ["TagNotExisting"], ["="], ["Guerbet"], [""], ["scan1", "scan2", "scan3"])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["FileName"], ["CONTAINS"], ["scan"], [""], ["scan1", "scan2", "scan3"])
        self.assertTrue("scan1" in return_list)
        self.assertTrue("scan2" in return_list)
        self.assertTrue("scan3" in return_list)
        self.assertEqual(len(return_list), 3)
        return_list = database.get_paths_matching_advanced_search(["OR"], ["PatientName", "SequenceName"], ["=", "CONTAINS"], ["Guerbet", "RARE"], ["", ""], ["scan1", "scan2", "scan3"])
        self.assertTrue("scan1" in return_list)
        self.assertTrue("scan2" in return_list)
        self.assertEqual(len(return_list), 2)
        return_list = database.get_paths_matching_advanced_search(["AND"], ["PatientName", "SequenceName"],
                                                                  ["=", "CONTAINS"], ["Guerbet", "RARE"], ["", ""], ["scan1", "scan2", "scan3"])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["BandWidth"], ["="], ["50000"], [""], ["scan1", "scan2", "scan3"])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["BandWidth"], ["="], [50000], [""], ["scan1", "scan2", "scan3"])
        self.assertEqual(return_list, ["scan3"])
        return_list = database.get_paths_matching_advanced_search([], ["BandWidth"], ["="], [50000], [""],
                                                                  ["scan1", "scan2"])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_advanced_search([], ["FileType"], ["="], ["scan"], [""],
                                                                  ["scan1", "scan2", "scan3"])
        self.assertTrue("scan1" in return_list)
        self.assertTrue("scan2" in return_list)
        self.assertTrue("scan3" in return_list)
        return_list = database.get_paths_matching_advanced_search([], [["FileName", "FileType"]], ["="], ["scan"], [""],
                                                                  ["scan1", "scan2", "scan3"])
        self.assertTrue("scan1" in return_list)
        self.assertTrue("scan2" in return_list)
        self.assertTrue("scan3" in return_list)

    def test_get_paths_matching_tag_value_couples(self):
        """
        Tests the method giving the list of scans having all the values given
        """

        database = Database(self.string_engine)

        # Testing with wrong parameters
        return_list = database.get_paths_matching_tag_value_couples([])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_tag_value_couples(False)
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_tag_value_couples([["tag_not_existing", "Guerbet"]])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_tag_value_couples([["tag_not_existing"]])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_tag_value_couples([["tag_not_existing", "Guerbet", "too_many"]])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_tag_value_couples([1])
        self.assertEqual(return_list, [])
        return_list = database.get_paths_matching_tag_value_couples("test")
        self.assertEqual(return_list, [])

        database.add_tag("PatientName", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, None)
        database.add_tag("SequenceName", TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None, None)
        database.add_tag("BandWidth", TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None, None)
        database.add_path("scan1", "scan")
        database.add_path("scan2", "scan")
        database.add_path("scan3", "scan")
        database.new_value("scan1", "PatientName", "Guerbet", "Guerbet")
        database.new_value("scan2", "SequenceName", "RARE", "RARE")
        database.new_value("scan2", "BandWidth", 50000, 50000)

        return_list = database.get_paths_matching_tag_value_couples([["PatientName", "Guerbet"]])
        self.assertEqual(return_list, ["scan1"])
        return_list = database.get_paths_matching_tag_value_couples([["PatientName", "Guerbet"], ["SequenceName", "RARE"]])
        self.assertEqual(return_list, [])
        database.new_value("scan2", "PatientName", "Guerbet", "Guerbet")
        return_list = database.get_paths_matching_tag_value_couples([["PatientName", "Guerbet"], ["SequenceName", "RARE"]])
        self.assertEqual(return_list, ["scan2"])
        return_list = database.get_paths_matching_tag_value_couples(
            [["PatientName", "Guerbet"], ["SequenceName", "RARE"], ["BandWidth", 50000]])
        self.assertEqual(return_list, ["scan2"])
        return_list = database.get_paths_matching_tag_value_couples(
            [["PatientName", "Guerbet"], ["SequenceName", "RARE"], ["BandWidth", "50000"]])
        self.assertEqual(return_list, [])

if __name__ == '__main__':
    unittest.main()