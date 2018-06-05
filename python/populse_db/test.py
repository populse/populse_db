import os
import shutil
import unittest
import tempfile
import datetime

import populse_db

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

    def test_add_field(self):
        """
        Tests the method adding a field
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Testing with a first field
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        # Checking the field properties
        field = database.get_field("collection1", "PatientName")
        self.assertEqual(field.name, "PatientName")
        self.assertEqual(field.type, populse_db.database.FIELD_TYPE_STRING)
        self.assertEqual(field.description, "Name of the patient")
        self.assertEqual(field.collection, "collection1")

        # Testing with a field that already exists
        try:
            database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
            self.fail()
        except ValueError:
            pass

        # Testing with all field types
        database.add_field("collection1", "BandWidth", populse_db.database.FIELD_TYPE_FLOAT, None)
        database.add_field("collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, "with space")
        database.add_field("collection1", "AcquisitionTime", populse_db.database.FIELD_TYPE_TIME, None)
        database.add_field("collection1", "AcquisitionDate", populse_db.database.FIELD_TYPE_DATETIME, None)
        database.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

        database.add_field("collection1", "Bitspervoxel", populse_db.database.FIELD_TYPE_INTEGER, "without space")
        self.assertEqual(database.get_field(
            "collection1", "Bitspervoxel").description, "without space")
        self.assertEqual(database.get_field(
            "collection1", "Bits per voxel").description, "with space")
        database.add_field("collection1", "Boolean", populse_db.database.FIELD_TYPE_BOOLEAN, None)
        database.add_field("collection1", "Boolean list", populse_db.database.FIELD_TYPE_LIST_BOOLEAN, None)

        # Testing with wrong parameters
        try:
            database.add_field("collection_not_existing", "Field", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
            self.fail()
        except ValueError:
            pass
        try:
            database.add_field(True, "Field", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
            self.fail()
        except ValueError:
            pass
        try:
            database.add_field("collection1", None, populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
            self.fail()
        except ValueError:
            pass
        try:
            database.add_field("collection1", "Patient Name", None, None)
            self.fail()
        except ValueError:
            pass
        try:
            database.add_field("collection1", "Patient Name", populse_db.database.FIELD_TYPE_STRING, 1.5)
            self.fail()
        except ValueError:
            pass

        # Testing that the document primary key field is taken
        try:
            database.add_field("collection1", "name", populse_db.database.FIELD_TYPE_STRING, None)
            self.fail()
        except ValueError:
            pass

        # TODO Testing column creation

    def test_remove_field(self):
        """
        Tests the method removing a field
        """

        database = populse_db.database.Database(self.string_engine, True)

        # Adding collection
        database.add_collection("current", "name")

        # Adding fields
        database.add_field("current", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
        database.add_field("current", "SequenceName", populse_db.database.FIELD_TYPE_STRING, None)
        database.add_field("current", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

        # Adding documents
        document = {}
        document["name"] = "document1"
        database.add_document("current", document)
        document = {}
        document["name"] = "document2"
        database.add_document("current", document)

        # Adding values
        database.new_value("current", "document1", "PatientName", "Guerbet")
        database.new_value("current", "document1", "SequenceName", "RARE")
        database.new_value("current", "document1", "Dataset dimensions", [1, 2])

        # Removing fields
        database.remove_field("current", "PatientName")
        database.remove_field("current", "Dataset dimensions")

        # Testing that the field does not exist anymore
        self.assertIsNone(database.get_field("current", "PatientName"))
        self.assertIsNone(database.get_field("current", "Dataset dimensions"))

        # Testing that the field values are removed
        self.assertIsNone(database.get_value("current", "document1", "PatientName"))
        self.assertEqual(database.get_value(
            "current", "document1", "SequenceName"), "RARE")
        self.assertIsNone(database.get_value(
            "current", "document1", "Dataset dimensions"))

        # Testing with list of fields
        database.remove_field("current", ["SequenceName"])
        self.assertIsNone(database.get_field("current", "SequenceName"))

        # Adding fields again
        database.add_field("current", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
        database.add_field("current", "SequenceName", populse_db.database.FIELD_TYPE_STRING, None)
        database.add_field("current", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

        # Testing with list of fields
        database.remove_field("current", ["SequenceName", "PatientName"])
        self.assertIsNone(database.get_field("current", "SequenceName"))
        self.assertIsNone(database.get_field("current", "PatientName"))

        # Testing with a field not existing
        try:
            database.remove_field("not_existing", "document1")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_field(1, "NotExisting")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_field("current", "NotExisting")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_field("current", "Dataset dimension")
            self.fail()
        except ValueError:
            pass

        # Testing with wrong parameter
        try:
            database.remove_field("current", 1)
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_field("current", None)
            self.fail()
        except ValueError:
            pass

        # TODO Testing column removal

    def test_get_field(self):
        """
        Tests the method giving the field row given a field
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding field
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        # Testing that the field is returned if it exists
        self.assertIsNotNone(database.get_field("collection1", "PatientName"))

        # Testing that None is returned if the field does not exist
        self.assertIsNone(database.get_field("collection1", "Test"))

        # Testing that None is returned if the collection does not exist
        self.assertIsNone(database.get_field("collection_not_existing", "PatientName"))

        # Testing that None is returned if both collection and field do not exist
        self.assertIsNone(database.get_field("collection_not_existing", "Test"))

    def test_get_fields(self):
        """
        Tests the method giving the fields rows, given a collection
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding field
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        fields = database.get_fields("collection1")
        self.assertEqual(len(fields), 2)

        database.add_field("collection1", "SequenceName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        fields = database.get_fields("collection1")
        self.assertEqual(len(fields), 3)

        # Adding second collection
        database.add_collection("collection2", "id")

        fields = database.get_fields("collection1")
        self.assertEqual(len(fields), 3)

        # Testing with a collection not existing
        self.assertEqual(database.get_fields("collection_not_existing"), [])

    def test_get_fields_names(self):
        """
        Tests the method giving the fields names, given a collection
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding field
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        fields = database.get_fields_names("collection1")
        self.assertEqual(len(fields), 2)
        self.assertTrue("name" in fields)
        self.assertTrue("PatientName" in fields)

        database.add_field("collection1", "SequenceName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        fields = database.get_fields_names("collection1")
        self.assertEqual(len(fields), 3)
        self.assertTrue("name" in fields)
        self.assertTrue("PatientName" in fields)
        self.assertTrue("SequenceName" in fields)

        # Adding second collection
        database.add_collection("collection2", "id")

        fields = database.get_fields_names("collection1")
        self.assertEqual(len(fields), 3)
        self.assertTrue("name" in fields)
        self.assertTrue("PatientName" in fields)
        self.assertTrue("SequenceName" in fields)

        # Testing with a collection not existing
        self.assertEqual(database.get_fields_names("collection_not_existing"), [])

    def test_get_value(self):
        """
        Tests the method giving the current value, given a document and a field
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding documents
        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)

        # Adding fields
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
        database.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
        database.add_field("collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
        database.add_field("collection1", "Grids spacing", populse_db.database.FIELD_TYPE_LIST_FLOAT, None)

        # Adding values
        database.new_value("collection1", "document1", "PatientName", "test")
        database.new_value("collection1", "document1", "Bits per voxel", 10)
        database.new_value(
            "collection1", "document1", "Dataset dimensions", [3, 28, 28, 3])
        database.new_value("collection1", "document1", "Grids spacing", [
                           0.234375, 0.234375, 0.4])

        # Testing that the value is returned if it exists
        self.assertEqual(database.get_value(
            "collection1", "document1", "PatientName"), "test")
        self.assertEqual(database.get_value(
            "collection1", "document1", "Bits per voxel"), 10)
        self.assertEqual(database.get_value(
            "collection1", "document1", "Dataset dimensions"), [3, 28, 28, 3])
        self.assertEqual(database.get_value(
            "collection1", "document1", "Grids spacing"), [0.234375, 0.234375, 0.4])

        # Testing when not existing
        self.assertIsNone(database.get_value("collection_not_existing", "document1", "PatientName"))
        self.assertIsNone(database.get_value("collection1", "document3", "PatientName"))
        self.assertIsNone(database.get_value("collection1", "document1", "NotExisting"))
        self.assertIsNone(database.get_value("collection1", "document3", "NotExisting"))
        self.assertIsNone(database.get_value("collection1", "document2", "Grids spacing"))

        # Testing with wrong parameters
        self.assertIsNone(database.get_value(3, "document1", "Grids spacing"))
        self.assertIsNone(database.get_value("collection1", 1, "Grids spacing"))
        self.assertIsNone(database.get_value("collection1", "document1", None))
        self.assertIsNone(database.get_value("collection1", 3.5, None))

    def test_set_value(self):
        """
        Tests the method setting a value
        """

        database = populse_db.database.Database(self.string_engine, True)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding document
        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)

        # Adding fields
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
        database.add_field(
            "collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
        database.add_field(
            "collection1", "AcquisitionDate", populse_db.database.FIELD_TYPE_DATETIME, None)
        database.add_field(
            "collection1", "AcquisitionTime", populse_db.database.FIELD_TYPE_TIME, None)

        # Adding values and changing it
        database.new_value("collection1", "document1", "PatientName", "test", "test")
        database.set_value("collection1", "document1", "PatientName", "test2")

        database.new_value("collection1", "document1", "Bits per voxel", 1, 1)
        database.set_value("collection1", "document1", "Bits per voxel", 2)

        date = datetime.datetime(2014, 2, 11, 8, 5, 7)
        database.new_value("collection1", "document1", "AcquisitionDate", date, date)
        self.assertEqual(database.get_value("collection1", "document1", "AcquisitionDate"), date)
        date = datetime.datetime(2015, 2, 11, 8, 5, 7)
        database.set_value("collection1", "document1", "AcquisitionDate", date)

        time = datetime.datetime(2014, 2, 11, 0, 2, 20).time()
        database.new_value("collection1", "document1", "AcquisitionTime", time, time)
        self.assertEqual(database.get_value(
            "collection1", "document1", "AcquisitionTime"), time)
        time = datetime.datetime(2014, 2, 11, 15, 24, 20).time()
        database.set_value("collection1", "document1", "AcquisitionTime", time)

        # Testing that the values are actually set
        self.assertEqual(database.get_value(
            "collection1", "document1", "PatientName"), "test2")
        self.assertEqual(database.get_value(
            "collection1", "document1", "Bits per voxel"), 2)
        self.assertEqual(database.get_value(
            "collection1", "document1", "AcquisitionDate"), date)
        self.assertEqual(database.get_value(
            "collection1", "document1", "AcquisitionTime"), time)
        database.set_value("collection1", "document1", "PatientName", None)
        self.assertIsNone(database.get_value("collection1", "document1", "PatientName"))

        # Testing when not existing
        try:
            database.set_value("collection_not_existing", "document3", "PatientName", None)
            self.fail()
        except ValueError:
            pass
        try:
            database.set_value("collection1", "document3", "PatientName", None)
            self.fail()
        except ValueError:
            pass
        try:
            database.set_value("collection1", "document1", "NotExisting", None)
            self.fail()
        except ValueError:
            pass
        try:
            database.set_value("collection1", "document3", "NotExisting", None)
            self.fail()
        except ValueError:
            pass

        # Testing with wrong types
        try:
            database.set_value("collection1", "document1", "Bits per voxel", "test")
            self.fail()
        except ValueError:
            pass
        self.assertEqual(database.get_value("collection1",
            "document1", "Bits per voxel"), 2)
        try:
            database.set_value("collection1", "document1", "Bits per voxel", 35.8)
            self.fail()
        except ValueError:
            pass
        self.assertEqual(database.get_value(
            "collection1", "document1", "Bits per voxel"), 2)

        # Testing with wrong parameters
        try:
            database.set_value(False, "document1", "Bits per voxel", 35)
            self.fail()
        except ValueError:
            pass
        try:
            database.set_value("collection1", 1, "Bits per voxel", "2")
            self.fail()
        except ValueError:
            pass
        try:
            database.set_value("collection1", "document1", None, "1")
            self.fail()
        except ValueError:
            pass
        try:
            database.set_value("collection1", 1, None, True)
            self.fail()
        except ValueError:
            pass

    def test_remove_value(self):
        """
        Tests the method removing a value
        """

        database = populse_db.database.Database(self.string_engine, True)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding document
        database.add_document("collection1", "document1")

        # Adding fields
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
        database.add_field("collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
        database.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

        # Adding values
        database.new_value("collection1", "document1", "PatientName", "test")
        try:
            database.new_value("collection1", "document1", "Bits per voxel", "space_field")
            self.fail()
        except ValueError:
            pass
        database.new_value(
            "collection1", "document1", "Dataset dimensions", [3, 28, 28, 3])
        value = database.get_value("collection1", "document1", "Dataset dimensions")
        self.assertEqual(value, [3, 28, 28, 3])

        # Removing values
        database.remove_value("collection1", "document1", "PatientName")
        database.remove_value("collection1", "document1", "Bits per voxel")
        database.remove_value("collection1", "document1", "Dataset dimensions")

        # Testing when not existing
        try:
            database.remove_value("collection_not_existing", "document1", "PatientName")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_value("collection1", "document3", "PatientName")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_value("collection1", "document1", "NotExisting")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_value("collection1", "document3", "NotExisting")
            self.fail()
        except ValueError:
            pass

        # Testing that the values are actually removed
        self.assertIsNone(database.get_value("collection1", "document1", "PatientName"))
        self.assertIsNone(database.get_value(
            "collection1", "document1", "Bits per voxel"))
        self.assertIsNone(database.get_value(
            "collection1", "document1", "Dataset dimensions"))

    def test_check_type_value(self):
        """
        Tests the method checking the validity of incoming values
        """

        database = populse_db.database.Database(self.string_engine)
        is_valid = database.check_type_value("string", populse_db.database.FIELD_TYPE_STRING)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1, populse_db.database.FIELD_TYPE_STRING)
        self.assertFalse(is_valid)
        is_valid = database.check_type_value(None, populse_db.database.FIELD_TYPE_STRING)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1, populse_db.database.FIELD_TYPE_INTEGER)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1, populse_db.database.FIELD_TYPE_FLOAT)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1.5, populse_db.database.FIELD_TYPE_FLOAT)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(None, None)
        self.assertFalse(is_valid)
        is_valid = database.check_type_value([1.5], populse_db.database.FIELD_TYPE_LIST_FLOAT)
        self.assertTrue(is_valid)
        is_valid = database.check_type_value(1.5, populse_db.database.FIELD_TYPE_LIST_FLOAT)
        self.assertFalse(is_valid)
        is_valid = database.check_type_value(
            [1.5, "test"], populse_db.database.FIELD_TYPE_LIST_FLOAT)
        self.assertFalse(is_valid)

    def test_new_value(self):
        """
        Tests the method adding a value
        """

        database = populse_db.database.Database(self.string_engine, True)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding documents
        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)
        document = {}
        document["name"] = "document2"
        database.add_document("collection1", document)

        # Adding fields
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
        database.add_field(
            "collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
        database.add_field("collection1", "BandWidth", populse_db.database.FIELD_TYPE_FLOAT, None)
        database.add_field("collection1", "AcquisitionTime", populse_db.database.FIELD_TYPE_TIME, None)
        database.add_field("collection1", "AcquisitionDate", populse_db.database.FIELD_TYPE_DATETIME, None)
        database.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
        database.add_field("collection1", "Grids spacing", populse_db.database.FIELD_TYPE_LIST_FLOAT, None)
        database.add_field("collection1", "Boolean", populse_db.database.FIELD_TYPE_BOOLEAN, None)
        database.add_field("collection1", "Boolean list", populse_db.database.FIELD_TYPE_LIST_BOOLEAN, None)

        # Adding values
        database.new_value("collection1", "document1", "PatientName", "test")
        database.new_value("collection1", "document2", "BandWidth", 35.5)
        database.new_value("collection1", "document1", "Bits per voxel", 1)
        database.new_value(
            "collection1", "document1", "Dataset dimensions", [3, 28, 28, 3])
        database.new_value("collection1", "document2", "Grids spacing", [
                           0.234375, 0.234375, 0.4])
        database.new_value("collection1", "document1", "Boolean", True)

        # Testing when not existing
        try:
            database.new_value("collection_not_existing", "document1", "PatientName", "test")
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", "document1", "NotExisting", "none")
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", "document3", "SequenceName", "none")
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", "document3", "NotExisting", "none")
            self.fail()
        except ValueError:
            pass
        self.assertIsNone(database.new_value("collection1", "document1", "BandWidth", 45))

        date = datetime.datetime(2014, 2, 11, 8, 5, 7)
        database.new_value("collection1", "document1", "AcquisitionDate", date)
        time = datetime.datetime(2014, 2, 11, 0, 2, 2).time()
        database.new_value("collection1", "document1", "AcquisitionTime", time)

        # Testing that the values are actually added
        self.assertEqual(database.get_value(
            "collection1", "document1", "PatientName"), "test")
        self.assertEqual(database.get_value(
            "collection1", "document2", "BandWidth"), 35.5)
        self.assertEqual(database.get_value(
            "collection1", "document1", "Bits per voxel"), 1)
        self.assertEqual(database.get_value("collection1", "document1", "BandWidth"), 45)
        self.assertEqual(database.get_value(
            "collection1", "document1", "AcquisitionDate"), date)
        self.assertEqual(database.get_value(
            "collection1", "document1", "AcquisitionTime"), time)
        self.assertEqual(database.get_value(
            "collection1", "document1", "Dataset dimensions"), [3, 28, 28, 3])
        self.assertEqual(database.get_value(
            "collection1", "document2", "Grids spacing"), [0.234375, 0.234375, 0.4])
        self.assertEqual(database.get_value("collection1", "document1", "Boolean"), True)

        # Test value override
        try:
            database.new_value("collection1", "document1", "PatientName", "test2", "test2")
            self.fail()
        except ValueError:
            pass
        value = database.get_value("collection1", "document1", "PatientName")
        self.assertEqual(value, "test")

        # Testing with wrong types
        try:
            database.new_value("collection1", "document2", "Bits per voxel",
                               "space_field", "space_field")
            self.fail()
        except ValueError:
            pass
        self.assertIsNone(database.get_value(
            "collection1", "document2", "Bits per voxel"))
        try:
            database.new_value("collection1", "document2", "Bits per voxel", 35.5)
            self.fail()
        except ValueError:
            pass
        self.assertIsNone(database.get_value(
            "collection1", "document2", "Bits per voxel"))
        try:
            database.new_value("collection1", "document1", "BandWidth", "test", "test")
            self.fail()
        except ValueError:
            pass
        self.assertEqual(database.get_value("collection1", "document1", "BandWidth"), 45)

        # Testing with wrong parameters
        try:
            database.new_value(5, "document1", "Grids spacing", "2", "2")
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", 1, "Grids spacing", "2", "2")
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", "document1", None, "1", "1")
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", "document1", "PatientName", None, None)
            self.fail()
        except ValueError:
            pass
        self.assertEqual(database.get_value(
            "collection1", "document1", "PatientName"), "test")
        try:
            database.new_value("collection1", 1, None, True)
            self.fail()
        except ValueError:
            pass
        try:
            database.new_value("collection1", "document2", "Boolean", "boolean")
            self.fail()
        except ValueError:
            pass

    def test_get_document(self):
        """
        Tests the method giving the document row given a document
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding document
        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)

        # Testing that a document is returned if it exists
        self.assertIsInstance(database.get_document(
            "collection1", "document1").row, database.table_classes["collection1"])

        # Testing that None is returned if the document does not exist
        self.assertIsNone(database.get_document("collection1", "document3"))

        # Testing that None is returned if the collection does not exist
        self.assertIsNone(database.get_document("collection_not_existing", "document1"))

        # Testing with wrong parameter
        self.assertIsNone(database.get_document(False, "document1"))
        self.assertIsNone(database.get_document("collection1", None))
        self.assertIsNone(database.get_document("collection1", 1))

    def test_remove_document(self):
        """
        Tests the method removing a document
        """
        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding documents
        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)
        document = {}
        document["name"] = "document2"
        database.add_document("collection1", document)

        # Adding field
        database.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

        # Adding value
        database.new_value("collection1", "document1", "PatientName", "test")

        # Removing document
        database.remove_document("collection1", "document1")

        # Testing that the document is removed from all tables
        self.assertIsNone(database.get_document("collection1", "document1"))

        # Testing that the values associated are removed
        self.assertIsNone(database.get_value("collection1", "document1", "PatientName"))

        # Testing with a collection not existing
        try:
            database.remove_document("collection_not_existing", "document1")
            self.fail()
        except ValueError:
            pass

        # Testing with a document not existing
        try:
            database.remove_document("collection1", "NotExisting")
            self.fail()
        except ValueError:
            pass

        # Removing document
        database.remove_document("collection1", "document2")

        # Testing that the document is removed from document (and initial) tables
        self.assertIsNone(database.get_document("collection1", "document2"))

        # Removing document a second time
        try:
            database.remove_document("collection1", "document1")
            self.fail()
        except ValueError:
            pass

    def test_add_document(self):
        """
        Tests the method adding a document
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding document
        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)

        # Testing that the document has been added
        document = database.get_document("collection1", "document1")
        self.assertIsInstance(document.row, database.table_classes["collection1"])
        self.assertEqual(document.name, "document1")

        # Testing when trying to add a document that already exists
        try:
            document = {}
            document["name"] = "document1"
            database.add_document("collection1", document)
            self.fail()
        except ValueError:
            pass

        # Testing with invalid parameters
        try:
            database.add_document(15, "document1")
            self.fail()
        except ValueError:
            pass
        try:
            database.add_document("collection_not_existing", "document1")
            self.fail()
        except ValueError:
            pass
        try:
            database.add_document("collection1", True)
            self.fail()
        except ValueError:
            pass

        # Testing the add of several documents
        document = {}
        document["name"] = "document2"
        database.add_document("collection1", document)

        # Adding document with dictionary without primary key
        try:
            document = {}
            document["no_primary_key"] = "document1"
            database.add_document("collection1", document)
            self.fail()
        except ValueError:
            pass

    def test_add_collection(self):
        """
        Tests the method adding a collection
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding a first collection
        database.add_collection("collection1")

        # Checking values
        collection = database.get_collection("collection1")
        self.assertEqual(collection.name, "collection1")
        self.assertEqual(collection.primary_key, "name")

        # Adding a second collection
        database.add_collection("collection2", "id")

        # Checking values
        collection = database.get_collection("collection2")
        self.assertEqual(collection.name, "collection2")
        self.assertEqual(collection.primary_key, "id")

        # Trying with a collection already existing
        try:
            database.add_collection("collection1")
            self.fail()
        except ValueError:
            pass

        # Trying with table names already taken
        try:
            database.add_collection("field")
            self.fail()
        except ValueError:
            pass

        try:
            database.add_collection("collection")
            self.fail()
        except ValueError:
            pass

    def test_remove_collection(self):
        """
        Tests the method removing a collection
        """

        database = populse_db.database.Database(self.string_engine, True)

        # Adding a first collection
        database.add_collection("collection1")

        # Checking values
        collection = database.get_collection("collection1")
        self.assertEqual(collection.name, "collection1")
        self.assertEqual(collection.primary_key, "name")

        # Removing collection
        database.remove_collection("collection1")

        # Testing that it does not exist anymore
        self.assertIsNone(database.get_collection("collection1"))

        # Adding new collections
        database.add_collection("collection1")
        database.add_collection("collection2")

        # Checking values
        collection = database.get_collection("collection1")
        self.assertEqual(collection.name, "collection1")
        self.assertEqual(collection.primary_key, "name")
        collection = database.get_collection("collection2")
        self.assertEqual(collection.name, "collection2")
        self.assertEqual(collection.primary_key, "name")

        # Removing one collection and testing that the other is unchanged
        database.remove_collection("collection2")
        collection = database.get_collection("collection1")
        self.assertEqual(collection.name, "collection1")
        self.assertEqual(collection.primary_key, "name")
        self.assertIsNone(database.get_collection("collection2"))

        # Adding field
        database.add_field("collection1", "Field", populse_db.database.FIELD_TYPE_STRING)
        field = database.get_field("collection1", "Field")
        self.assertEqual(field.name, "Field")
        self.assertEqual(field.collection, "collection1")
        self.assertIsNone(field.description)
        self.assertEqual(field.type, populse_db.database.FIELD_TYPE_STRING)

        # Adding document
        database.add_document("collection1", "document")
        document = database.get_document("collection1", "document")
        self.assertEqual(document.name, "document")

        # Removing the collection containing the field and the document and testing that everything is None
        database.remove_collection("collection1")
        self.assertIsNone(database.get_collection("collection1"))
        self.assertIsNone(database.get_field("collection1", "name"))
        self.assertIsNone(database.get_field("collection1", "Field"))
        self.assertIsNone(database.get_document("collection1", "document"))

        # Testing with a collection not existing
        try:
            database.remove_collection("collection_not_existing")
            self.fail()
        except ValueError:
            pass
        try:
            database.remove_collection(True)
            self.fail()
        except ValueError:
            pass

    def test_get_collection(self):
        """
        Tests the method giving the collection row
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding a first collection
        database.add_collection("collection1")

        # Checking values
        collection = database.get_collection("collection1")
        self.assertEqual(collection.name, "collection1")
        self.assertEqual(collection.primary_key, "name")

        # Adding a second collection
        database.add_collection("collection2", "id")

        # Checking values
        collection = database.get_collection("collection2")
        self.assertEqual(collection.name, "collection2")
        self.assertEqual(collection.primary_key, "id")

        # Trying with a collection not existing
        self.assertIsNone(database.get_collection("collection3"))

        # Trying with a table name already existing
        self.assertIsNone(database.get_collection("collection"))
        self.assertIsNone(database.get_collection("field"))

    def test_get_documents(self):
        """
        Tests the method returning the list of document rows, given a collection
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collections
        database.add_collection("collection1", "name")
        database.add_collection("collection2", "id")

        database.add_document("collection1", "document1")
        database.add_document("collection1", "document2")
        database.add_document("collection2", "document1")
        database.add_document("collection2", "document2")

        documents1 = database.get_documents("collection1")
        self.assertEqual(len(documents1), 2)

        documents2 = database.get_documents("collection2")
        self.assertEqual(len(documents2), 2)

        # Testing with collection not existing
        self.assertEqual(database.get_documents("collection_not_existing"), [])
        self.assertEqual(database.get_documents("collection"), [])
        self.assertEqual(database.get_documents(None), [])

    def test_get_documents_names(self):
        """
        Tests the method returning the list of document names, given a collection
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collections
        database.add_collection("collection1", "name")
        database.add_collection("collection2", "id")

        database.add_document("collection1", "document1")
        database.add_document("collection1", "document2")
        database.add_document("collection2", "document3")
        database.add_document("collection2", "document4")

        documents1 = database.get_documents_names("collection1")
        self.assertEqual(len(documents1), 2)
        self.assertTrue("document1" in documents1)
        self.assertTrue("document2" in documents1)

        documents2 = database.get_documents_names("collection2")
        self.assertEqual(len(documents2), 2)
        self.assertTrue("document3" in documents2)
        self.assertTrue("document4" in documents2)

        # Testing with collection not existing
        self.assertEqual(database.get_documents_names("collection_not_existing"), [])
        self.assertEqual(database.get_documents_names("collection"), [])
        self.assertEqual(database.get_documents_names(None), [])

    def test_list_dates(self):
        """
        Tests the storage and retrieval of fields of type list of time, date
        and datetime
        """

        database = populse_db.database.Database(self.string_engine)

        # Adding collection
        database.add_collection("collection1", "name")

        # Adding fields
        database.add_field("collection1", "list_date", populse_db.database.FIELD_TYPE_LIST_DATE, None)
        database.add_field("collection1", "list_time", populse_db.database.FIELD_TYPE_LIST_TIME, None)
        database.add_field("collection1", "list_datetime", populse_db.database.FIELD_TYPE_LIST_DATETIME, None)

        document = {}
        document["name"] = "document1"
        database.add_document("collection1", document)

        list_date = [datetime.date(2018, 5, 23), datetime.date(1899, 12, 31)]
        list_time = [datetime.time(12, 41, 33, 540), datetime.time(1, 2, 3)]
        list_datetime = [datetime.datetime(2018, 5, 23, 12, 41, 33, 540),
                         datetime.datetime(1899, 12, 31, 1, 2, 3)]

        database.new_value("collection1", "document1", "list_date", list_date)
        self.assertEqual(
            list_date, database.get_value("collection1", "document1", "list_date"))
        database.new_value("collection1", "document1", "list_time", list_time)
        self.assertEqual(
            list_time, database.get_value("collection1", "document1", "list_time"))
        database.new_value("collection1", "document1", "list_datetime", list_datetime)
        self.assertEqual(list_datetime, database.get_value(
            "collection1", "document1", "list_datetime"))

    def test_filters(self):
        list_datetime = [datetime.datetime(2018, 5, 23, 12, 41, 33, 540),
                         datetime.datetime(1981, 5, 8, 20, 0),
                         datetime.datetime(1899, 12, 31, 1, 2, 3)]

        database = populse_db.database.Database(self.string_engine)

        database.add_collection("collection1", "name")

        database.add_field("collection1", 'format', field_type='string', description=None)
        database.add_field("collection1", 'strings', field_type=populse_db.database.FIELD_TYPE_LIST_STRING, description=None)
        database.add_field("collection1", 'datetime', field_type=populse_db.database.FIELD_TYPE_DATETIME, description=None)

        database.save_modifications()
        files = ('abc', 'bcd', 'def', 'xyz')
        for file in files:
            for date in list_datetime:
                for format, ext in (('NIFTI', 'nii'),
                                    ('DICOM', 'dcm'),
                                    ('Freesurfer', 'mgz')):
                    document = dict(
                        name='/%s_%d.%s' % (file, date.year, ext),
                        format=format,
                        strings=list(file),
                        datetime=date,
                    )
                    database.add_document("collection1", document)
                document = '/%s_%d.none' % (file, date.year)
                database.add_document("collection1", dict(name=document, strings=list(file)))
        database.save_modifications()

        for filter, expected in (
            ('format == "NIFTI"',
             {
              '/xyz_1899.nii',
              '/xyz_2018.nii',
              '/abc_2018.nii',
              '/bcd_1899.nii',
              '/bcd_2018.nii',
              '/def_1899.nii',
              '/abc_1981.nii',
              '/def_2018.nii',
              '/def_1981.nii',
              '/bcd_1981.nii',
              '/abc_1899.nii',
              '/xyz_1981.nii'
             }
            ),
             
            ('"b" IN strings',
             {
              '/bcd_2018.mgz',
              '/abc_1899.mgz',
              '/abc_1899.dcm',
              '/bcd_1981.dcm',
              '/abc_1981.dcm',
              '/bcd_1981.mgz',
              '/bcd_1899.mgz',
              '/abc_1981.mgz',
              '/abc_2018.mgz',
              '/abc_2018.dcm',
              '/bcd_2018.dcm',
              '/bcd_1899.dcm',
              '/abc_2018.nii',
              '/bcd_1899.nii',
              '/abc_1981.nii',
              '/bcd_1981.nii',
              '/abc_1899.nii',
              '/bcd_2018.nii',
              '/abc_1899.none',
              '/bcd_1899.none',
              '/bcd_1981.none',
              '/abc_2018.none',
              '/bcd_2018.none',
              '/abc_1981.none'
             }
            ),
            
            ('(format == "NIFTI" OR NOT format == "DICOM")',
             {
              '/xyz_1899.nii',
              '/xyz_1899.mgz',
              '/bcd_2018.mgz',
              '/bcd_1899.nii',
              '/bcd_2018.nii',
              '/def_1899.nii',
              '/bcd_1981.mgz',
              '/abc_1981.nii',
              '/def_2018.mgz',
              '/abc_1899.nii',
              '/def_1899.mgz',
              '/xyz_1899.none',
              '/abc_2018.nii',
              '/def_1899.none',
              '/bcd_1899.mgz',
              '/def_2018.nii',
              '/abc_1981.mgz',
              '/abc_1899.none',
              '/xyz_1981.mgz',
              '/bcd_1981.nii',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/def_1981.nii',
              '/def_1981.mgz',
              '/bcd_1899.none',
              '/xyz_2018.mgz',
              '/bcd_1981.none',
              '/xyz_1981.none',
              '/abc_1981.none',
              '/def_2018.none',
              '/xyz_2018.none',
              '/abc_2018.none',
              '/def_1981.none',
              '/bcd_2018.none'
              }
            ),
            
            ('"a" IN strings',
             {
              '/abc_1899.none',
              '/abc_1899.nii',
              '/abc_2018.nii',
              '/abc_1899.mgz',
              '/abc_1899.dcm',
              '/abc_1981.dcm',
              '/abc_1981.nii',
              '/abc_1981.mgz',
              '/abc_2018.mgz',
              '/abc_2018.dcm',
              '/abc_2018.none',
              '/abc_1981.none'
             }
            ),
             
            ('NOT "b" IN strings',
             {
              '/xyz_1899.nii',
              '/xyz_2018.dcm',
              '/def_1981.dcm',
              '/xyz_2018.nii',
              '/xyz_1981.dcm',
              '/def_1899.none',
              '/xyz_1899.dcm',
              '/xyz_1981.nii',
              '/def_1899.dcm',
              '/def_1899.nii',
              '/def_2018.mgz',
              '/def_2018.nii',
              '/xyz_1899.mgz',
              '/def_2018.dcm',
              '/def_1899.mgz',
              '/def_1981.mgz',
              '/xyz_1981.mgz',
              '/xyz_2018.mgz',
              '/xyz_1899.none',
              '/def_1981.nii',
              '/xyz_2018.none',
              '/xyz_1981.none',
              '/def_2018.none',
              '/def_1981.none'
             }
            ),
            ('("a" IN strings OR NOT "b" IN strings)',
             {
              '/xyz_1899.nii',
              '/xyz_1899.mgz',
              '/def_1899.nii',
              '/abc_1981.nii',
              '/def_2018.mgz',
              '/abc_1899.nii',
              '/def_1899.mgz',
              '/abc_2018.dcm',
              '/xyz_1899.none',
              '/xyz_2018.dcm',
              '/def_1981.dcm',
              '/abc_2018.nii',
              '/def_1899.none',
              '/abc_1981.dcm',
              '/def_2018.nii',
              '/abc_1981.mgz',
              '/def_2018.dcm',
              '/abc_1899.none',
              '/xyz_1981.mgz',
              '/xyz_1899.dcm',
              '/abc_1899.dcm',
              '/def_1899.dcm',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/xyz_1981.dcm',
              '/def_1981.nii',
              '/def_1981.mgz',
              '/xyz_2018.mgz',
              '/xyz_1981.none',
              '/abc_1981.none',
              '/def_2018.none',
              '/xyz_2018.none',
              '/abc_2018.none',
              '/def_1981.none'
             }
            ),
            
            ('(format == "NIFTI" OR NOT format == "DICOM") AND ("a" IN strings OR NOT "b" IN strings)',
             {
              '/abc_1899.none',
              '/xyz_1899.mgz',
              '/abc_1981.mgz',
              '/abc_2018.nii',
              '/xyz_1899.nii',
              '/abc_1899.mgz',
              '/def_1899.mgz',
              '/def_1899.nii',
              '/def_1899.none',
              '/abc_1981.nii',
              '/def_2018.nii',
              '/xyz_2018.nii',
              '/def_1981.nii',
              '/abc_1899.nii',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/def_1981.mgz',
              '/xyz_2018.mgz',
              '/xyz_1899.none',
              '/def_2018.mgz',
              '/xyz_1981.mgz',
              '/xyz_1981.none',
              '/abc_1981.none',
              '/def_2018.none',
              '/xyz_2018.none',
              '/abc_2018.none',
              '/def_1981.none'
             }
            ),
            
            ('format > "DICOM"',
             {
              '/xyz_1899.nii',
              '/xyz_1899.mgz',
              '/bcd_2018.mgz',
              '/bcd_1899.nii',
              '/bcd_2018.nii',
              '/def_1899.nii',
              '/bcd_1981.mgz',
              '/abc_1981.nii',
              '/def_2018.mgz',
              '/abc_1899.nii',
              '/def_1899.mgz',
              '/abc_2018.nii',
              '/def_2018.nii',
              '/abc_1981.mgz',
              '/xyz_1981.mgz',
              '/bcd_1981.nii',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/def_1981.nii',
              '/def_1981.mgz',
              '/bcd_1899.mgz',
              '/xyz_2018.mgz'
             }
            ),
             
            ('format <= "DICOM"',
             {
              '/abc_1981.dcm',
              '/def_1899.dcm',
              #'/xyz_2018.none',
              '/abc_2018.dcm',
              #'/xyz_1899.none',
              '/bcd_1899.dcm',
              #'/bcd_1981.none',
              '/def_1981.dcm',
              #'/def_1899.none',
              #'/xyz_1981.none',
              #'/def_2018.none',
              '/bcd_2018.dcm',
              '/def_2018.dcm',
              #'/abc_1899.none',
              '/xyz_2018.dcm',
              '/xyz_1899.dcm',
              '/abc_1899.dcm',
              #'/def_1981.none',
              #'/bcd_2018.none',
              #'/abc_1981.none',
              '/xyz_1981.dcm',
              #'/abc_2018.none',
              '/bcd_1981.dcm',
              #'/bcd_1899.none'
             }
            ),
             
            ('format > "DICOM" AND strings != ["b", "c", "d"]',
             {
              '/xyz_1899.nii',
              '/xyz_1899.mgz',
              '/abc_1981.mgz',
              '/abc_2018.nii',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/def_1899.mgz',
              '/def_1899.nii',
              '/abc_1981.nii',
              '/def_2018.nii',
              '/def_1981.nii',
              '/abc_1899.nii',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/def_1981.mgz',
              '/xyz_2018.mgz',
              '/def_2018.mgz',
              '/xyz_1981.mgz'
             }
            ),
             
            ('format <= "DICOM" AND strings == ["b", "c", "d"]',
             {
              #'/bcd_1899.none',
              '/bcd_2018.dcm',
              '/bcd_1981.dcm',
              '/bcd_1899.dcm',
              #'/bcd_1981.none',
              #'/bcd_2018.none',
             }
            ),
            
            ('format in [True, false, null]',
             {
              '/def_1899.none',
              '/abc_1899.none',
              '/bcd_1899.none',
              '/xyz_1899.none',
              '/bcd_2018.none',
              '/abc_1981.none',
              '/def_2018.none',
              '/xyz_2018.none',
              '/abc_2018.none',
              '/def_1981.none',
              '/xyz_1981.none',
              '/bcd_1981.none',
             }
            ),
            
            ('format == null',
             {
              '/bcd_1981.none',
              '/abc_1899.none',
              '/def_1899.none',
              '/bcd_2018.none',
              '/abc_1981.none',
              '/def_2018.none',
              '/xyz_2018.none',
              '/abc_2018.none',
              '/def_1981.none',
              '/bcd_1899.none',
              '/xyz_1899.none',
              '/xyz_1981.none'
             }
            ),
            
            ('strings == null',
             set()),
            
            ('strings != NULL',
             {
              '/xyz_1899.nii',
              '/xyz_2018.dcm',
              '/xyz_1899.mgz',
              '/bcd_2018.mgz',
              '/bcd_1899.nii',
              '/def_2018.none',
              '/def_1899.mgz',
              '/def_1899.nii',
              '/bcd_1981.mgz',
              '/abc_1981.nii',
              '/def_2018.mgz',
              '/abc_1899.nii',
              '/bcd_2018.nii',
              '/abc_2018.dcm',
              '/xyz_1899.none',
              '/bcd_1899.dcm',
              '/bcd_1981.none',
              '/def_1981.dcm',
              '/abc_2018.nii',
              '/def_1899.none',
              '/xyz_1981.none',
              '/abc_1981.dcm',
              '/bcd_2018.dcm',
              '/def_2018.nii',
              '/abc_1981.mgz',
              '/def_2018.dcm',
              '/abc_1899.none',
              '/xyz_1981.mgz',
              '/xyz_1899.dcm',
              '/abc_1899.dcm',
              '/def_1899.dcm',
              '/bcd_1981.nii',
              '/def_1981.none',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/xyz_2018.none',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/bcd_1899.mgz',
              '/bcd_2018.none',
              '/abc_1981.none',
              '/xyz_1981.dcm',
              '/abc_2018.none',
              '/def_1981.nii',
              '/bcd_1981.dcm',
              '/def_1981.mgz',
              '/bcd_1899.none',
              '/xyz_2018.mgz'
             }
            ),
            
            ('format != NULL',
             {
              '/xyz_1899.nii',
              '/xyz_1899.mgz',
              '/bcd_2018.mgz',
              '/bcd_1899.nii',
              '/def_1899.mgz',
              '/def_1899.nii',
              '/bcd_1981.mgz',
              '/abc_1981.nii',
              '/def_2018.mgz',
              '/abc_1899.nii',
              '/bcd_2018.nii',
              '/abc_2018.dcm',
              '/xyz_1981.mgz',
              '/def_1981.dcm',
              '/abc_2018.nii',
              '/abc_1981.dcm',
              '/bcd_2018.dcm',
              '/def_2018.nii',
              '/bcd_1981.nii',
              '/abc_1981.mgz',
              '/def_2018.dcm',
              '/bcd_1899.dcm',
              '/xyz_2018.dcm',
              '/xyz_1899.dcm',
              '/abc_1899.dcm',
              '/def_1899.dcm',
              '/bcd_1899.mgz',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/xyz_1981.dcm',
              '/def_1981.nii',
              '/bcd_1981.dcm',
              '/def_1981.mgz',
              '/xyz_2018.mgz'
             }
            ),

            ('all',
             {
              '/xyz_1899.nii',
              '/xyz_2018.dcm',
              '/xyz_1899.mgz',
              '/bcd_2018.mgz',
              '/bcd_1899.nii',
              '/def_2018.none',
              '/def_1899.mgz',
              '/def_1899.nii',
              '/bcd_1981.mgz',
              '/abc_1981.nii',
              '/def_2018.mgz',
              '/abc_1899.nii',
              '/bcd_2018.nii',
              '/abc_2018.dcm',
              '/xyz_1899.none',
              '/bcd_1899.dcm',
              '/bcd_1981.none',
              '/def_1981.dcm',
              '/abc_2018.nii',
              '/def_1899.none',
              '/xyz_1981.none',
              '/abc_1981.dcm',
              '/bcd_2018.dcm',
              '/def_2018.nii',
              '/abc_1981.mgz',
              '/def_2018.dcm',
              '/abc_1899.none',
              '/xyz_1981.mgz',
              '/xyz_1899.dcm',
              '/abc_1899.dcm',
              '/def_1899.dcm',
              '/bcd_1981.nii',
              '/def_1981.none',
              '/xyz_1981.nii',
              '/abc_2018.mgz',
              '/xyz_2018.none',
              '/xyz_2018.nii',
              '/abc_1899.mgz',
              '/bcd_1899.mgz',
              '/bcd_2018.none',
              '/abc_1981.none',
              '/xyz_1981.dcm',
              '/abc_2018.none',
              '/def_1981.nii',
              '/bcd_1981.dcm',
              '/def_1981.mgz',
              '/bcd_1899.none',
              '/xyz_2018.mgz'
             }
            )):
            documents = set(document.name for document in database.filter_documents("collection1", filter))
            try:
                self.assertEqual(documents, expected)
            except Exception as e:
                query = database.filter_query(filter, 'collection1')
                print('!!!', str(query))
                e.message = 'While testing filter : %s\n%s' % (str(filter), e.message)
                e.args = (e.message,)
                raise

    def test_modify_list_field(self):
        database = populse_db.database.Database(self.string_engine)

        database.add_collection("collection1", "name")

        database.add_field("collection1", 'strings', field_type=populse_db.database.FIELD_TYPE_LIST_STRING, description=None)
        database.add_document("collection1", 'test')
        database.new_value("collection1", 'test', 'strings', ['a', 'b', 'c'])
        database.save_modifications()
        names = list(document.name for document in database.filter_documents("collection1", '"b" IN strings'))
        self.assertEqual(names, ['test'])
        
        database.set_value("collection1", 'test', 'strings', ['x', 'y', 'z'])
        database.save_modifications()
        names = list(document.name for document in database.filter_documents("collection1", '"b" IN strings'))
        self.assertEqual(names, [])
        names = list(document.name for document in database.filter_documents("collection1", '"z" IN strings'))
        self.assertEqual(names, ['test'])

        database.remove_value("collection1", 'test', 'strings')
        database.save_modifications()
        names = list(document.name for document in database.filter_documents("collection1", '"y" IN strings'))
        self.assertEqual(names, [])
        
        
    def test_filter_literals(self):
        """
        Test the Python values returned (internaly) for literals by the
        interpretor of filter expression 
        """
        literals = {
            'True': True,
            'TRUE': True,
            'true': True,
            'False': False,
            'FALSE': False,
            'false': False,
            'Null': None,
            'null': None,
            'Null': None,
            '0': 0,
            '123456789101112': 123456789101112,
            '-45': -45,
            '-46.8': -46.8,
            '1.5654353456363e-15': 1.5654353456363e-15,
            '""': '',
            '"2018-05-25"': '2018-05-25',
            '"a\n b\n  c"': 'a\n b\n  c',
            '"\\""': '"',
            '2018-05-25': datetime.date(2018, 5, 25),
            '2018-5-25': datetime.date(2018, 5, 25),
            '12:54': datetime.time(12, 54),
            '02:4:9': datetime.time(2, 4, 9),
            # The following interpretation of microsecond is a strange
            # behavior of datetime.strptime that expect up to 6 digits
            # with zeroes padded on the right !?
            '12:34:56.789': datetime.time(12, 34, 56, 789000),
            '12:34:56.000789': datetime.time(12, 34, 56, 789),
            '2018-05-25T12:34:56.000789': datetime.datetime(2018, 5, 25, 12, 34, 56, 789),
            '2018-5-25T12:34': datetime.datetime(2018, 5, 25, 12, 34),
            '[]': [],
        }
        # Adds the literal for a list of all elements in the dictionary
        literals['[%s]' % ','.join(literals.keys())] = list(literals.values())
        
        parser = populse_db.filter.literal_parser()
        for literal, expected_value in literals.items():
            tree = parser.parse(literal)
            value = populse_db.filter.FilterToQuery(None, None).transform(tree)
            self.assertEqual(value, expected_value)

if __name__ == '__main__':
    unittest.main()
