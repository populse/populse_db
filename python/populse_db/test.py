import os
import shutil
import unittest
import tempfile
import datetime

from sqlalchemy.exc import OperationalError

import populse_db

def create_test_case(**database_creation_parameters):        
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
            if 'string_engine' not in database_creation_parameters:
                database_creation_parameters['string_engine'] = 'sqlite:///' + self.path
            self.string_engine = database_creation_parameters['string_engine']

        def tearDown(self):
            """
            Called after every unit test
            Deletes the temporary folder created for the test
            """

            shutil.rmtree(self.temp_folder)

        def create_database(self, clear=True):
            try:
                db = populse_db.database.Database(**database_creation_parameters)
            except OperationalError as e:
                if database_creation_parameters['string_engine'].startswith('postgresql'):
                    raise unittest.SkipTest(str(e))
                raise
            if clear:
                db.clear()
            return db

        def test_wrong_constructor_parameters(self):
            """
            Tests the parameters of the Database class constructor
            """

            # Testing with wrong query_type
            try:
                populse_db.database.Database("engine", query_type="wrong_query_type")
                self.fail()
            except ValueError:
                pass
            try:
                populse_db.database.Database("engine", query_type=True)
                self.fail()
            except ValueError:
                pass
            # Testing with wrong caches
            try:
                populse_db.database.Database("engine", caches="False")
                self.fail()
            except ValueError:
                pass

        def test_add_field(self):
            """
            Tests the method adding a field
            """

            database = self.create_database()
            with database as session:

                # Adding collection
                session.add_collection("collection1", "name")

                # Testing with a first field
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                # Checking the field properties
                field = session.get_field("collection1", "PatientName")
                self.assertEqual(field.name, "PatientName")
                self.assertEqual(field.type, populse_db.database.FIELD_TYPE_STRING)
                self.assertEqual(field.description, "Name of the patient")
                self.assertEqual(field.collection, "collection1")

                # Testing with a field that already exists
                try:
                    session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
                    self.fail()
                except ValueError:
                    pass

                # Testing with all field types
                session.add_field("collection1", "BandWidth", populse_db.database.FIELD_TYPE_FLOAT, None)
                session.add_field("collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, "with space")
                session.add_field("collection1", "AcquisitionTime", populse_db.database.FIELD_TYPE_TIME, None)
                session.add_field("collection1", "AcquisitionDate", populse_db.database.FIELD_TYPE_DATETIME, None)
                session.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

                session.add_field("collection1", "Bitspervoxel", populse_db.database.FIELD_TYPE_INTEGER, "without space")
                self.assertEqual(session.get_field(
                    "collection1", "Bitspervoxel").description, "without space")
                self.assertEqual(session.get_field(
                    "collection1", "Bits per voxel").description, "with space")
                session.add_field("collection1", "Boolean", populse_db.database.FIELD_TYPE_BOOLEAN, None)
                session.add_field("collection1", "Boolean list", populse_db.database.FIELD_TYPE_LIST_BOOLEAN, None)

                # Testing with wrong parameters
                try:
                    session.add_field("collection_not_existing", "Field", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.add_field(True, "Field", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.add_field("collection1", None, populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.add_field("collection1", "Patient Name", None, None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.add_field("collection1", "Patient Name", populse_db.database.FIELD_TYPE_STRING, 1.5)
                    self.fail()
                except ValueError:
                    pass

                # Testing that the document primary key field is taken
                try:
                    session.add_field("collection1", "name", populse_db.database.FIELD_TYPE_STRING, None)
                    self.fail()
                except ValueError:
                    pass

                # TODO Testing column creation

        def test_remove_field(self):
            """
            Tests the method removing a field
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("current", "name")

                # Adding fields
                session.add_field("current", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
                session.add_field("current", "SequenceName", populse_db.database.FIELD_TYPE_STRING, None)
                session.add_field("current", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("current", document)
                document = {}
                document["name"] = "document2"
                session.add_document("current", document)

                # Adding values
                session.new_value("current", "document1", "PatientName", "Guerbet")
                session.new_value("current", "document1", "SequenceName", "RARE")
                session.new_value("current", "document1", "Dataset dimensions", [1, 2])

                # Removing fields
                session.remove_field("current", "PatientName")
                session.remove_field("current", "Dataset dimensions")

                # Testing that the field does not exist anymore
                self.assertIsNone(session.get_field("current", "PatientName"))
                self.assertIsNone(session.get_field("current", "Dataset dimensions"))

                # Testing that the field values are removed
                self.assertIsNone(session.get_value("current", "document1", "PatientName"))
                self.assertEqual(session.get_value(
                    "current", "document1", "SequenceName"), "RARE")
                self.assertIsNone(session.get_value(
                    "current", "document1", "Dataset dimensions"))

                # Testing with list of fields
                session.remove_field("current", ["SequenceName"])
                self.assertIsNone(session.get_field("current", "SequenceName"))

                # Adding fields again
                session.add_field("current", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
                session.add_field("current", "SequenceName", populse_db.database.FIELD_TYPE_STRING, None)
                session.add_field("current", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

                # Testing with list of fields
                session.remove_field("current", ["SequenceName", "PatientName"])
                self.assertIsNone(session.get_field("current", "SequenceName"))
                self.assertIsNone(session.get_field("current", "PatientName"))

                # Testing with a field not existing
                try:
                    session.remove_field("not_existing", "document1")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.remove_field(1, "NotExisting")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.remove_field("current", "NotExisting")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.remove_field("current", "Dataset dimension")
                    self.fail()
                except ValueError:
                    pass

                # Testing with wrong parameter
                try:
                    session.remove_field("current", 1)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.remove_field("current", None)
                    self.fail()
                except ValueError:
                    pass

                # TODO Testing column removal

        def test_get_field(self):
            """
            Tests the method giving the field row given a field
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding field
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                # Testing that the field is returned if it exists
                self.assertIsNotNone(session.get_field("collection1", "PatientName"))

                # Testing that None is returned if the field does not exist
                self.assertIsNone(session.get_field("collection1", "Test"))

                # Testing that None is returned if the collection does not exist
                self.assertIsNone(session.get_field("collection_not_existing", "PatientName"))

                # Testing that None is returned if both collection and field do not exist
                self.assertIsNone(session.get_field("collection_not_existing", "Test"))

        def test_get_fields(self):
            """
            Tests the method giving the fields rows, given a collection
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding field
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                fields = session.get_fields("collection1")
                self.assertEqual(len(fields), 2)

                session.add_field("collection1", "SequenceName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                fields = session.get_fields("collection1")
                self.assertEqual(len(fields), 3)

                # Adding second collection
                session.add_collection("collection2", "id")

                fields = session.get_fields("collection1")
                self.assertEqual(len(fields), 3)

                # Testing with a collection not existing
                self.assertEqual(session.get_fields("collection_not_existing"), [])

        def test_set_value(self):

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding document
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                # Adding fields
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
                session.add_field(
                    "collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
                session.add_field(
                    "collection1", "AcquisitionDate", populse_db.database.FIELD_TYPE_DATETIME, None)
                session.add_field(
                    "collection1", "AcquisitionTime", populse_db.database.FIELD_TYPE_TIME, None)

                # Adding values and changing it
                session.new_value("collection1", "document1", "PatientName", "test", "test")
                session.set_value("collection1", "document1", "PatientName", "test2")

                session.new_value("collection1", "document1", "Bits per voxel", 1, 1)
                session.set_value("collection1", "document1", "Bits per voxel", 2)

                date = datetime.datetime(2014, 2, 11, 8, 5, 7)
                session.new_value("collection1", "document1", "AcquisitionDate", date, date)
                self.assertEqual(session.get_value("collection1", "document1", "AcquisitionDate"), date)
                date = datetime.datetime(2015, 2, 11, 8, 5, 7)
                session.set_value("collection1", "document1", "AcquisitionDate", date)

                time = datetime.datetime(2014, 2, 11, 0, 2, 20).time()
                session.new_value("collection1", "document1", "AcquisitionTime", time, time)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "AcquisitionTime"), time)
                time = datetime.datetime(2014, 2, 11, 15, 24, 20).time()
                session.set_value("collection1", "document1", "AcquisitionTime", time)

                # Testing that the values are actually set
                self.assertEqual(session.get_value(
                    "collection1", "document1", "PatientName"), "test2")
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Bits per voxel"), 2)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "AcquisitionDate"), date)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "AcquisitionTime"), time)
                session.set_value("collection1", "document1", "PatientName", None)
                self.assertIsNone(session.get_value("collection1", "document1", "PatientName"))

                # Testing when not existing
                try:
                    session.set_value("collection_not_existing", "document3", "PatientName", None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.set_value("collection1", "document3", "PatientName", None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.set_value("collection1", "document1", "NotExisting", None)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.set_value("collection1", "document3", "NotExisting", None)
                    self.fail()
                except ValueError:
                    pass

                # Testing with wrong types
                try:
                    session.set_value("collection1", "document1", "Bits per voxel", "test")
                    self.fail()
                except ValueError:
                    pass
                self.assertEqual(session.get_value("collection1",
                    "document1", "Bits per voxel"), 2)
                try:
                    session.set_value("collection1", "document1", "Bits per voxel", 35.8)
                    self.fail()
                except ValueError:
                    pass
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Bits per voxel"), 2)

                # Testing with wrong parameters
                try:
                    session.set_value(False, "document1", "Bits per voxel", 35)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.set_value("collection1", 1, "Bits per voxel", "2")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.set_value("collection1", "document1", None, "1")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.set_value("collection1", 1, None, True)
                    self.fail()
                except ValueError:
                    pass

                # Testing primary key set impossible
                try:
                    session.set_value("collection1", "document1", "name", None)
                    self.fail()
                except ValueError:
                    pass

        def test_set_values(self):
            """
            Tests the method setting several values of a document
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1")

                # Adding fields
                session.add_field("collection1", "SequenceName", populse_db.database.FIELD_TYPE_STRING)
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING)
                session.add_field("collection1", "BandWidth", populse_db.database.FIELD_TYPE_FLOAT)

                # Adding documents
                session.add_document("collection1", "document1")
                session.add_document("collection1", "document2")

                # Adding values
                session.new_value("collection1", "document1", "SequenceName", "Flash")
                session.new_value("collection1", "document1", "PatientName", "Guerbet")
                session.new_value("collection1", "document1", "BandWidth", 50000)
                self.assertEqual(session.get_value("collection1", "document1", "SequenceName"), "Flash")
                self.assertEqual(session.get_value("collection1", "document1", "PatientName"), "Guerbet")
                self.assertEqual(session.get_value("collection1", "document1", "BandWidth"), 50000)

                # Setting all values
                values = {}
                values["PatientName"] = "Patient"
                values["BandWidth"] = 25000
                session.set_values("collection1", "document1", values)
                self.assertEqual(session.get_value("collection1", "document1", "PatientName"), "Patient")
                self.assertEqual(session.get_value("collection1", "document1", "BandWidth"), 25000)

                # Testing that the primary_key cannot be set
                values = {}
                values["name"] = "document3"
                values["BandWidth"] = 25000
                try:
                    session.set_values("collection1", "document1", values)
                    self.fail()
                except ValueError:
                    pass

                # Trying with field not existing
                values = {}
                values["PatientName"] = "Patient"
                values["BandWidth"] = 25000
                values["Field_not_existing"] = "value"
                try:
                    session.set_values("collection1", "document1", values)
                    self.fail()
                except ValueError:
                    pass

                # Trying with invalid values
                values = {}
                values["PatientName"] = 50
                values["BandWidth"] = 25000
                try:
                    session.set_values("collection1", "document1", values)
                    self.fail()
                except ValueError:
                    pass

        def test_get_field_names(self):
            """
            Tests the method removing a value
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding field
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                fields = session.get_fields_names("collection1")
                self.assertEqual(len(fields), 2)
                self.assertTrue("name" in fields)
                self.assertTrue("PatientName" in fields)

                session.add_field("collection1", "SequenceName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                fields = session.get_fields_names("collection1")
                self.assertEqual(len(fields), 3)
                self.assertTrue("name" in fields)
                self.assertTrue("PatientName" in fields)
                self.assertTrue("SequenceName" in fields)

                # Adding second collection
                session.add_collection("collection2", "id")

                fields = session.get_fields_names("collection1")
                self.assertEqual(len(fields), 3)
                self.assertTrue("name" in fields)
                self.assertTrue("PatientName" in fields)
                self.assertTrue("SequenceName" in fields)

                # Testing with a collection not existing
                self.assertEqual(session.get_fields_names("collection_not_existing"), [])

        def test_get_value(self):
            """
            Tests the method giving the current value, given a document and a field
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                # Adding fields
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
                session.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
                session.add_field("collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
                session.add_field("collection1", "Grids spacing", populse_db.database.FIELD_TYPE_LIST_FLOAT, None)

                # Adding values
                session.new_value("collection1", "document1", "PatientName", "test")
                session.new_value("collection1", "document1", "Bits per voxel", 10)
                session.new_value(
                    "collection1", "document1", "Dataset dimensions", [3, 28, 28, 3])
                session.new_value("collection1", "document1", "Grids spacing", [
                                0.234375, 0.234375, 0.4])

                # Testing that the value is returned if it exists
                self.assertEqual(session.get_value(
                    "collection1", "document1", "PatientName"), "test")
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Bits per voxel"), 10)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Dataset dimensions"), [3, 28, 28, 3])
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Grids spacing"), [0.234375, 0.234375, 0.4])

                # Testing when not existing
                self.assertIsNone(session.get_value("collection_not_existing", "document1", "PatientName"))
                self.assertIsNone(session.get_value("collection1", "document3", "PatientName"))
                self.assertIsNone(session.get_value("collection1", "document1", "NotExisting"))
                self.assertIsNone(session.get_value("collection1", "document3", "NotExisting"))
                self.assertIsNone(session.get_value("collection1", "document2", "Grids spacing"))

                # Testing with wrong parameters
                self.assertIsNone(session.get_value(3, "document1", "Grids spacing"))
                self.assertIsNone(session.get_value("collection1", 1, "Grids spacing"))
                self.assertIsNone(session.get_value("collection1", "document1", None))
                self.assertIsNone(session.get_value("collection1", 3.5, None))

        def test_check_type_value(self):
            """
            Tests the method checking the validity of incoming values
            """

            database = self.create_database()
            with database as session:
                is_valid = session.check_type_value("string", populse_db.database.FIELD_TYPE_STRING)
                self.assertTrue(is_valid)
                is_valid = session.check_type_value(1, populse_db.database.FIELD_TYPE_STRING)
                self.assertFalse(is_valid)
                is_valid = session.check_type_value(None, populse_db.database.FIELD_TYPE_STRING)
                self.assertTrue(is_valid)
                is_valid = session.check_type_value(1, populse_db.database.FIELD_TYPE_INTEGER)
                self.assertTrue(is_valid)
                is_valid = session.check_type_value(1, populse_db.database.FIELD_TYPE_FLOAT)
                self.assertTrue(is_valid)
                is_valid = session.check_type_value(1.5, populse_db.database.FIELD_TYPE_FLOAT)
                self.assertTrue(is_valid)
                is_valid = session.check_type_value(None, None)
                self.assertFalse(is_valid)
                is_valid = session.check_type_value([1.5], populse_db.database.FIELD_TYPE_LIST_FLOAT)
                self.assertTrue(is_valid)
                is_valid = session.check_type_value(1.5, populse_db.database.FIELD_TYPE_LIST_FLOAT)
                self.assertFalse(is_valid)
                is_valid = session.check_type_value(
                    [1.5, "test"], populse_db.database.FIELD_TYPE_LIST_FLOAT)
                self.assertFalse(is_valid)

        def test_new_value(self):
            """
            Tests the method adding a value
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)
                document = {}
                document["name"] = "document2"
                session.add_document("collection1", document)

                # Adding fields
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")
                session.add_field(
                    "collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
                session.add_field("collection1", "BandWidth", populse_db.database.FIELD_TYPE_FLOAT, None)
                session.add_field("collection1", "AcquisitionTime", populse_db.database.FIELD_TYPE_TIME, None)
                session.add_field("collection1", "AcquisitionDate", populse_db.database.FIELD_TYPE_DATETIME, None)
                session.add_field("collection1", "Dataset dimensions", populse_db.database.FIELD_TYPE_LIST_INTEGER, None)
                session.add_field("collection1", "Grids spacing", populse_db.database.FIELD_TYPE_LIST_FLOAT, None)
                session.add_field("collection1", "Boolean", populse_db.database.FIELD_TYPE_BOOLEAN, None)
                session.add_field("collection1", "Boolean list", populse_db.database.FIELD_TYPE_LIST_BOOLEAN, None)

                # Adding values
                session.new_value("collection1", "document1", "PatientName", "test")
                session.new_value("collection1", "document2", "BandWidth", 35.5)
                session.new_value("collection1", "document1", "Bits per voxel", 1)
                session.new_value(
                    "collection1", "document1", "Dataset dimensions", [3, 28, 28, 3])
                session.new_value("collection1", "document2", "Grids spacing", [
                                0.234375, 0.234375, 0.4])
                session.new_value("collection1", "document1", "Boolean", True)

                # Testing when not existing
                try:
                    session.new_value("collection_not_existing", "document1", "PatientName", "test")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", "document1", "NotExisting", "none")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", "document3", "SequenceName", "none")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", "document3", "NotExisting", "none")
                    self.fail()
                except ValueError:
                    pass
                self.assertIsNone(session.new_value("collection1", "document1", "BandWidth", 45))

                date = datetime.datetime(2014, 2, 11, 8, 5, 7)
                session.new_value("collection1", "document1", "AcquisitionDate", date)
                time = datetime.datetime(2014, 2, 11, 0, 2, 2).time()
                session.new_value("collection1", "document1", "AcquisitionTime", time)

                # Testing that the values are actually added
                self.assertEqual(session.get_value(
                    "collection1", "document1", "PatientName"), "test")
                self.assertEqual(session.get_value(
                    "collection1", "document2", "BandWidth"), 35.5)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Bits per voxel"), 1)
                self.assertEqual(session.get_value("collection1", "document1", "BandWidth"), 45)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "AcquisitionDate"), date)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "AcquisitionTime"), time)
                self.assertEqual(session.get_value(
                    "collection1", "document1", "Dataset dimensions"), [3, 28, 28, 3])
                self.assertEqual(session.get_value(
                    "collection1", "document2", "Grids spacing"), [0.234375, 0.234375, 0.4])
                self.assertEqual(session.get_value("collection1", "document1", "Boolean"), True)

                # Test value override
                try:
                    session.new_value("collection1", "document1", "PatientName", "test2", "test2")
                    self.fail()
                except ValueError:
                    pass
                value = session.get_value("collection1", "document1", "PatientName")
                self.assertEqual(value, "test")

                # Testing with wrong types
                try:
                    session.new_value("collection1", "document2", "Bits per voxel",
                                    "space_field", "space_field")
                    self.fail()
                except ValueError:
                    pass
                self.assertIsNone(session.get_value(
                    "collection1", "document2", "Bits per voxel"))
                try:
                    session.new_value("collection1", "document2", "Bits per voxel", 35.5)
                    self.fail()
                except ValueError:
                    pass
                self.assertIsNone(session.get_value(
                    "collection1", "document2", "Bits per voxel"))
                try:
                    session.new_value("collection1", "document1", "BandWidth", "test", "test")
                    self.fail()
                except ValueError:
                    pass
                self.assertEqual(session.get_value("collection1", "document1", "BandWidth"), 45)

                # Testing with wrong parameters
                try:
                    session.new_value(5, "document1", "Grids spacing", "2", "2")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", 1, "Grids spacing", "2", "2")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", "document1", None, "1", "1")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", "document1", "PatientName", None, None)
                    self.fail()
                except ValueError:
                    pass
                self.assertEqual(session.get_value(
                    "collection1", "document1", "PatientName"), "test")
                try:
                    session.new_value("collection1", 1, None, True)
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.new_value("collection1", "document2", "Boolean", "boolean")
                    self.fail()
                except ValueError:
                    pass

        def test_get_document(self):
            """
            Tests the method giving the document row given a document
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding document
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                # Testing that a document is returned if it exists
                self.assertIsInstance(session.get_document(
                    "collection1", "document1").row, session.table_classes["collection1"])

                # Testing that None is returned if the document does not exist
                self.assertIsNone(session.get_document("collection1", "document3"))

                # Testing that None is returned if the collection does not exist
                self.assertIsNone(session.get_document("collection_not_existing", "document1"))

                # Testing with wrong parameter
                self.assertIsNone(session.get_document(False, "document1"))
                self.assertIsNone(session.get_document("collection1", None))
                self.assertIsNone(session.get_document("collection1", 1))

        def test_remove_document(self):
            """
            Tests the method removing a document
            """
            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)
                document = {}
                document["name"] = "document2"
                session.add_document("collection1", document)

                # Adding field
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING, "Name of the patient")

                # Adding value
                session.new_value("collection1", "document1", "PatientName", "test")

                # Removing document
                session.remove_document("collection1", "document1")

                # Testing that the document is removed from all tables
                self.assertIsNone(session.get_document("collection1", "document1"))

                # Testing that the values associated are removed
                self.assertIsNone(session.get_value("collection1", "document1", "PatientName"))

                # Testing with a collection not existing
                try:
                    session.remove_document("collection_not_existing", "document1")
                    self.fail()
                except ValueError:
                    pass

                # Testing with a document not existing
                try:
                    session.remove_document("collection1", "NotExisting")
                    self.fail()
                except ValueError:
                    pass

                # Removing document
                session.remove_document("collection1", "document2")

                # Testing that the document is removed from document (and initial) tables
                self.assertIsNone(session.get_document("collection1", "document2"))

                # Removing document a second time
                try:
                    session.remove_document("collection1", "document1")
                    self.fail()
                except ValueError:
                    pass

        def test_add_document(self):
            """
            Tests the method adding a document
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding field
                session.add_field("collection1", "List", populse_db.database.FIELD_TYPE_LIST_INTEGER)
                session.add_field("collection1", "Int", populse_db.database.FIELD_TYPE_INTEGER)

                # Adding document
                document = {}
                document["name"] = "document1"
                document["List"] = [1, 2, 3]
                document["Int"] = 5
                session.add_document("collection1", document)

                # Testing that the document has been added
                document = session.get_document("collection1", "document1")
                self.assertIsInstance(document.row, session.table_classes["collection1"])
                self.assertEqual(document.name, "document1")

                # Testing when trying to add a document that already exists
                try:
                    document = {}
                    document["name"] = "document1"
                    session.add_document("collection1", document)
                    self.fail()
                except ValueError:
                    pass

                # Testing with invalid parameters
                try:
                    session.add_document(15, "document1")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.add_document("collection_not_existing", "document1")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.add_document("collection1", True)
                    self.fail()
                except ValueError:
                    pass

                # Testing the add of several documents
                document = {}
                document["name"] = "document2"
                session.add_document("collection1", document)

                # Adding document with dictionary without primary key
                try:
                    document = {}
                    document["no_primary_key"] = "document1"
                    session.add_document("collection1", document)
                    self.fail()
                except ValueError:
                    pass

        def test_add_collection(self):
            """
            Tests the method adding a collection
            """

            database = self.create_database()
            with database as session:
                # Adding a first collection
                session.add_collection("collection1")

                # Checking values
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, "name")

                # Adding a second collection
                session.add_collection("collection2", "id")

                # Checking values
                collection = session.get_collection("collection2")
                self.assertEqual(collection.name, "collection2")
                self.assertEqual(collection.primary_key, "id")

                # Trying with a collection already existing
                try:
                    session.add_collection("collection1")
                    self.fail()
                except ValueError:
                    pass

                # Trying with table names already taken
                try:
                    session.add_collection("field")
                    self.fail()
                except ValueError:
                    pass

                try:
                    session.add_collection("collection")
                    self.fail()
                except ValueError:
                    pass

        def test_remove_collection(self):
            """
            Tests the method removing a collection
            """

            database = self.create_database()
            with database as session:
                # Adding a first collection
                session.add_collection("collection1")

                # Checking values
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, "name")

                # Removing collection
                session.remove_collection("collection1")

                # Testing that it does not exist anymore
                self.assertIsNone(session.get_collection("collection1"))

                # Adding new collections
                session.add_collection("collection1")
                session.add_collection("collection2")

                # Checking values
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, "name")
                collection = session.get_collection("collection2")
                self.assertEqual(collection.name, "collection2")
                self.assertEqual(collection.primary_key, "name")

                # Removing one collection and testing that the other is unchanged
                session.remove_collection("collection2")
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, "name")
                self.assertIsNone(session.get_collection("collection2"))

                # Adding field
                session.add_field("collection1", "Field", populse_db.database.FIELD_TYPE_STRING)
                field = session.get_field("collection1", "Field")
                self.assertEqual(field.name, "Field")
                self.assertEqual(field.collection, "collection1")
                self.assertIsNone(field.description)
                self.assertEqual(field.type, populse_db.database.FIELD_TYPE_STRING)

                # Adding document
                session.add_document("collection1", "document")
                document = session.get_document("collection1", "document")
                self.assertEqual(document.name, "document")

                # Removing the collection containing the field and the document and testing that everything is None
                session.remove_collection("collection1")
                self.assertIsNone(session.get_collection("collection1"))
                self.assertIsNone(session.get_field("collection1", "name"))
                self.assertIsNone(session.get_field("collection1", "Field"))
                self.assertIsNone(session.get_document("collection1", "document"))

                # Testing with a collection not existing
                try:
                    session.remove_collection("collection_not_existing")
                    self.fail()
                except ValueError:
                    pass
                try:
                    session.remove_collection(True)
                    self.fail()
                except ValueError:
                    pass

        def test_get_collection(self):
            """
            Tests the method giving the collection row
            """

            database = self.create_database()
            with database as session:
                # Adding a first collection
                session.add_collection("collection1")

                # Checking values
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, "name")

                # Adding a second collection
                session.add_collection("collection2", "id")

                # Checking values
                collection = session.get_collection("collection2")
                self.assertEqual(collection.name, "collection2")
                self.assertEqual(collection.primary_key, "id")

                # Trying with a collection not existing
                self.assertIsNone(session.get_collection("collection3"))

                # Trying with a table name already existing
                self.assertIsNone(session.get_collection("collection"))
                self.assertIsNone(session.get_collection("field"))

        def test_get_collections(self):
            """
            Tests the method giving the list of collections rows
            """

            database = self.create_database()
            with database as session:
                # Testing that there is no collection at first
                self.assertEqual(session.get_collections(), [])

                # Adding collection
                session.add_collection("collection1")

                collections = session.get_collections()
                self.assertEqual(len(collections), 1)
                self.assertEqual(collections[0].name, "collection1")

                session.add_collection("collection2")

                collections = session.get_collections()
                self.assertEqual(len(collections), 2)

                session.remove_collection("collection2")

                collections = session.get_collections()
                self.assertEqual(len(collections), 1)
                self.assertEqual(collections[0].name, "collection1")

        def test_get_collections_names(self):
            """
            Tests the method giving the collections names
            """

            database = self.create_database()
            with database as session:
                # Testing that there is no collection at first
                self.assertEqual(session.get_collections_names(), [])

                # Adding collection
                session.add_collection("collection1")

                self.assertEqual(session.get_collections_names(), ["collection1"])

                session.add_collection("collection2")

                collections = session.get_collections_names()
                self.assertEqual(len(collections), 2)
                self.assertTrue("collection1" in collections)
                self.assertTrue("collection2" in collections)

                session.remove_collection("collection2")

                self.assertEqual(session.get_collections_names(), ["collection1"])

        def test_get_documents(self):
            """
            Tests the method returning the list of document rows, given a collection
            """

            database = self.create_database()
            with database as session:
                # Adding collections
                session.add_collection("collection1", "name")
                session.add_collection("collection2", "id")

                session.add_document("collection1", "document1")
                session.add_document("collection1", "document2")
                session.add_document("collection2", "document1")
                session.add_document("collection2", "document2")

                documents1 = session.get_documents("collection1")
                self.assertEqual(len(documents1), 2)

                documents2 = session.get_documents("collection2")
                self.assertEqual(len(documents2), 2)

                # Testing with collection not existing
                self.assertEqual(session.get_documents("collection_not_existing"), [])
                self.assertEqual(session.get_documents("collection"), [])
                self.assertEqual(session.get_documents(None), [])

        def test_get_documents_names(self):
            """
            Tests the method returning the list of document names, given a collection
            """

            database = self.create_database()
            with database as session:
                # Adding collections
                session.add_collection("collection1", "name")
                session.add_collection("collection2", "id")

                session.add_document("collection1", "document1")
                session.add_document("collection1", "document2")
                session.add_document("collection2", "document3")
                session.add_document("collection2", "document4")

                documents1 = session.get_documents_names("collection1")
                self.assertEqual(len(documents1), 2)
                self.assertTrue("document1" in documents1)
                self.assertTrue("document2" in documents1)

                documents2 = session.get_documents_names("collection2")
                self.assertEqual(len(documents2), 2)
                self.assertTrue("document3" in documents2)
                self.assertTrue("document4" in documents2)

                # Testing with collection not existing
                self.assertEqual(session.get_documents_names("collection_not_existing"), [])
                self.assertEqual(session.get_documents_names("collection"), [])
                self.assertEqual(session.get_documents_names(None), [])

        def test_remove_value(self):
            """
            Tests the method removing a value
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding document
                session.add_document("collection1", "document1")

                # Adding fields
                session.add_field("collection1", "PatientName", populse_db.database.FIELD_TYPE_STRING,
                                            "Name of the patient")
                session.add_field("collection1", "Bits per voxel", populse_db.database.FIELD_TYPE_INTEGER, None)
                session.add_field("collection1", "Dataset dimensions",
                                            populse_db.database.FIELD_TYPE_LIST_INTEGER, None)

                # Adding values
                session.new_value("collection1", "document1", "PatientName", "test")

                try:
                    session.new_value("collection1", "document1", "Bits per voxel", "space_field")
                    self.fail()
                except ValueError:
                    pass
                session.new_value("collection1", "document1", "Dataset dimensions", [3, 28, 28, 3])
                value = session.get_value("collection1", "document1", "Dataset dimensions")
                self.assertEqual(value, [3, 28, 28, 3])

                # Removing values
                session.remove_value("collection1", "document1", "PatientName")
                session.remove_value("collection1", "document1", "Bits per voxel")
                session.remove_value("collection1", "document1", "Dataset dimensions")

                # Testing when not existing

                try:
                    session.remove_value("collection_not_existing", "document1", "PatientName")
                    self.fail()
                except ValueError:
                    pass

                try:
                    session.remove_value("collection1", "document3", "PatientName")
                    self.fail()
                except ValueError:
                    pass

                try:
                    session.remove_value("collection1", "document1", "NotExisting")
                    self.fail()
                except ValueError:
                    pass

                try:
                    session.remove_value("collection1", "document3", "NotExisting")
                    self.fail()
                except ValueError:
                    pass

                # Testing that the values are actually removed
                self.assertIsNone(session.get_value("collection1", "document1", "PatientName"))
                self.assertIsNone(session.get_value("collection1", "document1", "Bits per voxel"))
                self.assertIsNone(session.get_value("collection1", "document1", "Dataset dimensions"))

        def test_list_dates(self):
            """
            Tests the storage and retrieval of fields of type list of time, date
            and datetime
            """

            database = self.create_database()
            with database as session:
                # Adding collection
                session.add_collection("collection1", "name")

                # Adding fields
                session.add_field("collection1", "list_date", populse_db.database.FIELD_TYPE_LIST_DATE, None)
                session.add_field("collection1", "list_time", populse_db.database.FIELD_TYPE_LIST_TIME, None)
                session.add_field("collection1", "list_datetime", populse_db.database.FIELD_TYPE_LIST_DATETIME, None)

                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                list_date = [datetime.date(2018, 5, 23), datetime.date(1899, 12, 31)]
                list_time = [datetime.time(12, 41, 33, 540), datetime.time(1, 2, 3)]
                list_datetime = [datetime.datetime(2018, 5, 23, 12, 41, 33, 540),
                                datetime.datetime(1899, 12, 31, 1, 2, 3)]

                session.new_value("collection1", "document1", "list_date", list_date)
                self.assertEqual(
                    list_date, session.get_value("collection1", "document1", "list_date"))
                session.new_value("collection1", "document1", "list_time", list_time)
                self.assertEqual(
                    list_time, session.get_value("collection1", "document1", "list_time"))
                session.new_value("collection1", "document1", "list_datetime", list_datetime)
                self.assertEqual(list_datetime, session.get_value(
                    "collection1", "document1", "list_datetime"))

        def test_filters(self):
            list_datetime = [datetime.datetime(2018, 5, 23, 12, 41, 33, 540),
                            datetime.datetime(1981, 5, 8, 20, 0),
                            datetime.datetime(1899, 12, 31, 1, 2, 3)]

            database = self.create_database()
            with database as session:
                session.add_collection("collection1", "name")

                session.add_field("collection1", 'format', field_type=populse_db.database.FIELD_TYPE_STRING, description=None, index=True)
                session.add_field("collection1", 'strings', field_type=populse_db.database.FIELD_TYPE_LIST_STRING, description=None)
                session.add_field("collection1", 'datetime', field_type=populse_db.database.FIELD_TYPE_DATETIME, description=None)
                session.add_field("collection1", 'has_format', field_type=populse_db.database.FIELD_TYPE_BOOLEAN, description=None)

                session.save_modifications()
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
                                has_format=True,
                            )
                            session.add_document("collection1", document)
                        document = '/%s_%d.none' % (file, date.year)
                        session.add_document("collection1", dict(name=document, strings=list(file)))
                #session.save_modifications()

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
                    
                    ('format IN ["DICOM", "NIFTI"]',
                    {
                    '/xyz_1899.nii',
                    '/xyz_2018.dcm',
                    '/bcd_1899.nii',
                    '/def_1899.nii',
                    '/abc_1981.nii',
                    '/abc_1899.nii',
                    '/bcd_2018.nii',
                    '/abc_2018.dcm',
                    '/bcd_1899.dcm',
                    '/def_1981.dcm',
                    '/abc_2018.nii',
                    '/abc_1981.dcm',
                    '/bcd_2018.dcm',
                    '/def_2018.nii',
                    '/def_2018.dcm',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/def_1899.dcm',
                    '/bcd_1981.nii',
                    '/xyz_1981.nii',
                    '/xyz_2018.nii',
                    '/xyz_1981.dcm',
                    '/def_1981.nii',
                    '/bcd_1981.dcm',
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
                    
                    ('has_format in [false, null]',
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

                    ('name like "%.nii"',
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

                    ('name ilike "%A%"',
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
                    try:
                        documents = set(document.name for document in session.filter_documents("collection1", filter))
                        self.assertEqual(documents, expected)
                    except Exception as e:
                        session.unsave_modifications()
                        query = session.filter_query('collection1', filter)
                        e.message = 'While testing filter : %s\n%s' % (str(filter), e.message)
                        e.args = (e.message,)
                        raise

        def test_modify_list_field(self):
            database = self.create_database()
            with database as session:
                session.add_collection("collection1", "name")
                session.add_field("collection1", 'strings', field_type=populse_db.database.FIELD_TYPE_LIST_STRING, description=None)
                session.add_document("collection1", 'test')
                session.new_value("collection1", 'test', 'strings', ['a', 'b', 'c'])
                session.save_modifications()
                names = list(document.name for document in session.filter_documents("collection1", '"b" IN strings'))
                self.assertEqual(names, ['test'])
                
                session.set_value("collection1", 'test', 'strings', ['x', 'y', 'z'])
                session.save_modifications()
                names = list(document.name for document in session.filter_documents("collection1", '"b" IN strings'))
                self.assertEqual(names, [])
                names = list(document.name for document in session.filter_documents("collection1", '"z" IN strings'))
                self.assertEqual(names, ['test'])

                session.remove_value("collection1", 'test', 'strings')
                session.save_modifications()
                names = list(document.name for document in session.filter_documents("collection1", '"y" IN strings'))
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

        def test_with(self):
            database = self.create_database()
            try:
                with database as session:
                    session.add_collection("collection1", "name")
                    session.add_document("collection1", {"name": "toto"})
                    boom # Raises an exception, modifications are rolled back
            except NameError:
                pass
            
            with database as session:
                session.add_collection("collection1", "name")
                session.add_document("collection1", {"name": "titi"})
            
            # Reopen the database to check that "titi" was commited
            database = self.create_database(clear=False)
            with database as session:
                names = [i.name for i in session.filter_documents("collection1", "all")]
                self.assertEqual(names, ['titi'])
                
                # Check that recursive session creation always return the
                # same object
                with database as session2:
                    self.assertIs(session, session2)
                    with database as session3:
                        self.assertIs(session, session3)
            
            # Check that previous session was released and that
            # a new one is created.
            with database as session4:
                self.assertIsNot(session, session4)

    return TestDatabaseMethods

def load_tests(loader, standard_tests, pattern):
    suite = unittest.TestSuite()

    for params in (dict(caches=False, list_tables=True, query_type='mixed'),
                   dict(caches=True, list_tables=True, query_type='mixed'),
                   dict(caches=False, list_tables=False, query_type='mixed'),
                   dict(caches=True, list_tables=False, query_type='mixed'),
                   dict(caches=False, list_tables=True, query_type='guess'),
                   dict(caches=True, list_tables=True, query_type='guess'),
                   dict(caches=False, list_tables=False, query_type='guess'),
                   dict(caches=True, list_tables=False, query_type='guess')):
        tests = loader.loadTestsFromTestCase(create_test_case(**params))
        suite.addTests(tests)
    
    # Tests with postgresql. All the tests will be skiped if
    # it is not possible to connect to populse_db_tests database.
    tests = loader.loadTestsFromTestCase(create_test_case(
        string_engine='postgresql:///populse_db_tests', 
        caches=False, 
        list_tables=True, 
        query_type='mixed'))
    suite.addTests(tests)
                   
    return suite

if __name__ == '__main__':
    unittest.main()
