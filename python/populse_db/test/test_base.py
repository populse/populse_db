import os
import shutil
import tempfile
import unittest
from datetime import date, datetime, time

from populse_db import Database
from populse_db.database import check_value_type, populse_db_table

# from populse_db.engine.sqlite import SQLiteSession
from populse_db.filter import FilterToSQL, literal_parser


class TestsSQLiteInMemory(unittest.TestCase):
    def test_add_get_document(self):
        now = datetime.now()
        db = Database("sqlite://:memory:", create=True)
        with db as dbs:
            dbs.add_collection("test", "index")
            base_doc = {
                "string": "string",
                "int": 1,
                "float": 1.4,
                "boolean": True,
                "datetime": now,
                "date": now.date(),
                "time": now.time(),
                "dict": {
                    "string": "string",
                    "int": 1,
                    "float": 1.4,
                    "boolean": True,
                },
            }
            doc = base_doc.copy()
            for k, v in base_doc.items():
                lk = f"list_{k}"
                doc[lk] = [v]
            doc["index"] = "test"
            dbs.add_document("test", doc)
            stored_doc = dbs.get_document("test", "test")
            self.maxDiff = None
            self.assertEqual(doc, stored_doc)


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
            self.database_creation_parameters = database_creation_parameters.copy()
            if "database_url" not in self.database_creation_parameters:
                self.temp_folder = tempfile.mkdtemp(prefix="populse_db")
                path = os.path.join(self.temp_folder, "test.db")
                self.database_creation_parameters["database_url"] = "sqlite://" + path
            else:
                self.temp_folder = None
            self.database_url = self.database_creation_parameters["database_url"]

        def tearDown(self):
            """
            Called after every unit test
            Deletes the temporary folder created for the test
            """
            if self.temp_folder:
                shutil.rmtree(self.temp_folder)
                del self.database_creation_parameters["database_url"]
            self.temp_folder = None

        def create_database(self, clear=True, create=True, echo_sql=None):
            """
            Opens the database
            :param clear: Bool to know if the database must be cleared
            """

            try:
                db = Database(
                    **self.database_creation_parameters,
                    create=create,
                    echo_sql=echo_sql,
                )
            except Exception as e:
                if self.database_creation_parameters["database_url"].startswith(
                    "postgresql"
                ):
                    raise unittest.SkipTest(str(e)) from e
                raise
            except ImportError as e:
                if "psycopg2" in str(e) and self.database_creation_parameters[
                    "database_url"
                ].startswith("postgresql"):
                    raise unittest.SkipTest(str(e)) from e
                raise
            if clear:
                with db as dbs:
                    dbs.clear()
            return db

        def test_wrong_constructor_parameters(self):
            """
            Tests the parameters of the Database class constructor
            """
            # Testing with wrong engine
            self.assertRaises(
                ValueError, lambda: Database("engine://something").__enter__()
            )

        def test_add_field(self):
            """
            Tests the method adding a field
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Testing with a first field
                session.add_field(
                    "collection1", "PatientName", str, description="Name of the patient"
                )

                # Checking the field properties
                field = session.get_field("collection1", "PatientName")
                self.assertEqual(field["name"], "PatientName")
                self.assertEqual(field["type"], str)
                self.assertEqual(field["description"], "Name of the patient")
                self.assertEqual(field["collection"], "collection1")

                # Testing with a field that already exists
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.add_field(
                        "collection1", "PatientName", str, "Name of the patient"
                    ),
                )

                # Testing with several field types
                session.add_field("collection1", "BandWidth", float, None)
                session.add_field("collection1", "AcquisitionTime", time, None)
                session.add_field("collection1", "AcquisitionDate", datetime, None)
                session.add_field("collection1", "Dataset dimensions", list[int], None)
                session.add_field("collection1", "Boolean", bool, None)
                session.add_field("collection1", "Boolean list", list[bool], None)

                # Testing with close field names
                session.add_field("collection1", "Bits per voxel", int, "with space")
                session.add_field("collection1", "Bitspervoxel", int, "without space")
                with self.assertRaises(session.database_exceptions):
                    session.add_field("collection1", "bitspervoxel", int, "lower case")
                self.assertEqual(
                    session.get_field("collection1", "Bitspervoxel")["description"],
                    "without space",
                )
                self.assertEqual(
                    session.get_field("collection1", "Bits per voxel")["description"],
                    "with space",
                )
                with self.assertRaises(TypeError):
                    self.assertEqual(
                        session.get_field("collection1", "bitspervoxel")["description"],
                        "lower case",
                    )

                # Testing with wrong parameters
                self.assertRaises(
                    ValueError,
                    lambda: session.add_field(
                        "collection_not_existing", "Field", list[int], None
                    ),
                )
                self.assertRaises(
                    ValueError,
                    lambda: session.add_field(True, "Field", list[int], None),
                )
                self.assertRaises(
                    AttributeError,
                    lambda: session.add_field(
                        "collection1", "Patient Name", None, None
                    ),
                )

                # Testing that the document primary key field is taken
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.add_field("collection1", "name", str, None),
                )

                with self.assertRaises(ValueError):
                    session.remove_field("collection", "name")

        def test_add_fields(self):
            """
            Tests the method adding several fields
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1")

                # Adding several fields
                session.add_field("collection1", "First name", str, "")
                session.add_field("collection1", "Last name", str, "")
                collection_fields = list(session.get_fields_names("collection1"))
                self.assertEqual(len(collection_fields), 3)
                self.assertTrue("primary_key" in collection_fields)
                self.assertTrue("First name" in collection_fields)
                self.assertTrue("Last name" in collection_fields)

        def test_remove_field(self):
            """
            Tests the method removing a field
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("current", "name")

                # Adding fields
                session.add_field("current", "PatientName", str, "Name of the patient")
                session.add_field("current", "SequenceName", str, None)
                session.add_field("current", "Dataset dimensions", list[int], None)

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("current", document)
                document = {
                    "name": "document2",
                    "PatientName": "Guerbet",
                    "SequenceName": "RARE",
                    "Dataset dimensions": [1, 2],
                }
                document["name"] = "document2"
                session.add_document("current", document)

                # Removing fields
                session.remove_field("current", "PatientName")
                session.remove_field("current", "Dataset dimensions")

                # Testing that the field does not exist anymore
                self.assertIsNone(session.get_field("current", "PatientName"))
                self.assertIsNone(session.get_field("current", "Dataset dimensions"))

                # Testing that the field values are removed
                with self.assertRaises(KeyError):
                    session["current"]["document1"]["PatientName"]
                with self.assertRaises(KeyError):
                    session["current"]["document2"]["PatientName"]
                self.assertIsNone(session["current"]["document1"]["SequenceName"])
                self.assertEqual(
                    session["current"]["document2"]["SequenceName"],
                    "RARE",
                )
                with self.assertRaises(KeyError):
                    session["current"]["document1"]["Dataset dimensions"]
                with self.assertRaises(KeyError):
                    session["current"]["document2"]["Dataset dimensions"]

                # /\ Deleting a list of fields is not yet implemented in populse_db 3.0! /\
                # Testing with list of fields
                # session.remove_field("current", ["SequenceName"])
                # self.assertIsNone(session.get_field("current", "SequenceName"))

                # Adding fields again
                session.add_field("current", "PatientName", str, "Name of the patient")
                # session.add_field("current", "SequenceName", str, None)
                session.add_field("current", "Dataset dimensions", list[int], None)

                # Testing with list of fields
                # session.remove_field("current", ["SequenceName", "PatientName"])
                # self.assertIsNone(session.get_field("current", "SequenceName"))
                # self.assertIsNone(session.get_field("current", "PatientName"))

                # Testing with a field not existing
                self.assertRaises(
                    ValueError,
                    lambda: session.remove_field("not_existing", "document1"),
                )
                self.assertRaises(
                    ValueError, lambda: session.remove_field(1, "NotExisting")
                )
                import sqlite3

                self.assertRaises(
                    sqlite3.OperationalError,
                    lambda: session.remove_field("current", "NotExisting"),
                )
                self.assertRaises(
                    sqlite3.OperationalError,
                    lambda: session.remove_field("current", "Dataset dimension"),
                )
                # /\ Deleting a list of fields is not yet implemented in populse_db 3.0! /\
                # self.assertRaises(
                #     ValueError,
                #     lambda: session.remove_field(
                #         "current", ["SequenceName", "PatientName", "Not_Existing"]
                #     ),
                # )

                # Testing with wrong parameters
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.remove_field("current", 1),
                )
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.remove_field("current", None),
                )

                # /\ Deleting a list of fields is not yet implemented in populse_db 3.0! /\
                # Removing list of fields with list type
                # session.add_field("current", "list1", list[int], None)
                # session.add_field("current", "list2", list[str], None)
                # session.remove_field("current", ["list1", "list2"])
                # self.assertIsNone(session.get_field("current", "list1"))
                # self.assertIsNone(session.get_field("current", "list2"))

        def test_get_field(self):
            """
            Tests the method giving the field row given a field
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding a field
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )

                # Testing that the field is returned if it exists
                self.assertIsNotNone(session.get_field("collection1", "PatientName"))

                # Testing that None is returned if the field does not exist
                self.assertIsNone(session.get_field("collection1", "Test"))

                # Testing that None is returned if the collection does not exist
                self.assertIsNone(
                    session.get_field("collection_not_existing", "PatientName")
                )

                # Testing that None is returned if both collection and field do not exist
                self.assertIsNone(session.get_field("collection_not_existing", "Test"))

        def test_get_fields(self):
            """
            Tests the method giving all fields rows, given a collection
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding a field
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )

                fields = session.get_fields("collection1")
                self.assertEqual(len(fields), 2)

                session.add_field(
                    "collection1", "SequenceName", str, "Name of the patient"
                )

                fields = session.get_fields("collection1")
                self.assertEqual(len(fields), 3)

                # Adding a second collection
                session.add_collection("collection2", "id")

                fields = session.get_fields("collection1")
                self.assertEqual(len(fields), 3)

                # Testing with a collection not existing
                self.assertEqual(
                    list(session.get_fields("collection_not_existing")), []
                )

        def test_set_values(self):
            """
            Tests the method setting several values of a document
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1")

                # Adding fields
                session.add_field("collection1", "SequenceName", str)
                session.add_field("collection1", "PatientName", str)
                session.add_field("collection1", "BandWidth", float)

                # Adding documents
                session["collection1"]["document1"] = {
                    "SequenceName": "Flash",
                    "PatientName": "Guerbet",
                    "BandWidth": 50000,
                }
                session["collection1"]["document2"] = {}

                # Adding values
                document1 = session["collection1"]["document1"]
                self.assertEqual(document1["SequenceName"], "Flash")
                self.assertEqual(document1["PatientName"], "Guerbet")
                self.assertEqual(document1["BandWidth"], 50000)

                # Setting all values
                values = {}
                values["PatientName"] = "Patient"
                values["BandWidth"] = 25000
                session.set_values("collection1", "document1", values)
                document1 = session["collection1"]["document1"]
                self.assertEqual(document1["SequenceName"], "Flash")
                self.assertEqual(document1["PatientName"], "Patient")
                self.assertEqual(document1["BandWidth"], 25000)

                # Testing that the primary_key cannot be set
                values = {"primary_key": "document3", "BandWidth": 25000}
                with self.assertRaises(ValueError):
                    session.set_values("collection1", "document1", values)

                # Trying with the field not existing
                values = {}
                values["PatientName"] = "Patient"
                values["BandWidth"] = 25000
                values["Field_not_existing"] = "value"
                session.set_values("collection1", "document1", values)

                # Trying with invalid values
                values = {}
                values["PatientName"] = 50
                values["BandWidth"] = 25000
                session.set_values("collection1", "document1", values)
                self.assertRaises(
                    AttributeError,
                    lambda: session.set_values("collection1", "document1", True),
                )

                # Trying with the collection not existing
                values = {}
                values["PatientName"] = "Guerbet"
                values["BandWidth"] = 25000
                self.assertRaises(
                    ValueError,
                    lambda: session.set_values(
                        "collection_not_existing", "document1", values
                    ),
                )

                # Trying with the document not existing
                values = {}
                values["PatientName"] = "Guerbet"
                values["BandWidth"] = 25000
                with self.assertRaises(ValueError):
                    session.set_values("collection1", "document_not_existing", values)

                # Testing with list values
                session.add_field("collection1", "list1", list[str])
                session.add_field("collection1", "list2", list[int])
                values = {}
                values["list1"] = ["a", "a", "a"]
                values["list2"] = [1, 1, 1]
                session.set_values("collection1", "document1", values)
                self.assertEqual(
                    session["collection1"]["document1"]["list1"], ["a", "a", "a"]
                )
                self.assertEqual(
                    session["collection1"]["document1"]["list2"], [1, 1, 1]
                )

        def test_get_field_names(self):
            """
            Tests the method removing a value
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding a field
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )

                fields = session.get_fields_names("collection1")
                self.assertEqual(len(fields), 2)
                self.assertTrue("name" in fields)
                self.assertTrue("PatientName" in fields)

                session.add_field(
                    "collection1", "SequenceName", str, "Name of the patient"
                )

                fields = session.get_fields_names("collection1")
                self.assertEqual(len(fields), 3)
                self.assertTrue("name" in fields)
                self.assertTrue("PatientName" in fields)
                self.assertTrue("SequenceName" in fields)

                # Adding a second collection
                session.add_collection("collection2", "id")

                fields = session.get_fields_names("collection1")
                self.assertEqual(len(fields), 3)
                self.assertTrue("name" in fields)
                self.assertTrue("PatientName" in fields)
                self.assertTrue("SequenceName" in fields)

                # Testing with a collection not existing
                self.assertEqual(
                    list(session.get_fields_names("collection_not_existing")), []
                )

        def test_get_value(self):
            """
            Tests the method giving the current value, given a document and a field
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                # Adding fields
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )
                session.add_field("collection1", "Dataset dimensions", list[int], None)
                session.add_field("collection1", "Bits per voxel", int, None)
                session.add_field("collection1", "Grids spacing", list[float], None)

                # Adding values
                document["PatientName"] = "test"
                document["Bits per voxel"] = 10
                document["Dataset dimensions"] = [3, 28, 28, 3]
                document["Grids spacing"] = [0.234375, 0.234375, 0.4]
                session["collection1"].add(document, replace=True)

                # Testing that the value is returned if it exists
                document1 = session["collection1"]["document1"]
                self.assertEqual(document1["PatientName"], "test")
                self.assertEqual(document1["Bits per voxel"], 10)
                self.assertEqual(document1["Dataset dimensions"], [3, 28, 28, 3])
                self.assertEqual(document1["Grids spacing"], [0.234375, 0.234375, 0.4])

        def test_check_type_value(self):
            """
            Tests the method checking the validity of incoming values
            """

            database = self.create_database()
            with database as _:
                self.assertTrue(check_value_type("string", str))
                self.assertFalse(check_value_type(1, str))
                self.assertTrue(check_value_type(None, str))
                self.assertTrue(check_value_type(1, int))
                self.assertTrue(check_value_type(1, float))
                self.assertTrue(check_value_type(1.5, float))
                self.assertTrue(check_value_type([1.5], list[float]))
                self.assertFalse(check_value_type(1.5, list[float]))
                self.assertFalse(check_value_type([1.5, "test"], list[float]))
                value = {}
                value["test1"] = 1
                value["test2"] = 2
                self.assertTrue(check_value_type(value, dict))
                value2 = {}
                value2["test3"] = 1
                value2["test4"] = 2
                self.assertTrue(check_value_type([value, value2], list[dict]))

        def test_add_value(self):
            """
            Tests the method adding a value
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)
                document = {}
                document["name"] = "document2"
                session.add_document("collection1", document)

                # Adding fields
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )
                session.add_field("collection1", "Bits per voxel", int, None)
                session.add_field("collection1", "BandWidth", float, None)
                session.add_field("collection1", "AcquisitionTime", time, None)
                session.add_field("collection1", "AcquisitionDate", datetime, None)
                session.add_field("collection1", "Dataset dimensions", list[int], None)
                session.add_field("collection1", "Grids spacing", list[float], None)
                session.add_field("collection1", "Boolean", bool, None)
                session.add_field("collection1", "Boolean list", list[bool], None)

                # Adding values
                collection1 = session["collection1"]

                document1 = collection1["document1"]
                document1["PatientName"] = "test"
                document1["Bits per voxel"] = 1
                document1["Dataset dimensions"] = [3, 28, 28, 3]
                document1["Boolean"] = True
                d = datetime(2014, 2, 11, 8, 5, 7)
                document1["AcquisitionDate"] = d
                t = datetime(2014, 2, 11, 0, 2, 2).time()
                document1["AcquisitionTime"] = t
                document1["BandWidth"] = 45
                collection1["document1"] = document1

                document2 = collection1["document2"]
                document2["Grids spacing"] = [0.234375, 0.234375, 0.4]
                document2["BandWidth"] = 35.5
                collection1["document2"] = document2

                # Testing that the values are actually added
                document1 = session["collection1"]["document1"]
                document2 = session["collection1"]["document2"]
                self.assertEqual(document1["PatientName"], "test")
                self.assertEqual(document2["BandWidth"], 35.5)
                self.assertEqual(document1["Bits per voxel"], 1)
                self.assertEqual(document1["BandWidth"], 45)
                self.assertEqual(document1["AcquisitionDate"], d)
                self.assertEqual(document1["AcquisitionTime"], t)
                self.assertEqual(document1["Dataset dimensions"], [3, 28, 28, 3])
                self.assertEqual(document2["Grids spacing"], [0.234375, 0.234375, 0.4])
                self.assertEqual(document1["Boolean"], True)

        def test_update_document(self):
            database = self.create_database()
            with database as session:
                session.add_collection("collection")
                collection = session["collection"]
                collection.add_field("status", str)
                collection.add_field("executable", dict)
                collection.add_field("execution_context", dict)

                doc = {
                    "primary_key": "doc",
                    "status": "submitted",
                    "executable": {"definition": "custom"},
                    "execution_context": {"tmp": "/tmp"},
                }
                collection["doc"] = doc
                self.assertEqual(collection["doc"], doc)

                collection.update_document(
                    "doc", {"status": "running", "other": "something"}
                )
                doc.update({"status": "running", "other": "something"})
                self.assertEqual(collection["doc"], doc)

        def test_update_document_without_field(self):
            database = self.create_database()
            with database as session:
                session.add_collection("collection")
                collection = session["collection"]

                doc = {
                    "primary_key": "doc",
                    "status": "submitted",
                    "executable": {"definition": "custom"},
                    "execution_context": {"tmp": "/tmp"},
                }
                collection["doc"] = doc
                self.assertEqual(collection["doc"], doc)

                collection.update_document(
                    "doc", {"status": "running", "other": "something"}
                )
                doc.update({"status": "running", "other": "something"})
                self.assertEqual(collection["doc"], doc)

        def test_get_document(self):
            """
            Tests the method giving the document row given a document
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding a document
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                # Testing that a document is returned if it exists
                self.assertIsNotNone(session.get_document("collection1", "document1"))

                # Testing that None is returned if the document does not exist
                self.assertIsNone(session.get_document("collection1", "document3"))

                # Testing that None is returned if the collection does not exist
                self.assertIsNone(
                    session.get_document("collection_not_existing", "document1")
                )

                # Testing with wrong parameters
                self.assertIsNone(session.get_document(False, "document1"))
                self.assertIsNone(session.get_document("collection1", None))
                self.assertIsNone(session.get_document("collection1", 1))

        def test_remove_document(self):
            """
            Tests the method removing a document
            """
            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding documents
                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)
                document = {}
                document["name"] = "document2"
                session.add_document("collection1", document)

                # Adding a field
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )
                session.add_field("collection1", "FOV", list[int], None)

                # Adding a value
                document = session["collection1"]["document1"]
                document["PatientName"] = "test"
                document["FOV"] = [1, 2, 3]
                session["collection1"]["document1"] = document

                # Removing a document
                session.remove_document("collection1", "document1")

                # Testing that the document is removed from all tables
                self.assertIsNone(session.get_document("collection1", "document1"))

                # Testing with a collection not existing
                self.assertRaises(
                    ValueError,
                    lambda: session.remove_document(
                        "collection_not_existing", "document1"
                    ),
                )

                # Removing a document
                session.remove_document("collection1", "document2")

                # Testing that the document is removed from the collection
                self.assertIsNone(session.get_document("collection1", "document2"))

        def test_add_document(self):
            """
            Tests the method adding a document
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding fields
                session.add_field("collection1", "List", list[int])
                session.add_field("collection1", "Int", int)

                # Adding a document
                document = {}
                document["name"] = "document1"
                document["List"] = [1, 2, 3]
                document["Int"] = 5
                session.add_document("collection1", document)

                # Testing that the document has been added
                document = session.get_document("collection1", "document1")
                self.assertEqual(document["name"], "document1")

                # Testing when trying to add a document that already exists
                document = {}
                document["name"] = "document1"
                with self.assertRaises(session.database_exceptions):
                    session.add_document("collection1", document)

                # Testing with invalid parameters
                self.assertRaises(
                    ValueError, lambda: session.add_document(15, "document1")
                )
                self.assertRaises(
                    ValueError,
                    lambda: session.add_document(
                        "collection_not_existing", "document1"
                    ),
                )
                self.assertRaises(
                    AttributeError, lambda: session.add_document("collection1", True)
                )

                # Testing the add of several documents
                document = {}
                document["name"] = "document2"
                session.add_document("collection1", document)

                # Adding a document with a dictionary without the primary key
                document = {}
                document["no_primary_key"] = "document1"
                with self.assertRaises(session.database_exceptions):
                    session.add_document("collection1", document)

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
                self.assertEqual(collection.primary_key, {"primary_key": str})

                # Adding a second collection
                session.add_collection("collection2", "id")

                # Checking values
                collection = session.get_collection("collection2")
                self.assertEqual(collection.name, "collection2")
                self.assertEqual(collection.primary_key, {"id": str})

                # Trying with a collection already existing
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.add_collection("collection1"),
                )

                # Trying with table names already taken
                session.add_field("collection1", "test", str, description="Test field")
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.add_collection(populse_db_table),
                )

                # Trying with wrong types
                self.assertRaises(
                    AttributeError,
                    lambda: session.add_collection("collection_valid", True),
                )

        def test_remove_collection(self):
            """
            Tests the method removing a collection
            """

            database = self.create_database()
            with database as session:
                # Adding a first collection
                self.assertFalse(session.has_collection("collection1"))
                session.add_collection("collection1")
                self.assertTrue(session.has_collection("collection1"))

                # Checking values
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, {"primary_key": str})

                # Removing a collection
                session.remove_collection("collection1")
                self.assertFalse(session.has_collection("collection1"))

                # Testing that it does not exist anymore
                self.assertIsNone(session.get_collection("collection1"))

                # Adding new collections
                session.add_collection("collection1")
                session.add_collection("collection2")

                # Checking values
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, {"primary_key": str})
                collection = session.get_collection("collection2")
                self.assertEqual(collection.name, "collection2")
                self.assertEqual(collection.primary_key, {"primary_key": str})

                # Removing one collection and testing that the other is unchanged
                session.remove_collection("collection2")
                collection = session.get_collection("collection1")
                self.assertEqual(collection.name, "collection1")
                self.assertEqual(collection.primary_key, {"primary_key": str})
                self.assertIsNone(session.get_collection("collection2"))

                # Adding a field
                session.add_field("collection1", "Field", str)
                field = session.get_field("collection1", "Field")
                self.assertEqual(field["name"], "Field")
                self.assertEqual(field["collection"], "collection1")
                self.assertIsNone(field["description"])
                self.assertEqual(field["type"], str)

                # Adding a document
                session["collection1"]["document"] = {}
                document = session.get_document("collection1", "document")
                self.assertEqual(document["primary_key"], "document")

                # Removing the collection containing the field and the document and testing that it is indeed removed
                session.remove_collection("collection1")
                self.assertIsNone(session.get_collection("collection1"))
                self.assertIsNone(session.get_field("collection1", "name"))
                self.assertIsNone(session.get_field("collection1", "Field"))
                self.assertIsNone(session.get_document("collection1", "document"))

                # Testing with a collection not existing
                self.assertRaises(
                    session.database_exceptions,
                    lambda: session.remove_collection("collection_not_existing"),
                )
                self.assertRaises(
                    session.database_exceptions, lambda: session.remove_collection(True)
                )

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
                self.assertEqual(collection.primary_key, {"primary_key": str})

                # Adding a second collection
                session.add_collection("collection2", "id")

                # Checking values
                collection = session.get_collection("collection2")
                self.assertEqual(collection.name, "collection2")
                self.assertEqual(collection.primary_key, {"id": str})

                # Trying with a collection not existing
                self.assertIsNone(session.get_collection("collection3"))

        def test_get_collections(self):
            """
            Tests the method giving the list of collections rows
            """

            database = self.create_database()
            with database as session:
                # Testing that there is no collection at first
                self.assertEqual(list(session.get_collections()), [])

                # Adding a collection
                session.add_collection("collection1")

                collections = list(session.get_collections())
                self.assertEqual(len(collections), 1)
                self.assertEqual(collections[0].name, "collection1")

                session.add_collection("collection2")

                collections = list(session.get_collections())
                self.assertEqual(len(collections), 2)

                session.remove_collection("collection2")

                collections = list(session.get_collections())
                self.assertEqual(len(collections), 1)
                self.assertEqual(collections[0].name, "collection1")

        def test_get_collections_names(self):
            """
            Tests the method giving the collections names
            """

            database = self.create_database()
            with database as session:
                # Testing that there is no collection at first
                self.assertEqual(list(session.get_collections_names()), [])

                # Adding a collection
                session.add_collection("collection1")

                self.assertEqual(list(session.get_collections_names()), ["collection1"])

                session.add_collection("collection2")

                collections = list(session.get_collections_names())
                self.assertEqual(len(collections), 2)
                self.assertTrue("collection1" in collections)
                self.assertTrue("collection2" in collections)

                session.remove_collection("collection2")

                self.assertEqual(list(session.get_collections_names()), ["collection1"])

        def test_get_documents(self):
            """
            Tests the method returning the list of document rows, given a collection
            """

            database = self.create_database()
            with database as session:
                # Adding collections
                session.add_collection("collection1", "name")
                session.add_collection("collection2", "id")

                session["collection1"]["document1"] = {}
                session["collection1"]["document2"] = {}
                session["collection2"]["document1"] = {}
                session["collection2"]["document2"] = {}

                documents1 = list(session.get_documents("collection1"))
                self.assertEqual(len(documents1), 2)

                documents2 = list(session.get_documents("collection2"))
                self.assertEqual(len(documents2), 2)

                # Testing with a collection not existing
                self.assertEqual(
                    list(session.get_documents("collection_not_existing")), []
                )
                self.assertEqual(list(session.get_documents("collection")), [])
                self.assertEqual(list(session.get_documents(None)), [])

        def test_get_documents_names(self):
            """
            Tests the method returning the list of document names, given a collection
            """

            database = self.create_database()
            with database as session:
                # Adding collections
                session.add_collection("collection1", "name")
                session.add_collection("collection2", "FileName")

                session["collection1"]["document1"] = {}
                session["collection1"]["document2"] = {}
                session["collection2"]["document3"] = {}
                session["collection2"]["document4"] = {}

                documents1 = list(session.get_documents_ids("collection1"))
                self.assertEqual(len(documents1), 2)
                self.assertTrue(["document1"] in documents1)
                self.assertTrue(["document2"] in documents1)

                documents2 = list(session.get_documents_ids("collection2"))
                self.assertEqual(len(documents2), 2)
                self.assertTrue(["document3"] in documents2)
                self.assertTrue(["document4"] in documents2)

                # Testing with a collection not existing
                self.assertEqual(
                    list(session.get_documents_ids("collection_not_existing")), []
                )
                self.assertEqual(list(session.get_documents_ids("collection")), [])
                self.assertEqual(list(session.get_documents_ids(None)), [])

        def test_remove_value(self):
            """
            Tests the method removing a value
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding fields
                session.add_field(
                    "collection1", "PatientName", str, "Name of the patient"
                )
                session.add_field("collection1", "Bits per voxel", int)
                session.add_field("collection1", "Dataset dimensions", list[int], None)

                # Adding a document
                doc = {
                    "PatientName": "test",
                    "Dataset dimensions": [3, 28, 28, 3],
                    "Bits per voxel": 42,
                }
                session["collection1"]["document1"] = doc

                doc["Bits per voxel"] = "space_field"
                # SQLite allows to store any type in columns
                # with self.assertRaises(TypeError):
                session["collection1"]["document1"] = doc
                value = session["collection1"]["document1"]["Dataset dimensions"]
                self.assertEqual(value, [3, 28, 28, 3])

                # Removing values
                del doc["PatientName"]
                del doc["Bits per voxel"]
                del doc["Dataset dimensions"]
                session["collection1"]["document1"] = doc

                # Testing that the values are actually removed
                doc = session["collection1"]["document1"]
                self.assertIsNone(doc.get("PatientName"))
                self.assertIsNone(doc.get("Bits per voxel"))
                self.assertIsNone(doc.get("Dataset dimensions"))

        def test_list_dates(self):
            """
            Tests the storage and retrieval of fields of type list of time, date
            and datetime
            """

            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding fields
                session.add_field("collection1", "list_date", list[date], None)
                session.add_field("collection1", "list_time", list[time], None)
                session.add_field("collection1", "list_datetime", list[datetime], None)

                document = {}
                document["name"] = "document1"
                session.add_document("collection1", document)

                list_date = [date(2018, 5, 23), date(1899, 12, 31)]
                list_time = [time(12, 41, 33, 540), time(1, 2, 3)]
                list_datetime = [
                    datetime(2018, 5, 23, 12, 41, 33, 540),
                    datetime(1899, 12, 31, 1, 2, 3),
                ]

                document["list_date"] = list_date
                document["list_time"] = list_time
                document["list_datetime"] = list_datetime
                session["collection1"]["document1"] = document

                document = session["collection1"]["document1"]
                self.assertEqual(list_date, document["list_date"])
                self.assertEqual(list_time, document["list_time"])
                self.assertEqual(list_datetime, document["list_datetime"])

        def test_json_field(self):
            """
            Tests the storage and retrieval of fields of type JSON
            """

            doc = {"name": "the_name", "json": {"key": [1, 2, "three"]}}
            database = self.create_database()
            with database as session:
                # Adding a collection
                session.add_collection("collection1", "name")

                # Adding fields
                session.add_field("collection1", "json", dict)

                session.add_document("collection1", doc)
                self.assertEqual(doc, session.get_document("collection1", "the_name"))
                self.assertIsNone(
                    session.get_document("collection1", "not_a_valid_name")
                )
            with database as session:
                self.assertEqual(doc, session.get_document("collection1", "the_name"))
                self.assertIsNone(
                    session.get_document("collection1", "not_a_valid_name")
                )

        def test_filter_documents(self):
            """
            Tests the method applying the filter
            """

            database = self.create_database()
            with database as session:
                session.add_collection("collection_test")
                session.add_field("collection_test", "field_test", str, None)
                session["collection_test"]["document_test"] = {}

                # Checking with invalid collection
                self.assertRaises(
                    ValueError,
                    lambda: {
                        document["primary_key"]
                        for document in session.filter_documents(
                            "collection_not_existing", None
                        )
                    },
                )

                # Checking that every document is returned if there is no filter
                documents = {
                    document["primary_key"]
                    for document in session.filter_documents("collection_test", None)
                }
                self.assertEqual(documents, {"document_test"})

        def test_filters(self):
            list_datetime = [
                datetime(2018, 5, 23, 12, 41, 33, 540),
                datetime(1981, 5, 8, 20, 0),
                datetime(1899, 12, 31, 1, 2, 3),
            ]

            database = self.create_database()
            with database as session:
                session.add_collection("collection1", "name")

                session.add_field(
                    "collection1",
                    "format",
                    field_type=str,
                    description=None,
                    index=True,
                )
                session.add_field(
                    "collection1", "strings", field_type=list[str], description=None
                )
                session.add_field(
                    "collection1", "datetime", field_type=datetime, description=None
                )
                session.add_field(
                    "collection1", "has_format", field_type=bool, description=None
                )

                files = ("abc", "bcd", "def", "xyz")
                for file in files:
                    for dt in list_datetime:
                        for format, ext in (
                            ("NIFTI", "nii"),
                            ("DICOM", "dcm"),
                            ("Freesurfer", "mgz"),
                        ):
                            document = dict(
                                name=f"/{file}_{dt.year}.{ext}",
                                format=format,
                                strings=list(file),
                                datetime=dt,
                                has_format=True,
                            )
                            session.add_document("collection1", document)
                        document = f"/{file}_{dt.year}.none"
                        d = dict(name=document, strings=list(file))
                        session.add_document("collection1", d)

                assert (
                    list(session.filter_documents("collection1", "format IN []")) == []
                )

                for filter, expected in (
                    (
                        'format == "NIFTI"',
                        {
                            "/xyz_1899.nii",
                            "/xyz_2018.nii",
                            "/abc_2018.nii",
                            "/bcd_1899.nii",
                            "/bcd_2018.nii",
                            "/def_1899.nii",
                            "/abc_1981.nii",
                            "/def_2018.nii",
                            "/def_1981.nii",
                            "/bcd_1981.nii",
                            "/abc_1899.nii",
                            "/xyz_1981.nii",
                        },
                    ),
                    (
                        '"b" IN strings',
                        {
                            "/bcd_2018.mgz",
                            "/abc_1899.mgz",
                            "/abc_1899.dcm",
                            "/bcd_1981.dcm",
                            "/abc_1981.dcm",
                            "/bcd_1981.mgz",
                            "/bcd_1899.mgz",
                            "/abc_1981.mgz",
                            "/abc_2018.mgz",
                            "/abc_2018.dcm",
                            "/bcd_2018.dcm",
                            "/bcd_1899.dcm",
                            "/abc_2018.nii",
                            "/bcd_1899.nii",
                            "/abc_1981.nii",
                            "/bcd_1981.nii",
                            "/abc_1899.nii",
                            "/bcd_2018.nii",
                            "/abc_1899.none",
                            "/bcd_1899.none",
                            "/bcd_1981.none",
                            "/abc_2018.none",
                            "/bcd_2018.none",
                            "/abc_1981.none",
                        },
                    ),
                    (
                        '(format == "NIFTI" OR NOT format == "DICOM")',
                        {
                            "/xyz_1899.nii",
                            "/xyz_1899.mgz",
                            "/bcd_2018.mgz",
                            "/bcd_1899.nii",
                            "/bcd_2018.nii",
                            "/def_1899.nii",
                            "/bcd_1981.mgz",
                            "/abc_1981.nii",
                            "/def_2018.mgz",
                            "/abc_1899.nii",
                            "/def_1899.mgz",
                            "/xyz_1899.none",
                            "/abc_2018.nii",
                            "/def_1899.none",
                            "/bcd_1899.mgz",
                            "/def_2018.nii",
                            "/abc_1981.mgz",
                            "/abc_1899.none",
                            "/xyz_1981.mgz",
                            "/bcd_1981.nii",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/def_1981.nii",
                            "/def_1981.mgz",
                            "/bcd_1899.none",
                            "/xyz_2018.mgz",
                            "/bcd_1981.none",
                            "/xyz_1981.none",
                            "/abc_1981.none",
                            "/def_2018.none",
                            "/xyz_2018.none",
                            "/abc_2018.none",
                            "/def_1981.none",
                            "/bcd_2018.none",
                        },
                    ),
                    (
                        '"a" IN strings',
                        {
                            "/abc_1899.none",
                            "/abc_1899.nii",
                            "/abc_2018.nii",
                            "/abc_1899.mgz",
                            "/abc_1899.dcm",
                            "/abc_1981.dcm",
                            "/abc_1981.nii",
                            "/abc_1981.mgz",
                            "/abc_2018.mgz",
                            "/abc_2018.dcm",
                            "/abc_2018.none",
                            "/abc_1981.none",
                        },
                    ),
                    (
                        'NOT "b" IN strings',
                        {
                            "/xyz_1899.nii",
                            "/xyz_2018.dcm",
                            "/def_1981.dcm",
                            "/xyz_2018.nii",
                            "/xyz_1981.dcm",
                            "/def_1899.none",
                            "/xyz_1899.dcm",
                            "/xyz_1981.nii",
                            "/def_1899.dcm",
                            "/def_1899.nii",
                            "/def_2018.mgz",
                            "/def_2018.nii",
                            "/xyz_1899.mgz",
                            "/def_2018.dcm",
                            "/def_1899.mgz",
                            "/def_1981.mgz",
                            "/xyz_1981.mgz",
                            "/xyz_2018.mgz",
                            "/xyz_1899.none",
                            "/def_1981.nii",
                            "/xyz_2018.none",
                            "/xyz_1981.none",
                            "/def_2018.none",
                            "/def_1981.none",
                        },
                    ),
                    (
                        '("a" IN strings OR NOT "b" IN strings)',
                        {
                            "/xyz_1899.nii",
                            "/xyz_1899.mgz",
                            "/def_1899.nii",
                            "/abc_1981.nii",
                            "/def_2018.mgz",
                            "/abc_1899.nii",
                            "/def_1899.mgz",
                            "/abc_2018.dcm",
                            "/xyz_1899.none",
                            "/xyz_2018.dcm",
                            "/def_1981.dcm",
                            "/abc_2018.nii",
                            "/def_1899.none",
                            "/abc_1981.dcm",
                            "/def_2018.nii",
                            "/abc_1981.mgz",
                            "/def_2018.dcm",
                            "/abc_1899.none",
                            "/xyz_1981.mgz",
                            "/xyz_1899.dcm",
                            "/abc_1899.dcm",
                            "/def_1899.dcm",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/xyz_1981.dcm",
                            "/def_1981.nii",
                            "/def_1981.mgz",
                            "/xyz_2018.mgz",
                            "/xyz_1981.none",
                            "/abc_1981.none",
                            "/def_2018.none",
                            "/xyz_2018.none",
                            "/abc_2018.none",
                            "/def_1981.none",
                        },
                    ),
                    (
                        'format IN ["DICOM", "NIFTI"]',
                        {
                            "/xyz_1899.nii",
                            "/xyz_2018.dcm",
                            "/bcd_1899.nii",
                            "/def_1899.nii",
                            "/abc_1981.nii",
                            "/abc_1899.nii",
                            "/bcd_2018.nii",
                            "/abc_2018.dcm",
                            "/bcd_1899.dcm",
                            "/def_1981.dcm",
                            "/abc_2018.nii",
                            "/abc_1981.dcm",
                            "/bcd_2018.dcm",
                            "/def_2018.nii",
                            "/def_2018.dcm",
                            "/xyz_1899.dcm",
                            "/abc_1899.dcm",
                            "/def_1899.dcm",
                            "/bcd_1981.nii",
                            "/xyz_1981.nii",
                            "/xyz_2018.nii",
                            "/xyz_1981.dcm",
                            "/def_1981.nii",
                            "/bcd_1981.dcm",
                        },
                    ),
                    (
                        '(format == "NIFTI" OR NOT format == "DICOM") AND ("a" IN strings OR NOT "b" IN strings)',
                        {
                            "/abc_1899.none",
                            "/xyz_1899.mgz",
                            "/abc_1981.mgz",
                            "/abc_2018.nii",
                            "/xyz_1899.nii",
                            "/abc_1899.mgz",
                            "/def_1899.mgz",
                            "/def_1899.nii",
                            "/def_1899.none",
                            "/abc_1981.nii",
                            "/def_2018.nii",
                            "/xyz_2018.nii",
                            "/def_1981.nii",
                            "/abc_1899.nii",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/def_1981.mgz",
                            "/xyz_2018.mgz",
                            "/xyz_1899.none",
                            "/def_2018.mgz",
                            "/xyz_1981.mgz",
                            "/xyz_1981.none",
                            "/abc_1981.none",
                            "/def_2018.none",
                            "/xyz_2018.none",
                            "/abc_2018.none",
                            "/def_1981.none",
                        },
                    ),
                    (
                        'format > "DICOM"',
                        {
                            "/xyz_1899.nii",
                            "/xyz_1899.mgz",
                            "/bcd_2018.mgz",
                            "/bcd_1899.nii",
                            "/bcd_2018.nii",
                            "/def_1899.nii",
                            "/bcd_1981.mgz",
                            "/abc_1981.nii",
                            "/def_2018.mgz",
                            "/abc_1899.nii",
                            "/def_1899.mgz",
                            "/abc_2018.nii",
                            "/def_2018.nii",
                            "/abc_1981.mgz",
                            "/xyz_1981.mgz",
                            "/bcd_1981.nii",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/def_1981.nii",
                            "/def_1981.mgz",
                            "/bcd_1899.mgz",
                            "/xyz_2018.mgz",
                        },
                    ),
                    (
                        'format <= "DICOM"',
                        {
                            "/abc_1981.dcm",
                            "/def_1899.dcm",
                            "/abc_2018.dcm",
                            "/bcd_1899.dcm",
                            "/def_1981.dcm",
                            "/bcd_2018.dcm",
                            "/def_2018.dcm",
                            "/xyz_2018.dcm",
                            "/xyz_1899.dcm",
                            "/abc_1899.dcm",
                            "/xyz_1981.dcm",
                            "/bcd_1981.dcm",
                        },
                    ),
                    (
                        'format > "DICOM" AND strings != ["b", "c", "d"]',
                        {
                            "/xyz_1899.nii",
                            "/xyz_1899.mgz",
                            "/abc_1981.mgz",
                            "/abc_2018.nii",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/def_1899.mgz",
                            "/def_1899.nii",
                            "/abc_1981.nii",
                            "/def_2018.nii",
                            "/def_1981.nii",
                            "/abc_1899.nii",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/def_1981.mgz",
                            "/xyz_2018.mgz",
                            "/def_2018.mgz",
                            "/xyz_1981.mgz",
                        },
                    ),
                    (
                        'format <= "DICOM" AND strings == ["b", "c", "d"]',
                        {
                            "/bcd_2018.dcm",
                            "/bcd_1981.dcm",
                            "/bcd_1899.dcm",
                        },
                    ),
                    (
                        "has_format in [false, null]",
                        {
                            "/def_1899.none",
                            "/abc_1899.none",
                            "/bcd_1899.none",
                            "/xyz_1899.none",
                            "/bcd_2018.none",
                            "/abc_1981.none",
                            "/def_2018.none",
                            "/xyz_2018.none",
                            "/abc_2018.none",
                            "/def_1981.none",
                            "/xyz_1981.none",
                            "/bcd_1981.none",
                        },
                    ),
                    (
                        "format == null",
                        {
                            "/bcd_1981.none",
                            "/abc_1899.none",
                            "/def_1899.none",
                            "/bcd_2018.none",
                            "/abc_1981.none",
                            "/def_2018.none",
                            "/xyz_2018.none",
                            "/abc_2018.none",
                            "/def_1981.none",
                            "/bcd_1899.none",
                            "/xyz_1899.none",
                            "/xyz_1981.none",
                        },
                    ),
                    ("strings == null", set()),
                    (
                        "strings != NULL",
                        {
                            "/xyz_1899.nii",
                            "/xyz_2018.dcm",
                            "/xyz_1899.mgz",
                            "/bcd_2018.mgz",
                            "/bcd_1899.nii",
                            "/def_2018.none",
                            "/def_1899.mgz",
                            "/def_1899.nii",
                            "/bcd_1981.mgz",
                            "/abc_1981.nii",
                            "/def_2018.mgz",
                            "/abc_1899.nii",
                            "/bcd_2018.nii",
                            "/abc_2018.dcm",
                            "/xyz_1899.none",
                            "/bcd_1899.dcm",
                            "/bcd_1981.none",
                            "/def_1981.dcm",
                            "/abc_2018.nii",
                            "/def_1899.none",
                            "/xyz_1981.none",
                            "/abc_1981.dcm",
                            "/bcd_2018.dcm",
                            "/def_2018.nii",
                            "/abc_1981.mgz",
                            "/def_2018.dcm",
                            "/abc_1899.none",
                            "/xyz_1981.mgz",
                            "/xyz_1899.dcm",
                            "/abc_1899.dcm",
                            "/def_1899.dcm",
                            "/bcd_1981.nii",
                            "/def_1981.none",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/xyz_2018.none",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/bcd_1899.mgz",
                            "/bcd_2018.none",
                            "/abc_1981.none",
                            "/xyz_1981.dcm",
                            "/abc_2018.none",
                            "/def_1981.nii",
                            "/bcd_1981.dcm",
                            "/def_1981.mgz",
                            "/bcd_1899.none",
                            "/xyz_2018.mgz",
                        },
                    ),
                    (
                        "format != NULL",
                        {
                            "/xyz_1899.nii",
                            "/xyz_1899.mgz",
                            "/bcd_2018.mgz",
                            "/bcd_1899.nii",
                            "/def_1899.mgz",
                            "/def_1899.nii",
                            "/bcd_1981.mgz",
                            "/abc_1981.nii",
                            "/def_2018.mgz",
                            "/abc_1899.nii",
                            "/bcd_2018.nii",
                            "/abc_2018.dcm",
                            "/xyz_1981.mgz",
                            "/def_1981.dcm",
                            "/abc_2018.nii",
                            "/abc_1981.dcm",
                            "/bcd_2018.dcm",
                            "/def_2018.nii",
                            "/bcd_1981.nii",
                            "/abc_1981.mgz",
                            "/def_2018.dcm",
                            "/bcd_1899.dcm",
                            "/xyz_2018.dcm",
                            "/xyz_1899.dcm",
                            "/abc_1899.dcm",
                            "/def_1899.dcm",
                            "/bcd_1899.mgz",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/xyz_1981.dcm",
                            "/def_1981.nii",
                            "/bcd_1981.dcm",
                            "/def_1981.mgz",
                            "/xyz_2018.mgz",
                        },
                    ),
                    (
                        'name like "%.nii"',
                        {
                            "/xyz_1899.nii",
                            "/xyz_2018.nii",
                            "/abc_2018.nii",
                            "/bcd_1899.nii",
                            "/bcd_2018.nii",
                            "/def_1899.nii",
                            "/abc_1981.nii",
                            "/def_2018.nii",
                            "/def_1981.nii",
                            "/bcd_1981.nii",
                            "/abc_1899.nii",
                            "/xyz_1981.nii",
                        },
                    ),
                    (
                        'name ilike "%A%"',
                        {
                            "/abc_1899.none",
                            "/abc_1899.nii",
                            "/abc_2018.nii",
                            "/abc_1899.mgz",
                            "/abc_1899.dcm",
                            "/abc_1981.dcm",
                            "/abc_1981.nii",
                            "/abc_1981.mgz",
                            "/abc_2018.mgz",
                            "/abc_2018.dcm",
                            "/abc_2018.none",
                            "/abc_1981.none",
                        },
                    ),
                    (
                        "all",
                        {
                            "/xyz_1899.nii",
                            "/xyz_2018.dcm",
                            "/xyz_1899.mgz",
                            "/bcd_2018.mgz",
                            "/bcd_1899.nii",
                            "/def_2018.none",
                            "/def_1899.mgz",
                            "/def_1899.nii",
                            "/bcd_1981.mgz",
                            "/abc_1981.nii",
                            "/def_2018.mgz",
                            "/abc_1899.nii",
                            "/bcd_2018.nii",
                            "/abc_2018.dcm",
                            "/xyz_1899.none",
                            "/bcd_1899.dcm",
                            "/bcd_1981.none",
                            "/def_1981.dcm",
                            "/abc_2018.nii",
                            "/def_1899.none",
                            "/xyz_1981.none",
                            "/abc_1981.dcm",
                            "/bcd_2018.dcm",
                            "/def_2018.nii",
                            "/abc_1981.mgz",
                            "/def_2018.dcm",
                            "/abc_1899.none",
                            "/xyz_1981.mgz",
                            "/xyz_1899.dcm",
                            "/abc_1899.dcm",
                            "/def_1899.dcm",
                            "/bcd_1981.nii",
                            "/def_1981.none",
                            "/xyz_1981.nii",
                            "/abc_2018.mgz",
                            "/xyz_2018.none",
                            "/xyz_2018.nii",
                            "/abc_1899.mgz",
                            "/bcd_1899.mgz",
                            "/bcd_2018.none",
                            "/abc_1981.none",
                            "/xyz_1981.dcm",
                            "/abc_2018.none",
                            "/def_1981.nii",
                            "/bcd_1981.dcm",
                            "/def_1981.mgz",
                            "/bcd_1899.none",
                            "/xyz_2018.mgz",
                        },
                    ),
                ):
                    for tested_filter in (
                        filter,
                        f"({filter}) AND ALL",
                        f"ALL AND ({filter})",
                    ):
                        try:
                            documents = {
                                document["name"]
                                for document in session.filter_documents(
                                    "collection1", tested_filter
                                )
                            }
                            self.assertEqual(documents, expected)
                        except Exception as e:
                            raise Exception(
                                f"Error while testing filter : {tested_filter}"
                            ) from e
                    all_documents = {
                        document["name"]
                        for document in session.filter_documents("collection1", "ALL")
                    }
                    for tested_filter in (
                        f"({filter}) OR ALL",
                        f"ALL OR ({filter})",
                    ):
                        try:
                            documents = {
                                document["name"]
                                for document in session.filter_documents(
                                    "collection1", tested_filter
                                )
                            }
                            self.assertEqual(documents, all_documents)
                        except Exception as e:
                            raise Exception(
                                f"Error while testing filter : {tested_filter}"
                            ) from e

        def test_modify_list_field(self):
            database = self.create_database()
            with database as session:
                session.add_collection("collection1", "name")
                session.add_field(
                    "collection1", "strings", field_type=list[str], description=None
                )
                session["collection1"]["test"] = {"strings": ["a", "b", "c"]}
                names = list(
                    document["name"]
                    for document in session.filter_documents(
                        "collection1", '"b" IN strings'
                    )
                )
                self.assertEqual(names, ["test"])

                session["collection1"]["test"] = {"strings": ["x", "y", "z"]}
                names = list(
                    document["name"]
                    for document in session.filter_documents(
                        "collection1", '"b" IN strings'
                    )
                )
                self.assertEqual(names, [])
                names = list(
                    document["name"]
                    for document in session.filter_documents(
                        "collection1", '"z" IN strings'
                    )
                )
                self.assertEqual(names, ["test"])

                session["collection1"]["test"] = {}
                names = list(
                    document["name"]
                    for document in session.filter_documents(
                        "collection1", '"y" IN strings'
                    )
                )
                self.assertEqual(names, [])

        def test_filter_literals(self):
            """
            Test the Python values returned (internally) for literals by the
            interpreter of filter expression
            """

            literals = {
                "True": True,
                "TRUE": True,
                "true": True,
                "False": False,
                "FALSE": False,
                "false": False,
                "Null": None,
                "NULL": None,
                "null": None,
                "0": 0,
                "123456789101112": 123456789101112,
                "-45": -45,
                "-46.8": -46.8,
                "1.5654353456363e-15": 1.5654353456363e-15,
                '""': "",
                '"2018-05-25"': "2018-05-25",
                '"a\n b\n  c"': "a\n b\n  c",
                '"\\""': '"',
                "2018-05-25": date(2018, 5, 25),
                "2018-5-25": date(2018, 5, 25),
                "12:54": time(12, 54),
                "02:4:9": time(2, 4, 9),
                # The following interpretation of microsecond is a strange
                # behavior of datetime.strptime that expect up to 6 digits
                # with zeroes padded on the right !?
                "12:34:56.789": time(12, 34, 56, 789000),
                "12:34:56.000789": time(12, 34, 56, 789),
                "2018-05-25T12:34:56.000789": datetime(2018, 5, 25, 12, 34, 56, 789),
                "2018-5-25T12:34": datetime(2018, 5, 25, 12, 34),
                "[]": [],
            }
            # Adds the literal for a list of all elements in the dictionary
            literals[f"[{','.join(literals.keys())}]"] = list(literals.values())

            parser = literal_parser()
            for literal, expected_value in literals.items():
                tree = parser.parse(literal)
                value = FilterToSQL(None).transform(tree)
                self.assertEqual(value, expected_value)

        def test_with(self):
            """
            Tests the database session
            """

            database = self.create_database()
            try:
                with database as session:
                    session.add_collection("collection1", "name")
                    session.add_document("collection1", {"name": "toto"})
                    # Raise an exception to roll back modifications
                    boom  # noqa: B018
            except NameError:
                pass

            with database as session:
                session.add_collection("collection1", "name")
                session.add_document("collection1", {"name": "titi"})

            # Reopen the database to check that "titi" was committed
            database = self.create_database(clear=False)
            with database as session:
                self.assertTrue(session.has_collection("collection1"))
                names = [
                    i["name"] for i in session.filter_documents("collection1", "all")
                ]
                self.assertEqual(names, ["titi"])

                # Check that recursive session creation always return the
                # same object
                with database as session2:
                    self.assertIs(session, session2)
                    with database as session3:
                        self.assertIs(session, session3)
                        session.add_document("collection1", {"name": "toto"})

            # Check that previous session was committed and released.
            with database as session4:
                self.assertIsNot(session, session4)

            # Destroy the database and create a new one
            database = self.create_database(clear=False)

            # Check that previous session was destroyed and that
            # a new one is created.
            with database as session5:
                self.assertIsNot(session, session5)
                self.assertEqual(len(list(session5.get_documents("collection1"))), 2)

        def test_automatic_fields_creation(self):
            """
            Test automatic creation of fields with add_document
            """
            database = self.create_database()
            with database as session:
                now = datetime.now()
                session.add_collection("test")
                base_doc = {
                    "string": "string",
                    "int": 1,
                    "float": 1.4,
                    "boolean": True,
                    "datetime": now,
                    "date": now.date(),
                    "time": now.time(),
                    "dict": {
                        "string": "string",
                        "int": 1,
                        "float": 1.4,
                        "boolean": True,
                    },
                }
                doc = base_doc.copy()
                for k, v in base_doc.items():
                    lk = f"list_{k}"
                    doc[lk] = [v]
                doc["primary_key"] = "test"
                session.add_document("test", doc)
                self.maxDiff = None
                stored_doc = session.get_document("test", "test")
                self.assertEqual(doc, stored_doc)

        def test_delete(self):
            """
            Test automatic creation of fields with add_document
            """
            database = self.create_database()
            with database as session:
                session.add_collection("things", ("one", "two"))
                for one in range(10):
                    for two in range(10):
                        session["things"][(str(one), str(two))] = {
                            "content": one * two,
                        }
                deleted = session["things"].delete("all")
                self.assertEqual(deleted, 100)
                deleted = session["things"].delete("{content} >= 4")
                self.assertEqual(deleted, 0)
                for one in range(10):
                    for two in range(10):
                        session["things"][(str(one), str(two))] = {
                            "content": one * two,
                        }
                deleted = session["things"].delete('{one} == "5"')
                self.assertEqual(deleted, 10)
                deleted = session["things"].delete("{content} >= 4")
                self.assertEqual(deleted, 67)
                self.maxDiff = 1000
                self.assertEqual(
                    list(session["things"].documents()),
                    [
                        {"content": 0, "one": 0, "two": 0},
                        {"content": 0, "one": 0, "two": 1},
                        {"content": 0, "one": 0, "two": 2},
                        {"content": 0, "one": 0, "two": 3},
                        {"content": 0, "one": 0, "two": 4},
                        {"content": 0, "one": 0, "two": 5},
                        {"content": 0, "one": 0, "two": 6},
                        {"content": 0, "one": 0, "two": 7},
                        {"content": 0, "one": 0, "two": 8},
                        {"content": 0, "one": 0, "two": 9},
                        {"content": 0, "one": 1, "two": 0},
                        {"content": 1, "one": 1, "two": 1},
                        {"content": 2, "one": 1, "two": 2},
                        {"content": 3, "one": 1, "two": 3},
                        {"content": 0, "one": 2, "two": 0},
                        {"content": 2, "one": 2, "two": 1},
                        {"content": 0, "one": 3, "two": 0},
                        {"content": 3, "one": 3, "two": 1},
                        {"content": 0, "one": 4, "two": 0},
                        {"content": 0, "one": 6, "two": 0},
                        {"content": 0, "one": 7, "two": 0},
                        {"content": 0, "one": 8, "two": 0},
                        {"content": 0, "one": 9, "two": 0},
                    ],
                )

    return TestDatabaseMethods


TestDatabaseMethods = create_test_case()

# def load_tests(loader, standard_tests, pattern):
#     """
#     Prepares the tests parameters

#     :param loader:

#     :param standard_tests:

#     :param pattern:

#     :return: A test suite
#     """
#     suite = unittest.TestSuite()
#     suite.addTests(loader.loadTestsFromTestCase(TestsSQLiteInMemory))
#     tests = loader.loadTestsFromTestCase(create_test_case())
#     suite.addTests(tests)

#     # Tests with postgresql. All the tests will be skipped if
#     # it is not possible to connect to populse_db_tests database.
#     # tests = loader.loadTestsFromTestCase(create_test_case(
#     # database_url='postgresql:///populse_db_tests',
#     # caches=False,
#     # list_tables=True,
#     # query_type='mixed'))
#     # suite.addTests(tests)

#     return suite
