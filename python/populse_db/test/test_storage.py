import os
import signal
import subprocess
import sys
from datetime import datetime
from tempfile import NamedTemporaryFile

import pytest

from populse_db import Storage
from populse_db.storage import SchemaSession

snapshots = [
    {
        "orientation": "coronal",
        "top": [0.0, 0.0],
        "size": [872.0, 890.0],
        "dataset": "database_brainvisa",
        "software": "morphologist",
        "time_point": "M0",
        "data_type": "greywhite",
        "side": "both",
        "image": "/home/yann/dev/snapcheck/database_brainvisa/snapshots/morphologist/M0/greywhite/snapshot_greywhite_0001292COG_M0.png",
        "subject": "0001292COG",
    },
    {
        "orientation": "coronal",
        "top": [0.0, 0.0],
        "size": [872.0, 887.0],
        "dataset": "database_brainvisa",
        "software": "morphologist",
        "time_point": "M0",
        "data_type": "greywhite",
        "side": "both",
        "image": "/home/yann/dev/snapcheck/database_brainvisa/snapshots/morphologist/M0/greywhite/snapshot_greywhite_0001017COG_M0.png",
        "subject": "0001017COG",
    },
    {
        "orientation": "coronal",
        "top": [872.0, 0.0],
        "size": [872.0, 890.0],
        "dataset": "database_brainvisa",
        "software": "morphologist",
        "time_point": "M0",
        "data_type": "greywhite",
        "side": "both",
        "image": "/home/yann/dev/snapcheck/database_brainvisa/snapshots/morphologist/M0/greywhite/snapshot_greywhite_0001235COG_M0.png",
        "subject": "0001235COG",
    },
    {
        "orientation": "axial",
        "top": [1.0, 0.0],
        "size": [2.0, 3.0],
        "dataset": "test",
        "software": "none",
        "time_point": "first",
        "data_type": "void",
        "image": "/somewhere/something.png",
        "subject": "john doe",
    },
]


def test_storage_schema():
    snapshot_1_0_1 = {
        "name": "populse_db.test.schema.snapshot",
        "version": "1.0.1",
        "collections": {
            "snapshots": {
                "data_type": ["str", {"primary_key": True}],
                "execution": ["str", {"index": True}],
                "image": ["str", {}],
                "side": ["str", {}],
                "size": ["list[float]", {}],
                "subject": ["str", {"primary_key": True}],
                "time_point": ["str", {"primary_key": True}],
                "top": ["list[int]", {}],
            }
        },
    }
    snapshot_1_0_0 = {
        "name": "populse_db.test.schema.snapshot",
        "version": "1.0.0",
        "collections": {
            "snapshots": {
                "data_type": ["str", {"primary_key": True}],
                "image": ["str", {}],
                "side": ["str", {}],
                "size": ["list[float]", {}],
                "subject": ["str", {"primary_key": True}],
                "time_point": ["str", {"primary_key": True}],
                "top": ["list[int]", {}],
            }
        },
    }
    schema = SchemaSession.find_schema("populse_db.test.schema.snapshot")
    assert schema == snapshot_1_0_1
    schema = SchemaSession.find_schema("populse_db.test.schema.snapshot", "1.0.1")
    assert schema == snapshot_1_0_1
    schema = SchemaSession.find_schema("populse_db.test.schema.snapshot", "1.0.0")
    assert schema == snapshot_1_0_0
    schema = SchemaSession.find_schema("populse_db.test.schema.snapshot", "1.0")
    assert schema == snapshot_1_0_1
    assert SchemaSession.find_schema("populse_db.test.schema.snapshot", "1") is None
    assert SchemaSession.find_schema("populse_db.test.schema.snapshot", "1.1") is None

    import populse_db.test.test_schema

    assert set(populse_db.test.schema.snapshot._schemas_to_collections) == {
        "1.0.1",
        "1.0.0",
        "1.0",
        None,
    }

    with pytest.raises(ModuleNotFoundError):
        SchemaSession.find_schema("populse_db.test.schema.non_existing")


def run_storage_tests(store):
    with store.schema() as schema:
        schema.add_collection("test_collection_1", "primary_key")
        schema.add_collection("test_collection_1", "primary_key")
        with pytest.raises(ValueError):
            schema.add_collection("test_collection_1", "another_key")
        schema.add_collection(
            "test_collection_2", {"primary_key_1": "str", "primary_key_2": int}
        )
        schema.add_collection(
            "test_collection_2", {"primary_key_1": "str", "primary_key_2": int}
        )
        with pytest.raises(ValueError):
            schema.add_collection(
                "test_collection_2", ("primary_key_1", "primary_key_2")
            )
        with pytest.raises(ValueError):
            schema.add_collection(
                "test_collection_3", {"primary_key_1": "str", "primary_key_2": "what ?"}
            )

        schema.add_field("test_collection_1", "test", list[int])
        schema.add_field("test_collection_1", "test", list[int])
        schema.add_field("test_collection_1", "test", "list[int]")
        with pytest.raises(ValueError):
            schema.add_field("test_collection_1", "test", list[float])
        with pytest.raises(ValueError):
            schema.add_field("test_collection_1", "test_2", "what ?")
        schema.add_schema("populse_db.test.schema.dataset")
        schema.add_schema("populse_db.test.schema.snapshot")
        schema.add_schema("populse_db.test.schema.processing")

        schema.add_collection("test_update", "key")
        schema.add_field("test_update", "f1", str)
        schema.add_field("test_update", "f2", str)
        schema.add_field("test_update", "f3", str)
        schema.add_field("test_update", "dict", dict)

    with store.data(write=True) as d:
        now = datetime.now()

        # Set a global value
        d.last_update = now
        d.a_dict = {"one": 1}

        # Read a global value
        assert d.last_update.get() == now
        assert d.a_dict.get() == {"one": 1}

        assert d.does_not_exist.get() is None

        # Modify global value
        d.a_dict["two"] = 2
        assert d.a_dict.get() == {"one": 1, "two": 2}

        # Set single document fields
        d.dataset.directory = "/somewhere"
        d.dataset.schema.set("BIDS")
        # Following field is not in schema
        my_data = {
            "creation_date": now,
            "manager": "me",
            "my_list": ["zero", "one", "two"],
        }
        d.dataset.my_data = my_data
        assert d.dataset.directory.get() == "/somewhere"
        assert d.dataset.get() == {
            "directory": "/somewhere",
            "schema": "BIDS",
            "my_data": my_data,
        }
        assert d.dataset.my_data.my_list[2].get() == "two"

        # Set values not in schema
        d.dataset.my_data.new_value = now
        assert d.dataset.my_data.new_value.get() == now
        d.dataset.my_data.my_list.append("three")
        assert d.dataset.my_data.my_list[3].get() == "three"
        d.dataset.my_data.my_list[3] = "last"
        assert d.dataset.my_data.my_list[3].get() == "last"

        # Update a document
        d.test_update["test"] = {
            "f1": "f1",
            "f3": "value",
            "dict": {"one": 1, "three": 4},
        }
        d.test_update.test.update(
            {
                "f2": "f2",
                "f3": "f3",
            }
        )
        d.test_update.test.dict.update(
            {
                "two": 2,
                "three": 3,
            }
        )

        assert d.test_update.test.get() == {
            "key": "test",
            "f1": "f1",
            "f2": "f2",
            "f3": "f3",
            "dict": {"one": 1, "two": 2, "three": 3},
        }

        # Delete data
        del d.test_update.test.dict.two
        del d.test_update.test.f2
        assert d.test_update.test.get() == {
            "key": "test",
            "f1": "f1",
            "f2": None,
            "f3": "f3",
            "dict": {"one": 1, "three": 3},
        }
        del d.test_update.test
        assert d.test_update.get() == []

        with pytest.raises(ValueError):
            d.update({})
        with pytest.raises(ValueError):
            d.test_update.update({})

        # Adds many documents in a collection
        for snapshot in snapshots:
            d.snapshots.append(snapshot)

        # Select one document from a collection (ignore fields with None value)
        assert {
            k: v
            for k, v in d.snapshots["0001292COG", "M0", "greywhite"].get().items()
            if v is not None
        } == snapshots[0]

        # Select one document field from a collection
        assert (
            d.snapshots["0001292COG", "M0", "greywhite"].image.get()
            == snapshots[0]["image"]
        )

        # Select all documents from a collection (ignore fields with None value)
        assert [
            {k: v for k, v in d.items() if v is not None} for d in d.snapshots.get()
        ] == snapshots

        # Set a full document content
        modified_snapshot = snapshots[0].copy()
        modified_snapshot["software"] = "something"
        del modified_snapshot["dataset"]
        d.snapshots["0001292COG", "M0", "greywhite"] = modified_snapshot
        assert {
            k: v
            for k, v in d.snapshots["0001292COG", "M0", "greywhite"].get().items()
            if v is not None
        } == modified_snapshot

        # Set a whole collection
        d["snapshots"] = []
        assert d.snapshots.get() == []
        d.snapshots = snapshots
        assert [
            {k: v for k, v in d.items() if v is not None} for d in d.snapshots.get()
        ] == snapshots

        # Search in a collection
        assert [
            {k: v for k, v in d.items() if v is not None}
            for d in d.snapshots.search('subject LIKE "%7%"')
        ] == snapshots[1:2]

        assert [
            {k: v for k, v in doc.items() if v is not None}
            for doc in d.snapshots.search(image="/somewhere/something.png")
        ] == snapshots[3:4]

        assert d.snapshots.search(
            time_point="M0", as_list=True, fields=["data_type"], distinct=True
        ) == [["greywhite"]]

        # Count elements
        assert d.snapshots.count() == 4
        assert d.snapshots.count('image LIKE "/home/yann%"') == 3

        # Find all unique values
        assert set(d.snapshots.distinct_values("data_type")) == {"greywhite", "void"}
        assert set(
            row[0]
            for row in d.snapshots.get(
                fields=["data_type"], as_list=True, distinct=True
            )
        ) == {"greywhite", "void"}

        # Get non existent data
        assert d.nothing.get() is None
        assert d.nothing.get("default") == "default"
        assert d.nothing.x.get() is None
        assert d.nothing.x.get("default") == "default"
        assert d.nothing.x.y.get() is None
        assert d.nothing.x.y.get("default") == "default"
        assert d.snapshots["0001292COG", "bad", "greywhite"].get() is None
        assert d.snapshots["0001292COG", "bad", "greywhite"].get("default") == "default"
        assert d.snapshots["0001292COG", "M0", "greywhite"].nothing.get() is None
        assert (
            d.snapshots["0001292COG", "M0", "greywhite"].nothing.get("default")
            == "default"
        )

        # Search and delete
        d.snapshots.search_and_delete('image LIKE "/home/yann%"')
        assert len(d.snapshots.get()) == 1

    # Test read only session
    with store.data(write=False) as d:
        with pytest.raises(PermissionError):
            d.last_update = now

        with pytest.raises(PermissionError):
            for snapshot in snapshots:
                d.snapshots.append(snapshot)


def test_storage():
    tmp = NamedTemporaryFile()
    store = Storage(tmp.name)
    run_storage_tests(store)


def test_storage_server():
    pytest.importorskip("fastapi")
    pytest.importorskip("uvicorn")
    pytest.importorskip("tblib")
    tmp = NamedTemporaryFile()
    cmd = [sys.executable, "-m", "populse_db.server", tmp.name]
    server = subprocess.Popen(cmd)
    import time

    time.sleep(1)
    try:
        store = Storage(tmp.name)
        run_storage_tests(store)
    finally:
        os.kill(server.pid, signal.SIGTERM)
