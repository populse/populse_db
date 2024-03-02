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


def test_storage():
    tmp = NamedTemporaryFile()
    store = Storage(tmp.name)
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

    with store.data(write=True) as d:
        now = datetime.now()

        # Set a global value
        d.last_update = now

        # Read a global value
        assert d.last_update.get() == now

        assert d.does_not_exist.get() is None

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

        # Find all unique values
        assert sorted(d.snapshots.distinct_values("data_type")) == ["greywhite", "void"]

    # Test read only session
    with store.data(write=False) as d:
        with pytest.raises(PermissionError):
            d.last_update = now

        with pytest.raises(PermissionError):
            for snapshot in snapshots:
                d.snapshots.append(snapshot)
