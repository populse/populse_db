from datetime import datetime
from tempfile import NamedTemporaryFile

from populse_db import Storage

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
]


def test_storage():
    # A schema can be defined in a class deriving from
    # Storage
    class MyStorage(Storage):
        schema = {
            # A global value
            "last_update": datetime,
            # A single document (i.e. not in a collection)
            "dataset": {
                "directory": str,
                "schema": "str",
            },
            # A collection of metadata associated to a path.
            "metadata": [
                {
                    "path": [str, {"primary_key": True}],
                    "subject": str,
                    "time_point": str,
                    "history": list[str],  # contains a list of execution_id
                }
            ],
            # A collection of executions to track data provenance
            "execution": [
                {
                    "execution_id": ["str", {"primary_key": True}],
                    "start_time": "datetime",
                    "end_time": datetime,
                    "status": str,
                    "capsul_executable": str,
                    "capsul_parameters": dict,
                    "software": str,
                    "software_module": str,
                    "software_version": str,
                }
            ],
            # A collection of snapshots requir
            "snapshots": [
                {
                    "subject": ["str", {"primary_key": True}],
                    "time_point": ["str", {"primary_key": True}],
                    "image": str,
                    "top": "list[int]",
                    "size": list[float],
                    "execution": "str",
                    "data_type": "str",
                    "side": "str",
                }
            ],
        }

    json_schema = {
        "last_update": "datetime",
        "dataset": {
            "directory": "str",
            "schema": "str",
        },
        "metadata": [
            {
                "path": ["str", {"primary_key": True}],
                "subject": "str",
                "time_point": "str",
                "history": "list[str]",
            }
        ],
        "execution": [
            {
                "execution_id": ["str", {"primary_key": True}],
                "start_time": "datetime",
                "end_time": "datetime",
                "status": "str",
                "capsul_executable": "str",
                "capsul_parameters": "dict",
                "software": "str",
                "software_module": "str",
                "software_version": "str",
            }
        ],
        "snapshots": [
            {
                "subject": ["str", {"primary_key": True}],
                "time_point": ["str", {"primary_key": True}],
                "image": "str",
                "top": "list[int]",
                "size": "list[float]",
                "execution": "str",
                "data_type": "str",
                "side": "str",
            }
        ],
    }

    tmp = NamedTemporaryFile()
    store = MyStorage(tmp.name)
    assert store.get_schema() == json_schema
    with store.session() as d:
        now = datetime.now()

        # Set a global value
        d.last_update = now

        # Read a global value
        assert d.last_update.get() == now

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
            for k, v in d.snapshots["0001292COG", "M0"].get().items()
            if v is not None
        } == snapshots[0]

        # Select one document field from a collection
        d.snapshots["0001292COG", "M0"].image.get() == snapshots[0]["image"]

        # Select all documents from a collection (ignore fields with None value)
        assert [
            {k: v for k, v in d.items() if v is not None} for d in d.snapshots.get()
        ] == snapshots

        # Set a full document content
        modified_snapshot = snapshots[0].copy()
        modified_snapshot["software"] = "something"
        del modified_snapshot["data_type"]
        d.snapshots["0001292COG", "M0"] = modified_snapshot
        assert {
            k: v
            for k, v in d.snapshots["0001292COG", "M0"].get().items()
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
