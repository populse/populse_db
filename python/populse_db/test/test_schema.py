from datetime import datetime

schemas = [
    {
        "version": "1.0.0",
        "schema": {
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
            # A collection of snapshots
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
        },
    }
]
