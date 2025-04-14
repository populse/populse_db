schemas = [
    {
        "version": "1.0.1",
        "schema": {
            "snapshots": [
                {
                    "subject": ["str", {"primary_key": True}],
                    "time_point": [str, {"primary_key": True}],
                    "data_type": ["str", {"primary_key": True}],
                    "image": str,
                    "top": "list[int]",
                    "size": list[float],
                    "side": "str",
                    "execution": [str, {"index": True}],
                }
            ],
        },
    },
    {
        "version": "1.0.0",
        "schema": {
            "snapshots": [
                {
                    "subject": ["str", {"primary_key": True}],
                    "time_point": [str, {"primary_key": True}],
                    "data_type": ["str", {"primary_key": True}],
                    "image": str,
                    "top": "list[int]",
                    "size": list[float],
                    "side": "str",
                }
            ],
        },
    },
]
