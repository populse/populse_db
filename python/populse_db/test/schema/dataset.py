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
        },
    }
]
