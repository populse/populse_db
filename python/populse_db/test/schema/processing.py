from datetime import datetime

schemas = [
    {
        "version": "1.0.0",
        "schema": {
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
        },
    }
]
