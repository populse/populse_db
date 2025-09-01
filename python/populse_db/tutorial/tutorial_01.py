from populse_db import Database

with Database(":memory:") as db:
    db.add_collection("test")
    db["test"]["doc"] = {}
