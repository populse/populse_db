import sqlite3
import sys
from typing import Annotated

import uvicorn
from fastapi import Body, FastAPI, Request
from fastapi.responses import JSONResponse

from .database import json_decode, json_encode, populse_db_table
from .storage_api import StorageFileAPI, serialize_exception

body_str = Annotated[str, Body(embed=True)]
body_path = Annotated[list[str | int | list[str]], Body(embed=True)]
body_bool = Annotated[bool, Body()]
body_dict = Annotated[dict, Body()]
body_json = Annotated[str | int | float | bool | None | list | dict, Body()]


def create_server(database_file):
    storage_api = StorageFileAPI(database_file)
    app = FastAPI()

    async def catch_exceptions_middleware(request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content=serialize_exception(exc),
            )

    app.middleware("http")(catch_exceptions_middleware)

    @app.get("/access_token")
    async def access_token():
        # TODO: give a real re/write challenge to the user in
        # order to get its access rights.
        return storage_api.access_token()

    @app.post("/connection")
    async def connect(
        access_token: body_str,
        exclusive: body_bool,
        write: body_bool,
        create: body_bool,
    ):
        return storage_api.connect(access_token, exclusive, write, create)

    @app.delete("/connection")
    async def disconnect(connection_id: body_str, rollback: body_bool):
        return storage_api.disconnect(connection_id, rollback)

    @app.post("/schema_collection")
    async def add_schema_collections(
        connection_id: body_str, schema_to_collections: body_dict
    ):
        return storage_api.add_schema_collections(connection_id, schema_to_collections)

    @app.post("/schema/{name}")
    async def add_collection(
        connection_id: body_str,
        name: str,
        primary_key: Annotated[str | list[str] | dict[str, str], Body()],
    ):
        return storage_api.add_collection(connection_id, name, primary_key)

    @app.post("/schema/{collection_name}/{field_name}")
    async def add_field(
        connection_id: body_str,
        collection_name: str,
        field_name: str,
        field_type: body_str,
        description: Annotated[str | None, Body()] = None,
        index: body_bool = False,
    ):
        return storage_api.add_field(
            connection_id,
            collection_name,
            field_name,
            field_type,
            description,
            index,
        )

    @app.delete("/schema/{collection_name}/{field_name}")
    async def remove_field(
        connection_id: body_str,
        collection_name: str,
        field_name: str,
    ):
        return storage_api.remove_field(
            connection_id,
            collection_name,
            field_name,
        )

    @app.get("/data")
    async def get(
        connection_id: body_str,
        path: body_path,
        default: body_json = None,
        fields: Annotated[list[str] | None, Body()] = None,
        as_list: body_bool = False,
        distinct: body_bool = False,
    ):
        result = storage_api.get(
            connection_id,
            path,
            default,
            fields,
            as_list,
            distinct,
        )
        return json_encode(result)

    @app.get("/count")
    async def count(
        connection_id: body_str,
        path: body_path,
        query: Annotated[str | None, Body()] = None,
    ):
        return storage_api.count(connection_id, path, query)

    @app.get("/primary_key")
    async def primary_key(connection_id: body_str, path: body_path):
        return storage_api.primary_key(connection_id, path)

    @app.post("/data")
    async def set(connection_id: body_str, path: body_path, value: body_json):
        return storage_api.set(connection_id, path, json_decode(value))

    @app.delete("/data")
    async def delete(connection_id: body_str, path: body_path):
        return storage_api.delete(connection_id, path)

    @app.put("/data")
    async def update(connection_id: body_str, path: body_path, value: body_json):
        return storage_api.update(connection_id, path, json_decode(value))

    @app.patch("/data")
    async def append(connection_id: body_str, path: body_path, value: body_json):
        return storage_api.append(connection_id, path, json_decode(value))

    @app.get("/search")
    async def search(
        connection_id: body_str,
        path: body_path,
        query: Annotated[str | None, Body()] = None,
        fields: Annotated[list[str] | None, Body()] = None,
        as_list: body_bool = False,
        distinct: body_bool = False,
    ):
        result = storage_api.search(
            connection_id, path, query, fields, as_list, distinct
        )
        return json_encode(result)

    @app.delete("/search")
    async def search_and_delete(
        connection_id: body_str,
        path: body_path,
        query: Annotated[str | None, Body()] = None,
    ):
        return storage_api.search_and_delete(connection_id, path, query)

    @app.get("/distinct")
    async def distinct_values(
        connection_id: body_str,
        path: body_path,
        field: body_str,
    ):
        result = storage_api.distinct_values(connection_id, path, field)
        return json_encode(result)

    @app.delete("/")
    async def clear_database(
        connection_id: body_str,
        path: body_path,
    ):
        return storage_api.clear_database(connection_id, path)

    @app.get("/has_collection")
    async def has_collection(
        connection_id: body_str,
        path: body_path,
        collection: body_str,
    ):
        return storage_api.has_collection(connection_id, path, collection)

    @app.get("/collection_names")
    async def collection_names(
        connection_id: body_str,
        path: body_path,
    ):
        return storage_api.collection_names(connection_id, path)

    @app.get("/keys")
    async def keys(
        connection_id: body_str,
        path: body_path,
    ):
        result = storage_api.keys(connection_id, path)
        return list(result)

    return app


if __name__ == "__main__":
    database_file = sys.argv[1]
    cnx = sqlite3.connect(database_file, isolation_level="EXCLUSIVE", timeout=10)
    try:
        sql = (
            "CREATE TABLE IF NOT EXISTS "
            f"[{populse_db_table}] ("
            "category TEXT NOT NULL,"
            "key TEXT NOT NULL,"
            "_json TEXT,"
            "PRIMARY KEY (category, key))"
        )
        cnx.execute(sql)
        row = cnx.execute(
            f"SELECT _json FROM [{populse_db_table}] WHERE category='server' AND key='url'"
        ).fetchone()
        if row:
            raise RuntimeError(f"{database_file} already have a server in {row[0]}")
        url = "http://127.0.0.1:5000"
        cnx.execute(
            f"INSERT INTO [{populse_db_table}] (category, key, _json) VALUES (?,?,?)",
            ["server", "url", url],
        )
        cnx.commit()
    finally:
        cnx.close()
    app = create_server(database_file)
    uvicorn.run(app, port=5000, log_level="critical")
