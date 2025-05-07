import argparse
import json
from typing import Annotated

import uvicorn
from fastapi import Body, Query, FastAPI, Request
from fastapi.responses import JSONResponse

from .database import json_decode, json_encode, populse_db_table
from .storage_api import StorageFileAPI, serialize_exception

body_str = Annotated[str, Body(embed=True)]
body_path = Annotated[list[str | int | list[str]], Body(embed=True)]
body_bool = Annotated[bool, Body()]
body_dict = Annotated[dict, Body()]
body_json = Annotated[str | int | float | bool | None | list | dict, Body()]

query_str = Annotated[str, Query()]
query_path = Annotated[str, Query()]
query_bool = Annotated[bool, Query()]
query_dict = Annotated[dict, Query()]
query_json = Annotated[str, Query()]


def str_to_json(value):
    if value and (value[0] in {"[", '"', "{"} or value[0].isdigit()):
        return json.loads(value)
    return value

def create_server(database_file, create=True):
    storage_api = StorageFileAPI(database_file, create=create)
    app = FastAPI()

    @app.middleware("http")
    async def cors_middleware(request: Request, call_next):

        response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

    @app.middleware("http")
    async def catch_exceptions_middleware(request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content=serialize_exception(exc),
            )

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
        connection_id: query_str,
        path: query_path,
        default: query_json = None,
        fields: Annotated[list[str] | None, Query()] = None,
        as_list: query_bool = False,
        distinct: query_bool = False,
    ):
        result = storage_api.get(
            connection_id,
            str_to_json(path),
            str_to_json(default),
            fields,
            as_list,
            distinct,
        )
        return json_encode(result)

    @app.get("/count")
    async def count(
        connection_id: query_str,
        path: query_path,
        query: Annotated[str | None, Query()] = None,
    ):
        return storage_api.count(connection_id, str_to_json(path), query)

    @app.get("/primary_key")
    async def primary_key(connection_id: query_str, path: query_path):
        return storage_api.primary_key(connection_id, str_to_json(path))

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
        connection_id: query_str,
        path: query_path,
        query: Annotated[str | None, Query()] = None,
        fields: Annotated[list[str] | None, Query()] = None,
        as_list: query_bool = False,
        distinct: query_bool = False,
    ):
        result = storage_api.search(
            connection_id, str_to_json(path), query, fields, as_list, distinct
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
        connection_id: query_str,
        path: query_path,
        field: query_str,
    ):
        result = storage_api.distinct_values(connection_id, str_to_json(path), field)
        return json_encode(result)

    @app.delete("/")
    async def clear_database(
        connection_id: body_str,
        path: body_path,
    ):
        return storage_api.clear_database(connection_id, path)

    @app.get("/has_collection")
    async def has_collection(
        connection_id: query_str,
        path: query_path,
        collection: query_str,
    ):
        return storage_api.has_collection(connection_id, str_to_json(path), collection)

    @app.get("/collection_names")
    async def collection_names(
        connection_id: query_str,
        path: query_path,
    ):
        return storage_api.collection_names(connection_id, str_to_json(path))

    @app.get("/keys")
    async def keys(
        connection_id: query_str,
        path: query_path,
    ):
        result = storage_api.keys(connection_id, str_to_json(path))
        return list(result)

    return app


if __name__ == "__main__":
    import sqlite3

    parser = argparse.ArgumentParser(
        prog="python -m populse_db.server",
        description="Run a web server fo a populse_db database",
    )

    parser.add_argument("database")
    parser.add_argument("-b", "--bind", default="0.0.0.0")
    parser.add_argument("-p", "--port", default="8080")
    parser.add_argument("-u", "--url", default=None)
    parser.add_argument("-v", "--verbose", action="store_true")

    options = parser.parse_args()
    cnx = sqlite3.connect(options.database, isolation_level="EXCLUSIVE", timeout=10)
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
            raise RuntimeError(f"{options.database} already have a server in {row[0]}")
        if options.url is None:
            if options.bind == "0.0.0.0":
                host = "127.0.0.1"
            else:
                host = options.bind
            options.url = f"http://{host}:{options.port}"
        if options.verbose:
            print("Storing external URL:", options.url)
        cnx.execute(
            f"INSERT INTO [{populse_db_table}] (category, key, _json) VALUES (?,?,?)",
            ["server", "url", options.url],
        )
        cnx.commit()
    finally:
        cnx.close()
    try:
        app = create_server(options.database)
        uvicorn.run(
            app,
            host=options.bind,
            port=int(options.port),
            log_level=("debug" if options.verbose else "critical"),
        )
    finally:
        cnx = sqlite3.connect(options.database, isolation_level="EXCLUSIVE", timeout=10)
        try:
            cnx.execute(
                f"DELETE FROM [{populse_db_table}] WHERE category='server' AND key='url'"
            )
            cnx.commit()
        finally:
            cnx.close()
