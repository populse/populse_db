from sqlalchemy import (Column, Table, String, Boolean,
                        Enum, Integer, MetaData, create_engine, Float, Date, DateTime, Time)

# Field types
FIELD_TYPE_STRING = "string"
FIELD_TYPE_INTEGER = "int"
FIELD_TYPE_FLOAT = "float"
FIELD_TYPE_BOOLEAN = "boolean"
FIELD_TYPE_DATE = "date"
FIELD_TYPE_DATETIME = "datetime"
FIELD_TYPE_TIME = "time"
FIELD_TYPE_LIST_STRING = "list_string"
FIELD_TYPE_LIST_INTEGER = "list_int"
FIELD_TYPE_LIST_FLOAT = "list_float"
FIELD_TYPE_LIST_BOOLEAN = "list_boolean"
FIELD_TYPE_LIST_DATE = "list_date"
FIELD_TYPE_LIST_DATETIME = "list_datetime"
FIELD_TYPE_LIST_TIME = "list_time"

LIST_TYPES = [FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER, FIELD_TYPE_LIST_FLOAT,
              FIELD_TYPE_LIST_BOOLEAN, FIELD_TYPE_LIST_DATE, FIELD_TYPE_LIST_DATETIME, FIELD_TYPE_LIST_TIME]
SIMPLE_TYPES = [FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT,
                FIELD_TYPE_BOOLEAN, FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME]
ALL_TYPES = [FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER, FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN, FIELD_TYPE_LIST_DATE, FIELD_TYPE_LIST_DATETIME,
             FIELD_TYPE_LIST_TIME, FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT, FIELD_TYPE_BOOLEAN, FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME]

TYPE_TO_COLUMN = {}
TYPE_TO_COLUMN[FIELD_TYPE_INTEGER] = Integer
TYPE_TO_COLUMN[FIELD_TYPE_LIST_INTEGER] = Integer
TYPE_TO_COLUMN[FIELD_TYPE_FLOAT] = Float
TYPE_TO_COLUMN[FIELD_TYPE_LIST_FLOAT] = Float
TYPE_TO_COLUMN[FIELD_TYPE_BOOLEAN] = Boolean
TYPE_TO_COLUMN[FIELD_TYPE_LIST_BOOLEAN] = Boolean
TYPE_TO_COLUMN[FIELD_TYPE_DATE] = Date
TYPE_TO_COLUMN[FIELD_TYPE_LIST_DATE] = Date
TYPE_TO_COLUMN[FIELD_TYPE_DATETIME] = DateTime
TYPE_TO_COLUMN[FIELD_TYPE_LIST_DATETIME] = DateTime
TYPE_TO_COLUMN[FIELD_TYPE_TIME] = Time
TYPE_TO_COLUMN[FIELD_TYPE_LIST_TIME] = Time
TYPE_TO_COLUMN[FIELD_TYPE_STRING] = String
TYPE_TO_COLUMN[FIELD_TYPE_LIST_STRING] = String

# Tables names
DOCUMENT_TABLE = "document"
INITIAL_TABLE = "initial"
FIELD_TABLE = "field"

DOCUMENT_PRIMARY_KEY = "name"

def create_database(string_engine, initial_table=False):
    """
    Creates the database file with an empty schema
    :param string_engine: Path of the new database file
    :param initial_table: To know if the initial table must be created
    """

    engine = create_engine(string_engine)
    metadata = MetaData(bind=engine)
    fill_tables(metadata, initial_table)
    metadata.create_all(engine)

    # Document primary key name added to the list of columns
    field_table = metadata.tables[FIELD_TABLE]
    insert = field_table.insert().values(name=DOCUMENT_PRIMARY_KEY, type=FIELD_TYPE_STRING, description="Name of the document")
    engine.execute(insert)


def fill_tables(metadata, initial_table):
    """
    Fills the metadata with an empty schema
    :param metadata: Metadata filled
    :param initial_table: To know if the initial table must be created
    """
    Table(FIELD_TABLE, metadata,
          Column("name", String, primary_key=True),
          Column(
              "type", Enum(FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT, FIELD_TYPE_BOOLEAN,
                           FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME,
                           FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER,
                           FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN, FIELD_TYPE_LIST_DATE,
                           FIELD_TYPE_LIST_DATETIME, FIELD_TYPE_LIST_TIME),
              nullable=False),
          Column("description", String, nullable=True))

    Table(DOCUMENT_TABLE, metadata, Column(DOCUMENT_PRIMARY_KEY, String, primary_key=True))

    if initial_table:
        Table(INITIAL_TABLE, metadata, Column(
            DOCUMENT_PRIMARY_KEY, String, primary_key=True))
