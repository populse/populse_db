from sqlalchemy import (Column, Table, String, Boolean,
                        Enum, Integer, MetaData, create_engine, Float, Date, DateTime, Time)

# Tag type
COLUMN_TYPE_STRING = "string"
COLUMN_TYPE_INTEGER = "int"
COLUMN_TYPE_FLOAT = "float"
COLUMN_TYPE_BOOLEAN = "boolean"
COLUMN_TYPE_DATE = "date"
COLUMN_TYPE_DATETIME = "datetime"
COLUMN_TYPE_TIME = "time"
COLUMN_TYPE_LIST_STRING = "list_string"
COLUMN_TYPE_LIST_INTEGER = "list_int"
COLUMN_TYPE_LIST_FLOAT = "list_float"
COLUMN_TYPE_LIST_BOOLEAN = "list_boolean"
COLUMN_TYPE_LIST_DATE = "list_date"
COLUMN_TYPE_LIST_DATETIME = "list_datetime"
COLUMN_TYPE_LIST_TIME = "list_time"

LIST_TYPES = [COLUMN_TYPE_LIST_STRING, COLUMN_TYPE_LIST_INTEGER, COLUMN_TYPE_LIST_FLOAT,
              COLUMN_TYPE_LIST_BOOLEAN, COLUMN_TYPE_LIST_DATE, COLUMN_TYPE_LIST_DATETIME, COLUMN_TYPE_LIST_TIME]
SIMPLE_TYPES = [COLUMN_TYPE_STRING, COLUMN_TYPE_INTEGER, COLUMN_TYPE_FLOAT,
                COLUMN_TYPE_BOOLEAN, COLUMN_TYPE_DATE, COLUMN_TYPE_DATETIME, COLUMN_TYPE_TIME]
ALL_TYPES = [COLUMN_TYPE_LIST_STRING, COLUMN_TYPE_LIST_INTEGER, COLUMN_TYPE_LIST_FLOAT, COLUMN_TYPE_LIST_BOOLEAN, COLUMN_TYPE_LIST_DATE, COLUMN_TYPE_LIST_DATETIME,
             COLUMN_TYPE_LIST_TIME, COLUMN_TYPE_STRING, COLUMN_TYPE_INTEGER, COLUMN_TYPE_FLOAT, COLUMN_TYPE_BOOLEAN, COLUMN_TYPE_DATE, COLUMN_TYPE_DATETIME, COLUMN_TYPE_TIME]

TYPE_TO_COLUMN = {}
TYPE_TO_COLUMN[COLUMN_TYPE_INTEGER] = Integer
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_INTEGER] = Integer
TYPE_TO_COLUMN[COLUMN_TYPE_FLOAT] = Float
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_FLOAT] = Float
TYPE_TO_COLUMN[COLUMN_TYPE_BOOLEAN] = Boolean
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_BOOLEAN] = Boolean
TYPE_TO_COLUMN[COLUMN_TYPE_DATE] = Date
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_DATE] = Date
TYPE_TO_COLUMN[COLUMN_TYPE_DATETIME] = DateTime
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_DATETIME] = DateTime
TYPE_TO_COLUMN[COLUMN_TYPE_TIME] = Time
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_TIME] = Time
TYPE_TO_COLUMN[COLUMN_TYPE_STRING] = String
TYPE_TO_COLUMN[COLUMN_TYPE_LIST_STRING] = String

DOCUMENT_TABLE = "document"
INITIAL_TABLE = "initial"
COLUMN_TABLE = "column"

DOCUMENT_PRIMARY_KEY = "name"

# List value type
VALUE_CURRENT = "current"
VALUE_INITIAL = "initial"


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
    column_table = metadata.tables[COLUMN_TABLE]
    insert = column_table.insert().values(name=DOCUMENT_PRIMARY_KEY, type=COLUMN_TYPE_STRING, description="Name of the document")
    engine.execute(insert)


def fill_tables(metadata, initial_table):
    """
    Fills the metadata with an empty schema
    :param metadata: Metadata filled
    :param initial_table: To know if the initial table must be created
    """
    Table(COLUMN_TABLE, metadata,
          Column("name", String, primary_key=True),
          Column(
              "type", Enum(COLUMN_TYPE_STRING, COLUMN_TYPE_INTEGER, COLUMN_TYPE_FLOAT, COLUMN_TYPE_BOOLEAN,
                           COLUMN_TYPE_DATE, COLUMN_TYPE_DATETIME, COLUMN_TYPE_TIME,
                           COLUMN_TYPE_LIST_STRING, COLUMN_TYPE_LIST_INTEGER,
                           COLUMN_TYPE_LIST_FLOAT, COLUMN_TYPE_LIST_BOOLEAN, COLUMN_TYPE_LIST_DATE,
                           COLUMN_TYPE_LIST_DATETIME, COLUMN_TYPE_LIST_TIME),
              nullable=False),
          Column("description", String, nullable=True))

    Table(DOCUMENT_TABLE, metadata, Column(DOCUMENT_PRIMARY_KEY, String, primary_key=True))

    if initial_table:
        Table(INITIAL_TABLE, metadata, Column(
            DOCUMENT_PRIMARY_KEY, String, primary_key=True))
