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
FIELD_TABLE = "field"
COLLECTION_TABLE = "collection"

def create_database(string_engine):
    """
    Creates the database file with an empty schema
    :param string_engine: Path of the new database file
    """

    engine = create_engine(string_engine)
    metadata = MetaData(bind=engine)
    fill_tables(metadata)
    metadata.create_all(engine)

def fill_tables(metadata):
    """
    Fills the metadata with an empty schema
    :param metadata: Metadata filled
    """
    Table(FIELD_TABLE, metadata,
          Column("name", String, primary_key=True),
          Column("collection", String, primary_key=True),
          Column(
              "type", Enum(FIELD_TYPE_STRING, FIELD_TYPE_INTEGER, FIELD_TYPE_FLOAT, FIELD_TYPE_BOOLEAN,
                           FIELD_TYPE_DATE, FIELD_TYPE_DATETIME, FIELD_TYPE_TIME,
                           FIELD_TYPE_LIST_STRING, FIELD_TYPE_LIST_INTEGER,
                           FIELD_TYPE_LIST_FLOAT, FIELD_TYPE_LIST_BOOLEAN, FIELD_TYPE_LIST_DATE,
                           FIELD_TYPE_LIST_DATETIME, FIELD_TYPE_LIST_TIME),
              nullable=False),
          Column("description", String, nullable=True))

    Table(COLLECTION_TABLE, metadata,
          Column("name", String, primary_key=True),
          Column("primary_key", String, nullable=False))

    # Put collection foreign key in field table
