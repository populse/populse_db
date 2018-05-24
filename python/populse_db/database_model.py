from sqlalchemy import (Column, Table, String, Boolean,
                        Enum, Integer, MetaData, create_engine, Float, Date, DateTime, Time)

# Tag origin
TAG_ORIGIN_BUILTIN = "builtin"
TAG_ORIGIN_USER = "user"

# Tag type
TAG_TYPE_STRING = "string"
TAG_TYPE_INTEGER = "int"
TAG_TYPE_FLOAT = "float"
TAG_TYPE_BOOLEAN = "boolean"
TAG_TYPE_DATE = "date"
TAG_TYPE_DATETIME = "datetime"
TAG_TYPE_TIME = "time"
TAG_TYPE_LIST_STRING = "list_string"
TAG_TYPE_LIST_INTEGER = "list_int"
TAG_TYPE_LIST_FLOAT = "list_float"
TAG_TYPE_LIST_BOOLEAN = "list_boolean"
TAG_TYPE_LIST_DATE = "list_date"
TAG_TYPE_LIST_DATETIME = "list_datetime"
TAG_TYPE_LIST_TIME = "list_time"

LIST_TYPES = [TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT,
              TAG_TYPE_LIST_BOOLEAN, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME]
SIMPLE_TYPES = [TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT,
                TAG_TYPE_BOOLEAN, TAG_TYPE_DATE, TAG_TYPE_DATETIME, TAG_TYPE_TIME]
ALL_TYPES = [TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_BOOLEAN, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME,
             TAG_TYPE_LIST_TIME, TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_BOOLEAN, TAG_TYPE_DATE, TAG_TYPE_DATETIME, TAG_TYPE_TIME]

TYPE_TO_COLUMN = {}
TYPE_TO_COLUMN[TAG_TYPE_INTEGER] = Integer
TYPE_TO_COLUMN[TAG_TYPE_LIST_INTEGER] = Integer
TYPE_TO_COLUMN[TAG_TYPE_FLOAT] = Float
TYPE_TO_COLUMN[TAG_TYPE_LIST_FLOAT] = Float
TYPE_TO_COLUMN[TAG_TYPE_BOOLEAN] = Boolean
TYPE_TO_COLUMN[TAG_TYPE_LIST_BOOLEAN] = Boolean
TYPE_TO_COLUMN[TAG_TYPE_DATE] = Date
TYPE_TO_COLUMN[TAG_TYPE_LIST_DATE] = Date
TYPE_TO_COLUMN[TAG_TYPE_DATETIME] = DateTime
TYPE_TO_COLUMN[TAG_TYPE_LIST_DATETIME] = DateTime
TYPE_TO_COLUMN[TAG_TYPE_TIME] = Time
TYPE_TO_COLUMN[TAG_TYPE_LIST_TIME] = Time
TYPE_TO_COLUMN[TAG_TYPE_STRING] = String
TYPE_TO_COLUMN[TAG_TYPE_LIST_STRING] = String

# Tag unit
TAG_UNIT_MS = "ms"
TAG_UNIT_MM = "mm"
TAG_UNIT_DEGREE = "degree"
TAG_UNIT_HZPIXEL = "Hz/pixel"
TAG_UNIT_MHZ = "MHz"

ALL_UNITS = [TAG_UNIT_MS, TAG_UNIT_MM,
             TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL, TAG_UNIT_MHZ]

PATH_TABLE = "path"
INITIAL_TABLE = "initial"
TAG_TABLE = "tag"

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

    # Path primary key name added to the list of tags
    tag_table = metadata.tables[TAG_TABLE]
    insert = tag_table.insert().values(name="name", origin=TAG_ORIGIN_BUILTIN,
                                       type=TAG_TYPE_STRING, unit=None, default_value=None, description=None)
    engine.execute(insert)


def fill_tables(metadata, initial_table):
    """
    Fills the metadata with an empty schema
    :param metadata: Metadata filled
    :param initial_table: To know if the initial table must be created
    """
    Table(TAG_TABLE, metadata,
          Column("name", String, primary_key=True),
          Column(
              "origin", Enum(TAG_ORIGIN_BUILTIN, TAG_ORIGIN_USER), nullable=False),
          Column(
              "type", Enum(TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_BOOLEAN,
                           TAG_TYPE_DATE, TAG_TYPE_DATETIME, TAG_TYPE_TIME,
                           TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER,
                           TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_BOOLEAN, TAG_TYPE_LIST_DATE,
                           TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME),
              nullable=False),
          Column(
              "unit", Enum(
                  TAG_UNIT_MS, TAG_UNIT_MM, TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL,
                  TAG_UNIT_MHZ),
              nullable=True),
          Column("default_value", String, nullable=True),
          Column("description", String, nullable=True))

    Table(PATH_TABLE, metadata, Column("name", String, primary_key=True))

    if initial_table:
        Table(INITIAL_TABLE, metadata, Column(
            "name", String, primary_key=True))
