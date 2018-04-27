from sqlalchemy import (Column, Table, ForeignKeyConstraint, String, Boolean,
                        Enum, Integer, MetaData, create_engine, Float, Date, DateTime, Time)

# Tag origin
TAG_ORIGIN_BUILTIN = "builtin"
TAG_ORIGIN_USER = "user"

# Tag type
TAG_TYPE_STRING = "string"
TAG_TYPE_INTEGER = "int"
TAG_TYPE_FLOAT = "float"
TAG_TYPE_DATE = "date"
TAG_TYPE_DATETIME = "datetime"
TAG_TYPE_TIME = "time"
TAG_TYPE_LIST_STRING = "list_string"
TAG_TYPE_LIST_INTEGER = "list_int"
TAG_TYPE_LIST_FLOAT = "list_float"
TAG_TYPE_LIST_DATE = "list_date"
TAG_TYPE_LIST_DATETIME = "list_datetime"
TAG_TYPE_LIST_TIME = "list_time"

LIST_TYPES = [TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME]
SIMPLE_TYPES = [TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_DATE, TAG_TYPE_DATETIME, TAG_TYPE_TIME]

TYPE_TO_COLUMN = {}
TYPE_TO_COLUMN[TAG_TYPE_INTEGER] = Integer
TYPE_TO_COLUMN[TAG_TYPE_LIST_INTEGER] = Integer
TYPE_TO_COLUMN[TAG_TYPE_FLOAT] = Float
TYPE_TO_COLUMN[TAG_TYPE_LIST_FLOAT] = Float
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

def create_database(path):
    """
    Creates the database file with an empty schema
    :param path: Path of the new database file
    """

    engine = create_engine('sqlite:///' + path)
    metadata = MetaData(bind=engine)
    fill_tables(metadata)
    metadata.create_all(engine)


def fill_tables(metadata):
    """
    Fills the metadata with an empty schema
    :param metadata: Metadata filled
    """
    Table('tag', metadata,
          Column("name", String, primary_key=True),
          Column(
              "origin", Enum(TAG_ORIGIN_BUILTIN, TAG_ORIGIN_USER), nullable=False),
          Column(
              "type", Enum(TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT,
                           TAG_TYPE_DATE, TAG_TYPE_DATETIME, TAG_TYPE_TIME,
                           TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER,
                           TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_DATE,
                           TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME),
              nullable=False),
          Column(
              "unit", Enum(
                  TAG_UNIT_MS, TAG_UNIT_MM, TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL,
                  TAG_UNIT_MHZ),
              nullable=True),
          Column("default_value", String, nullable=True),
          Column("description", String, nullable=True))

    Table('path', metadata,
          Column("name", String, primary_key=True),
          Column("checksum", String, nullable=True))

    Table('current', metadata, Column("name", String, primary_key=True),
          ForeignKeyConstraint(["name"], ["path.name"], ondelete="CASCADE",
                               onupdate="CASCADE"))

    Table('initial', metadata, Column("name", String, primary_key=True),
          ForeignKeyConstraint(["name"], ["path.name"], ondelete="CASCADE",
                               onupdate="CASCADE"))