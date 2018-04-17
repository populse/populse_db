from sqlalchemy import (Column, Table, ForeignKeyConstraint, String, Boolean,
                        Enum, Integer, MetaData, create_engine)

# Tag origin
TAG_ORIGIN_RAW = "raw"
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
          Column("visible", Boolean, nullable=False),
          Column(
              "origin", Enum(TAG_ORIGIN_RAW, TAG_ORIGIN_USER), nullable=False),
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
          Column("name", String, unique=True, nullable=False),
          Column("checksum", String, nullable=False),
          Column("index", Integer, primary_key=True, autoincrement=True))

    Table('current', metadata, Column("index", Integer, primary_key=True),
          ForeignKeyConstraint(["index"], ["path.index"], ondelete="CASCADE",
                               onupdate="CASCADE"))

    Table('initial', metadata, Column("index", Integer, primary_key=True),
          ForeignKeyConstraint(["index"], ["path.index"], ondelete="CASCADE",
                               onupdate="CASCADE"))
