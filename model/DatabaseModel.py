from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Boolean, Enum, String
from sqlalchemy import create_engine

# Tag origins
TAG_ORIGIN_RAW = "raw"
TAG_ORIGIN_USER = "user"

# Tag types
TAG_TYPE_STRING = "string"
TAG_TYPE_INTEGER = "integer"
TAG_TYPE_FLOAT = "float"
TAG_TYPE_DATE = "date"
TAG_TYPE_DATETIME = "datetime"
TAG_TYPE_TIME = "time"
TAG_TYPE_LIST_STRING = "list_string"
TAG_TYPE_LIST_INTEGER = "list_integer"
TAG_TYPE_LIST_FLOAT = "list_float"
TAG_TYPE_LIST_DATE = "list_date"
TAG_TYPE_LIST_DATETIME = "list_datetime"
TAG_TYPE_LIST_TIME = "list_time"

# Tag units
TAG_UNIT_MS = "ms"
TAG_UNIT_MM = "mm"
TAG_UNIT_DEGREE = "degree"
TAG_UNIT_HZPIXEL = "Hz/pixel"
TAG_UNIT_MHZ = "MHz"

Base = declarative_base()

def createDatabase(path):
    """
    Creates the database with the following schema
    :param path:Path of the database created
    """
    engine = create_engine('sqlite:///' + path)
    Base.metadata.create_all(engine)

class Path_Initial(Base):
    """
    Table that contains the initial values
    """
    __tablename__ = 'path_initial'
    name = Column(String, primary_key=True)
    checksum = Column(String, nullable=False)

    def __repr__(self):
        return "<Path_Initial(name='%s', checksum='%s')>" % (self.name, self.checksum)

class Path_Current(Base):
    """
    Table that contains the current values
    """
    __tablename__ = 'path_current'
    name = Column(String, primary_key=True)

    def __repr__(self):
        return "<Path_Current(name='%s', checksum='%s')>" % (self.name, self.checksum)

class Tag(Base):
    """
    Table that contains the tags properties
    """
    __tablename__ = 'tag'
    tag = Column(String, primary_key=True)
    visible = Column(Boolean, nullable=False)
    origin = Column(Enum(TAG_ORIGIN_RAW, TAG_ORIGIN_USER), nullable=False)
    type = Column(Enum(TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_DATE, TAG_TYPE_DATETIME, TAG_TYPE_TIME, TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME), nullable=False)
    unit = Column(Enum(TAG_UNIT_MS, TAG_UNIT_MM, TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL, TAG_UNIT_MHZ), nullable=True)
    default_value = Column(String, nullable=True)
    description = Column(String, nullable=True)

    def __repr__(self):
        return "<Tag(tag='%s', visible='%s', origin='%s', type='%s', unit='%s', default_value='%s', description='%s')>" % (self.tag, self.visible, self.origin, self.type, self.unit, self.default_value, self.description)