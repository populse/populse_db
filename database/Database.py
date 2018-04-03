import os
from model.DatabaseModel import createDatabase, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT
from sqlalchemy import create_engine, Column, String, Integer, Float, MetaData, Table
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

class Database:

    def __init__(self, path):
        """
        Creates the database instance that will do the API between the database and the software
        :param path: Path of the database file
        """

        self.path = path
        self.classes = {}
        self.base = automap_base()

        # We create the database file if it does not already exist
        if not os.path.exists(self.path):
            createDatabase(self.path)

        self.engine = create_engine('sqlite:///' + self.path)
        self.metadata = MetaData(bind=self.engine)
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()
        self.base.prepare(self.engine, reflect=True)
        self.classes["tag"] = self.base.classes.tag
        self.classes["path"] = self.base.classes.path

    """ TAGS """

    def add_tag(self, name, visible, origin, tag_type, unit, default_value, description):
        """
        Adds a tag to the database if it does not already exist
        :param name: Tag name
        :param visible: Tag visibility (True or False)
        :param origin: Tag origin (Raw or user)
        :param type: Tag type (string, integer, float, date, datetime, time, list_string, list_integer, list_float, list_date, list_datetime, or list_time)
        :param unit: Tag unit (ms, mm, degree, Hz/pixel, or MHz)
        :param default_value: Tag default value
        :param description: Tag description
        """

        # We add the tag if it does not already exist
        if self.get_tag(name) is None:

            # Adding the tag in the Tag table
            tag = self.classes["tag"](name=name, visible=visible, origin=origin, type=tag_type, unit=unit,
                                      default_value=default_value, description=description)
            self.session.add(tag)

            tag_table = Table(name, self.metadata,
                         Column("id", Integer, primary_key=True, autoincrement=True),
                         Column("index", Integer, nullable=False),
                         Column("initial_value", String),
                         Column("current_value", String))
            self.session.commit() #self.session.add(tag_table)
            tag_table.create(self.engine)
            self.base = automap_base()
            self.base.prepare(self.engine, reflect=True)
            self.classes[name] = getattr(self.base.classes, name)

    def remove_tag(self, name):
        """
        Removes a tag
        :param name: Tag name
        """

        # Tag removed from the Tag table
        tags = self.session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        if len(tags) == 1:
            self.session.delete(tags[0])

            # Tag removed from the Path tables

    def get_tag(self, name):
        """
        Gives the Tag object of the tag
        :param name: Tag name
        :return: The tag object if the tag exists, None otherwise
        """

        tags = self.session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        if len(tags) == 1:
            return tags[0]
        else:
            return None

    """ VALUES """

    def get_current_value(self, scan, tag):
        #print(getattr(Path_Current, tag))
        scans = self.session.query(self.classes["path"]).filter(self.classes["path"].name == scan).all()
        if len(scans) == 1:
            scan = scans[0]
        else:
            return None

    def get_initial_value(self, scan, tag):
        pass

    def is_value_modified(self, scan, tag):
        pass

    def set_value(self, scan, tag, new_value):
        pass

    def reset_value(self, scan, tag):
        pass

    def remove_value(self, scan, tag):
        pass

    def add_value(self, scan, tag, value):
        values = self.session.query(Path_Current).filter(Path_Current.name == scan).all()

    """ SCANS """

    def remove_scan(self, scan):
        pass

    def add_scan(self, scan, checksum):
        """
        Adds a scan
        :param scan: scan path
        :param checksum: scan checksum
        """

        # Adding the scan in the Tag table
        scan = self.classes["path"](name=scan, checksum=checksum)
        self.session.add(scan)