import os
from model.DatabaseModel import createDatabase, Base, Tag, Path_Initial, Path_Current, TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.orm import sessionmaker

class Database:

    def __init__(self, path):
        """
        Creates the database instance that will do the API between the database and the software
        :param path: Path of the database file
        """

        self.path = path

        # We create the database file if it does not already exist
        if not os.path.exists(self.path):
            createDatabase(self.path)

        self.engine = create_engine('sqlite:///' + self.path)
        Base.metadata.bind = self.engine
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()

    """ TAGS """

    def add_tag(self, name, visible, origin, type, unit, default_value, description):
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
            tag = Tag(name=name, visible=visible, origin=origin, type=type, unit=unit, default_value=default_value, description=description)
            self.session.add(tag)

            # Adding the tag in the Path tables
            if type == TAG_TYPE_INTEGER:
                column = Column(name, Integer, nullable=True)
            elif type == TAG_TYPE_FLOAT:
                column = Column(name, Float, nullable=True)
            else:
                column = Column(name, String, nullable=True)
            column_name = column.compile(dialect=self.engine.dialect)
            column_type = column.type.compile(self.engine.dialect)
            self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("path_initial", column_name, column_type))
            self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("path_current", column_name, column_type))

    def remove_tag(self, name):
        """
        Removes a tag
        :param name: Tag name
        """

        # Tag removed from the Tag table
        tags = self.session.query(Tag).filter(Tag.name == name).all()
        if len(tags) == 1:
            self.session.delete(tags[0])

            # Tag removed from the Path tables

    def get_tag(self, name):
        """
        Gives the Tag object of the tag
        :param name: Tag name
        :return: The tag object if the tag exists, None otherwise
        """

        tags = self.session.query(Tag).filter(Tag.name == name).all()
        if len(tags) == 1:
            return tags[0]
        else:
            return None

    """ VALUES """

    def get_current_value(self, scan, tag):
        pass

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
        pass

    """ SCANS """

    def remove_scan(self, scan):
        pass

    def add_scan(self, scan, checksum):
        pass