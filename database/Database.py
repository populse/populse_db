import os
from model.DatabaseModel import createDatabase, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT
from sqlalchemy import create_engine, Column, String, Integer, Float, MetaData, Table
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
import shutil
import tempfile

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

        # Temporary database file created that will be kept updated
        self.temp_folder = os.path.relpath(tempfile.mkdtemp())
        self.temp_file = os.path.join(self.temp_folder, "temp_database.db")
        shutil.copy(path, self.temp_file)

        # Database opened (temporary file)
        self.engine = create_engine('sqlite:///' + self.temp_file)
        self.metadata = MetaData(bind=self.engine, reflect=True)
        self.session_maker = sessionmaker(bind=self.engine)
        self.base.prepare(self.engine, reflect=True)
        for table in self.metadata.tables.values():
            table_name = table.name
            self.classes[table_name] = getattr(self.base.classes, table_name)

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
            session = self.session_maker()
            tag = self.classes["tag"](name=name, visible=visible, origin=origin, type=tag_type, unit=unit,
                                      default_value=default_value, description=description)
            session.add(tag)
            session.commit()

            # Table and class associated creation
            tag_table = Table(name, self.metadata,
                         Column("id", Integer, primary_key=True, autoincrement=True),
                         Column("index", Integer, nullable=False),
                         Column("initial_value", String),
                         Column("current_value", String))
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare()
            self.classes[name] = getattr(self.base.classes, name)
            tag_table.create()

    def remove_tag(self, name):
        """
        Removes a tag
        :param name: Tag name
        """

        session = self.session_maker()
        # Tag removed from the Tag table
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        if len(tags) == 1:
            session.delete(tags[0])
            session.commit()

            # Tag table removed
            self.classes[name].__table__.drop()
        else:
            session.close()

    def get_tag(self, name):
        """
        Gives the Tag object of the tag
        :param name: Tag name
        :return: The tag object if the tag exists, None otherwise
        """

        session = self.session_maker()
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        session.close()
        if len(tags) == 1:
            return tags[0]
        else:
            return None

    """ VALUES """

    def get_current_value(self, scan, tag):
        """
        Gives the current value of <scan, tag>
        :param scan: Scan name
        :param tag: Tag name
        :return: The current value of <scan, tag>
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"].index).filter(self.classes["path"].name == scan).all()
        if len(scans) == 1:
            scan = scans[0]
            index = scan.index
            values = session.query(self.classes[tag].current_value).filter(self.classes[tag].index == index).all()
            session.close()
            if len(values) == 1:
                value = values[0]
                return value.current_value
            else:
                return None
        else:
            return None

    def get_initial_value(self, scan, tag):
        """
        Gives the initial value of <scan, tag>
        :param scan: Scan name
        :param tag: Tag name
        :return: The initial value of <scan, tag>
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"].index).filter(self.classes["path"].name == scan).all()
        if len(scans) == 1:
            scan = scans[0]
            index = scan.index
            values = session.query(self.classes[tag].initial_value).filter(self.classes[tag].index == index).all()
            session.close()
            if len(values) == 1:
                value = values[0]
                return value.initial_value
            else:
                return None
        else:
            return None

    def is_value_modified(self, scan, tag):
        pass

    def set_value(self, scan, tag, new_value):
        pass

    def reset_value(self, scan, tag):
        pass

    def remove_value(self, scan, tag):
        pass

    def add_value(self, scan, tag, value):
        """
        Adds a value for <scan, tag> (as initial and current)
        :param scan: scan name
        :param tag: tag name
        :param value: value
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"].index).filter(self.classes["path"].name == scan).all()
        if len(scans) == 1:
            scan = scans[0]
            index = scan.index
            value_to_add = self.classes[tag](index=index, initial_value=value, current_value=value)
            session.add(value_to_add)
            session.commit()
        else:
            session.close()

    """ SCANS """

    def get_scan(self, scan):
        """
        Gives the Scan object of a scan
        :param scan: Scan name
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"]).filter(self.classes["path"].name == scan).all()
        session.close()
        if len(scans) == 1:
            scan = scans[0]
            return scan
        else:
            return None

    def remove_scan(self, scan):
        """
        Removes a scan
        :param scan: Scan name
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"]).filter(self.classes["path"].name == scan).all()
        if len(scans) == 1:
            scan = scans[0]
            index = scan.index
            session.delete(scan)
            session.commit()

            # Values removed from all tag tables
            for table_class in self.classes:
                if table_class != "tag" and table_class != "path":
                    session = self.session_maker()
                    values = session.query(self.classes[table_class]).filter(self.classes[table_class].index == index).all()
                    if len(values) == 1:
                        value = values[0]
                        session.delete(value)
                        session.commit()
                    else:
                        session.close()
        else:
            session.close()


    def add_scan(self, scan, checksum):
        """
        Adds a scan
        :param scan: scan path
        :param checksum: scan checksum
        """

        # Adding the scan in the Tag table
        session = self.session_maker()
        scan = self.classes["path"](name=scan, checksum=checksum)
        session.add(scan)
        session.commit()

    def save_modifications(self):
        """
        Saves the modifications by copying the updated temporary database into the real database
        """

        shutil.copy(self.temp_file, self.path)

    def __del__(self):
        """
        Overrides the instance closing to remove the temporary folder
        """

        shutil.rmtree(self.temp_folder)
