import os
from model.DatabaseModel import createDatabase, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT
from sqlalchemy import create_engine, Column, String, Integer, Float, MetaData, Table, ForeignKey
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

            # Setting the column type of the values
            if tag_type is TAG_TYPE_INTEGER:
                column_type = Integer
            elif tag_type is TAG_TYPE_FLOAT:
                column_type = Float
            else:
                column_type = String

            column = Column(name, column_type)
            column_type = column.type.compile(self.engine.dialect)

            """
            # Table and class associated creation
            tag_table = Table(name, self.metadata,
                         Column("id", Integer, primary_key=True, autoincrement=True),
                         Column("index", Integer, ForeignKey(self.classes["path"].index), nullable=False),
                         Column("initial_value", column_type),
                         Column("current_value", column_type))
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare()
            self.classes[name] = getattr(self.base.classes, name)
            tag_table.create()
            """

            # Tag column added to both initial and current tables
            session = self.session_maker()
            self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("initial", name, column_type))
            self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("current", name, column_type))
            self.base = automap_base()
            self.base.prepare(self.engine, reflect=True)
            for table in self.metadata.tables.values():
                table_name = table.name
                self.classes[table_name] = getattr(self.base.classes, table_name)
            session.commit()

            session = self.session_maker()
            initial = self.classes["initial"](index=self.get_scan_index(name))
            current = self.classes["current"](index=self.get_scan_index(name))
            session.add(current)
            session.add(initial)
            session.commit()

    def remove_tag(self, name):
        """
        Removes a tag
        :param name: Tag name
        """

        session = self.session_maker()
        # Tag removed from the Tag table
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        if len(tags) is 1:
            session.delete(tags[0])
            session.commit()

            """
            # Tag table removed
            self.classes[name].__table__.drop()
            self.classes[name] = None
            """

            # Tag column removed from both initial and current tables
            session = self.session_maker()
            self.engine.execute('ALTER TABLE %s DROP COLUMN %s' % ("initial", name))
            self.engine.execute('ALTER TABLE %s DROP COLUMN %s' % ("current", name))
            self.base = automap_base()
            self.base.prepare(self.engine, reflect=True)
            for table in self.metadata.tables.values():
                table_name = table.name
                self.classes[table_name] = getattr(self.base.classes, table_name)
            session.commit()
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
        if len(tags) is 1:
            return tags[0]
        return None

    """ VALUES """

    def get_current_value(self, scan, tag):
        """
        Gives the current value of <scan, tag>
        :param scan: Scan name
        :param tag: Tag name
        :return: The current value of <scan, tag>
        """


        # Checking that the tag table exists
        if self.get_tag(tag) is not None:
            session = self.session_maker()
            values = session.query(self.classes["current"]).filter(self.classes["current"].index == self.get_scan_index(scan)).all()
            session.close()
            if len(values) is 1:
                value = values[0]
                return getattr(value, tag)
        return None

    def get_initial_value(self, scan, tag):
        """
        Gives the initial value of <scan, tag>
        :param scan: Scan name
        :param tag: Tag name
        :return: The initial value of <scan, tag>
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None:
            session = self.session_maker()
            values = session.query(self.classes["initial"]).filter(self.classes["initial"].index == self.get_scan_index(scan)).all()
            session.close()
            if len(values) is 1:
                value = values[0]
                return getattr(value, tag)
        return None

    def is_value_modified(self, scan, tag):
        """
        To know if a value has been modified
        :param scan: scan name
        :param tag: tag name
        :return: True if the value has been modified, False otherwise
        """

        return self.get_current_value(scan, tag) != self.get_initial_value(scan, tag)

    def set_value(self, scan, tag, new_value):
        """
        Sets the value associated to <scan, tag>
        :param scan: scan name
        :param tag: tag name
        :param new_value: New value
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None:
            session = self.session_maker()
            values = session.query(self.classes["current"]).filter(
                self.classes["current"].index == self.get_scan_index(scan)).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, tag, new_value)
            session.commit()

    def reset_value(self, scan, tag):
        """
        Resets the value associated to <scan, tag>
        :param scan: scan name
        :param tag: tag name
        """


        # Checking that the tag exists
        if self.get_tag(tag) is not None:
            session = self.session_maker()
            values = session.query(self.classes["current"]).filter(
                self.classes["current"].index == self.get_scan_index(scan)).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, tag, self.get_initial_value(scan, tag))
            session.commit()

    def remove_value(self, scan, tag):
        """
        Removes the value associated to <scan, tag>
        :param scan: scan name
        :param tag: tag name
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None:
            session = self.session_maker()
            values = session.query(self.classes["current"]).filter(
                self.classes["current"].index == self.get_scan_index(scan)).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, tag, None)
            session.commit()

    def add_value(self, scan, tag, value):
        """
        Adds a value for <scan, tag> (as initial and current)
        :param scan: scan name
        :param tag: tag name
        :param value: value
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None:
            session = self.session_maker()
            scans_initial = session.query(self.classes["initial"]).filter(self.classes["initial"].index == self.get_scan_index(scan)).all()
            scans_current = session.query(self.classes["current"]).filter(self.classes["current"].index == self.get_scan_index(scan)).all()
            if len(scans_initial) is 1 and len(scans_current) is 1:
                scan_initial = scans_initial[0]
                scan_current = scans_current[0]
                database_current_value = getattr(scan_current, tag)
                database_initial_value = getattr(scan_initial, tag)

                # We add the value only if it does not already exist
                if database_current_value is None and database_initial_value is None:
                    setattr(scan_initial, tag, value)
                    setattr(scan_current, tag, value)
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
        if len(scans) is 1:
            scan = scans[0]
            return scan
        return None

    def get_scan_index(self, scan):
        """
        Gives the index of a scan
        :param scan: Scan name
        :return Index of the scan
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"]).filter(self.classes["path"].name == scan).all()
        session.close()
        if len(scans) is 1:
            scan = scans[0]
            return scan.index
        return None

    def remove_scan(self, scan):
        """
        Removes a scan
        :param scan: Scan name
        """

        session = self.session_maker()
        scans = session.query(self.classes["path"]).filter(self.classes["path"].name == scan).all()
        if len(scans) is 1:
            scan = scans[0]
            session.delete(scan)
            session.commit()

            """
            # Values removed from all tag tables
            for table_class in self.classes:
                if table_class != "tag" and table_class != "path":
                    session = self.session_maker()
                    values = session.query(self.classes[table_class]).filter(self.classes[table_class].index == index).all()
                    if len(values) is 1:
                        value = values[0]
                        session.delete(value)
                        session.commit()
                    else:
                        session.close()
            """
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
        scans = session.query(self.classes["path"]).filter(self.classes["path"].name == scan).all()
        if len(scans) is 0:
            scan = self.classes["path"](name=scan, checksum=checksum)
            session.add(scan)
            session.commit()
        else:
            session.close()

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
