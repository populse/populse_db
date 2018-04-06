import os
from model.DatabaseModel import createDatabase, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_TIME, TAG_TYPE_DATETIME, TAG_TYPE_DATE, TAG_TYPE_STRING, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_TIME
from sqlalchemy import create_engine, Column, String, Integer, Float, MetaData, Date, DateTime, Time, Table, ForeignKeyConstraint
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.interfaces import PoolListener
from sqlalchemy.schema import CreateTable
import shutil
import tempfile
from datetime import date, time, datetime

class ForeignKeysListener(PoolListener):
    """
    Class to activate the pragma case_sensitive_like, that makes the like and contains functions case sensitive
    """
    def connect(self, dbapi_con, con_record):
        db_cursor = dbapi_con.execute('pragma case_sensitive_like=ON')
        db_cursor = dbapi_con.execute('pragma foreign_keys=ON')

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
        self.engine = create_engine('sqlite:///' + self.temp_file, listeners=[ForeignKeysListener()])
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
            elif tag_type is TAG_TYPE_DATE:
                column_type = Date
            elif tag_type is TAG_TYPE_DATETIME:
                column_type = DateTime
            elif tag_type is TAG_TYPE_TIME:
                column_type = Time
            else:
                column_type = String

            if self.is_tag_list(name):

                # Tag is list: new table added
                tag_table_current = Table(name + "_current", self.metadata,
                                  Column("index", Integer, nullable=False, primary_key=True),
                                  Column("order", Integer, nullable=False, primary_key=True),
                                  Column("value", column_type),
                                  ForeignKeyConstraint(["index"], ["path.index"], ondelete="CASCADE",
                                                       onupdate="CASCADE"))
                tag_table_initial = Table(name + "_initial", self.metadata,
                                          Column("index", Integer, nullable=False, primary_key=True),
                                          Column("order", Integer, nullable=False, primary_key=True),
                                          Column("value", column_type),
                                          ForeignKeyConstraint(["index"], ["path.index"], ondelete="CASCADE",
                                                               onupdate="CASCADE"))
                tag_table_current.create()
                tag_table_initial.create()
                self.base = automap_base(metadata=self.metadata)
                self.base.prepare()
                self.classes[name + "_initial"] = getattr(self.base.classes, name + "_initial")
                self.classes[name + "_current"] = getattr(self.base.classes, name + "_current")
            else:

                # Tag is not list, new column added
                column = Column(name, column_type)
                column_type = column.type.compile(self.engine.dialect)

                # Tag column added to both initial and current tables
                session = self.session_maker()
                self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("initial", name.replace(" ", ""), column_type))
                self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("current", name.replace(" ", ""), column_type))
                self.base = automap_base()
                self.base.prepare(self.engine, reflect=True)
                for table in self.metadata.tables.values():
                    table_name = table.name
                    self.classes[table_name] = getattr(self.base.classes, table_name)
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

            # Tag column removed from initial table
            columns = ""
            columns_list = []
            sql_table_create = CreateTable(self.classes["initial"].__table__)
            for column in sql_table_create.columns:
                if name in str(column):
                    column_to_remove = column
                else:
                    columns += str(column).split(" ")[0] + ", "
                    columns_list.append(str(column).split(" ")[0])
            sql_table_create.columns.remove(column_to_remove)
            sql_query = str(sql_table_create)
            sql_query = sql_query[:21] + '_backup' + sql_query[21:]
            columns = columns[:-2]
            columns = columns.replace("index", "\"index\"")
            sql_query = sql_query.replace("index", "\"index\"")
            session = self.session_maker()
            session.execute(sql_query)
            session.execute("INSERT INTO initial_backup SELECT " + columns + " FROM initial")
            session.execute("DROP TABLE initial")
            sql_query = sql_query[:21] + sql_query[29:]
            session.execute(sql_query)
            session.execute("INSERT INTO initial SELECT " + columns + " FROM initial_backup")
            session.execute("DROP TABLE initial_backup")

            # Tag column removed from current table
            columns = ""
            columns_list = []
            sql_table_create = CreateTable(self.classes["current"].__table__)
            for column in sql_table_create.columns:
                if name in str(column):
                    column_to_remove = column
                else:
                    columns += str(column).split(" ")[0] + ", "
                    columns_list.append(str(column).split(" ")[0])
            sql_table_create.columns.remove(column_to_remove)
            sql_query = str(sql_table_create)
            sql_query = sql_query[:21] + '_backup' + sql_query[21:]
            columns = columns[:-2]
            columns = columns.replace("index", "\"index\"")
            sql_query = sql_query.replace("index", "\"index\"")
            session = self.session_maker()
            session.execute(sql_query)
            session.execute("INSERT INTO current_backup SELECT " + columns + " FROM current")
            session.execute("DROP TABLE current")
            sql_query = sql_query[:21] + sql_query[29:]
            session.execute(sql_query)
            session.execute("INSERT INTO current SELECT " + columns + " FROM current_backup")
            session.execute("DROP TABLE current_backup")

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

    def get_tag_type(self, name):
        """
        Gives the tag type
        :param name: Tag name
        :return: The tag type: See possibilities in DatabaseModel.py
        """

        session = self.session_maker()
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        session.close()
        if len(tags) is 1:
            return tags[0].type
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

            if self.is_tag_list(tag):

                # The tag is a list, we get the values from the tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                session.close()
                if len(values) is 0:
                    return None
                values_list = []
                for value in values:
                    value_to_add = value.value
                    tag_type = self.get_tag_type(tag)
                    if tag_type == TAG_TYPE_LIST_INTEGER:
                        value_to_add = int(value_to_add)
                    elif tag_type == TAG_TYPE_LIST_STRING:
                        value_to_add = str(value_to_add)
                    elif tag_type == TAG_TYPE_LIST_FLOAT:
                        value_to_add = float(value_to_add)
                    # TODO add other types
                    values_list.insert(value.order, value_to_add)
                return values_list

            else:

                # The tag is not a list, we get the value from current table
                session = self.session_maker()
                values = session.query(self.classes["current"]).filter(self.classes["current"].index == self.get_scan_index(scan)).all()
                session.close()
                if len(values) is 1:
                    value = values[0]
                    return getattr(value, tag.replace(" ", ""))
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

            if self.is_tag_list(tag):
                # The tag is a list, we get the values from the tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_initial"]).filter(
                    self.classes[tag + "_initial"].index == self.get_scan_index(scan)).all()
                session.close()
                if len(values) is 0:
                    return None
                values_list = []
                for value in values:
                    value_to_add = value.value
                    tag_type = self.get_tag_type(tag)
                    if tag_type == TAG_TYPE_LIST_INTEGER:
                        value_to_add = int(value_to_add)
                    elif tag_type == TAG_TYPE_LIST_STRING:
                        value_to_add = str(value_to_add)
                    elif tag_type == TAG_TYPE_LIST_FLOAT:
                        value_to_add = float(value_to_add)
                    # TODO add other types
                    values_list.insert(value.order, value_to_add)
                return values_list

            else:

                # The tag is a list, we get the values from the tag initial table
                session = self.session_maker()
                values = session.query(self.classes["initial"]).filter(self.classes["initial"].index == self.get_scan_index(scan)).all()
                session.close()
                if len(values) is 1:
                    value = values[0]
                    return getattr(value, tag.replace(" ", ""))
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
        if self.get_tag(tag) is not None and self.check_type_value(new_value, self.get_tag_type(tag)) and self.get_scan_index(scan) is not None:

            if self.is_tag_list(tag):

                # The scan is a list type, we reset the values in the tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                for index in range(0, len(values)):
                    value_to_modify = values[index]
                    value_to_modify.value = new_value[index]
                session.commit()

            else:

                # The scan is a simple type, we set the value in current table
                session = self.session_maker()
                values = session.query(self.classes["current"]).filter(
                    self.classes["current"].index == self.get_scan_index(scan)).all()
                if len(values) is 1:
                    value = values[0]
                    setattr(value, tag.replace(" ", ""), new_value)
                session.commit()

    def reset_value(self, scan, tag):
        """
        Resets the value associated to <scan, tag>
        :param scan: scan name
        :param tag: tag name
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None and self.get_scan_index(scan) is not None:

            if self.is_tag_list(tag):

                # The scan is a list type, we reset the values in the tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                for index in range(0, len(values)):
                    value_to_modify = values[index]
                    value_to_modify.value = self.get_initial_value(scan, tag)[index]
                session.commit()

            else:

                # The scan is a simple type, we reset the value in current table
                session = self.session_maker()
                values = session.query(self.classes["current"]).filter(
                    self.classes["current"].index == self.get_scan_index(scan)).all()
                if len(values) is 1:
                    value = values[0]
                    setattr(value, tag.replace(" ", ""), self.get_initial_value(scan, tag))
                session.commit()

    def remove_value(self, scan, tag):
        """
        Removes the value associated to <scan, tag>
        :param scan: scan name
        :param tag: tag name
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None and self.get_scan_index(scan) is not None:

            if self.is_tag_list(tag):

                # The tag has a list type, we remove the values from tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                for value in values:
                    session.delete(value)
                session.commit()

            else:

                # The tag has a simple type, we remove it from the tag column in the current table
                session = self.session_maker()
                values = session.query(self.classes["current"]).filter(
                    self.classes["current"].index == self.get_scan_index(scan)).all()
                if len(values) is 1:
                    value = values[0]
                    setattr(value, tag.replace(" ", ""), None)
                session.commit()

    def check_type_value(self, value, valid_type):
        """
        Checks the type of the value
        :param value: value
        :param type: type that the value should have
        :return: True if the value is valid, False otherwise
        """

        value_type = type(value)
        if valid_type == TAG_TYPE_INTEGER and value_type == int:
            return True
        if valid_type == TAG_TYPE_FLOAT and value_type == int:
            return True
        if valid_type == TAG_TYPE_FLOAT and value_type == float:
            return True
        if valid_type == TAG_TYPE_STRING and value_type == str:
            return True
        if valid_type == TAG_TYPE_DATETIME and value_type == datetime:
            return True
        if valid_type == TAG_TYPE_TIME and value_type == time:
            return True
        if valid_type == TAG_TYPE_DATE and value_type == date:
            return True
        if valid_type == TAG_TYPE_LIST_DATE and value_type == list:
            return True
        if valid_type == TAG_TYPE_LIST_DATETIME and value_type == list:
            return True
        if valid_type == TAG_TYPE_LIST_TIME and value_type == list:
            return True
        if valid_type == TAG_TYPE_LIST_STRING and value_type == list:
            return True
        if valid_type == TAG_TYPE_LIST_FLOAT and value_type == list:
            return True
        if valid_type == TAG_TYPE_LIST_INTEGER and value_type == list:
            return True
        return False

    def is_tag_list(self, tag):
        """
        To know if the given tag is a list
        :param tag: tag
        :return: True if the tag is a list, False otherwise
        """

        tag_type = self.get_tag(tag).type
        if tag_type == TAG_TYPE_LIST_DATETIME:
            return True
        if tag_type == TAG_TYPE_LIST_TIME:
            return True
        if tag_type == TAG_TYPE_LIST_DATE:
            return True
        if tag_type == TAG_TYPE_LIST_STRING:
            return True
        if tag_type == TAG_TYPE_LIST_INTEGER:
            return True
        if tag_type == TAG_TYPE_LIST_FLOAT:
            return True
        return False

    def add_value(self, scan, tag, value):
        """
        Adds a value for <scan, tag> (as initial and current)
        :param scan: scan name
        :param tag: tag name
        :param value: value
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None and self.check_type_value(value, self.get_tag_type(tag)) and self.get_scan_index(scan) is not None:

            if self.is_tag_list(tag):

                # The tag is a list, we add it in the tag tables
                session = self.session_maker()
                for order in range(0, len(value)):
                    element = value[order]
                    current_to_add = self.classes[tag + "_current"](index=self.get_scan_index(scan), order=order, value=element)
                    initial_to_add = self.classes[tag + "_initial"](index=self.get_scan_index(scan), order=order, value=element)
                    session.add(current_to_add)
                    session.add(initial_to_add)
                session.commit()
            else:

                # The tag is not a list, we add it in both current and initial tables
                session = self.session_maker()
                scans_initial = session.query(self.classes["initial"]).filter(self.classes["initial"].index == self.get_scan_index(scan)).all()
                scans_current = session.query(self.classes["current"]).filter(self.classes["current"].index == self.get_scan_index(scan)).all()
                if len(scans_initial) is 1 and len(scans_current) is 1:
                    scan_initial = scans_initial[0]
                    scan_current = scans_current[0]
                    database_current_value = getattr(scan_current, tag.replace(" ", ""))
                    database_initial_value = getattr(scan_initial, tag.replace(" ", ""))

                    # We add the value only if it does not already exist
                    if database_current_value is None and database_initial_value is None:
                        setattr(scan_initial, tag.replace(" ", ""), value)
                        setattr(scan_current, tag.replace(" ", ""), value)
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

            # Thanks to the foreign key and on delete cascade, the scan is also removed from both initial and current tables

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
            scan_to_add = self.classes["path"](name=scan, checksum=checksum)
            session.add(scan_to_add)
            session.commit()

            # Adding the index to both initial and current tables
            session = self.session_maker()
            initial = self.classes["initial"](index=self.get_scan_index(scan))
            current = self.classes["current"](index=self.get_scan_index(scan))
            session.add(current)
            session.add(initial)
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
