import os
import shutil
import tempfile
from datetime import date, time, datetime

from sqlalchemy import create_engine, Column, String, Integer, Float, MetaData, Date, DateTime, Time, Table, \
    ForeignKeyConstraint
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.interfaces import PoolListener
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateTable

from populse_db.DatabaseModel import createDatabase, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_TIME, TAG_TYPE_DATETIME, \
    TAG_TYPE_DATE, TAG_TYPE_STRING, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_FLOAT, \
    TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_TIME, TAG_UNIT_MS, TAG_UNIT_MM, TAG_UNIT_HZPIXEL, \
    TAG_UNIT_DEGREE, TAG_UNIT_MHZ, TAG_ORIGIN_USER, TAG_ORIGIN_RAW


class ForeignKeysListener(PoolListener):
    """
    Manages the database pragmas
    """

    def connect(self, dbapi_con, con_record):
        """
        Manages the pragmas during the database opening
        :param dbapi_con:
        :param con_record:
        """
        dbapi_con.execute('pragma case_sensitive_like=ON')
        dbapi_con.execute('pragma foreign_keys=ON')


class Database:
    """
    Database API

    attributes:
        - path: path of the database file
        - classes: list of table classes, generated automatically
        - base: database base
        - temp_folder: temporary folder containing the temporary database file
        - temp_file: temporary database file
        - engine: database engine
        - metadata: database metadata
        - session_maker: sessions manager

    methods:
        - add_tag: adds a tag
        - remove_tag: removes a tag
        - get_tag: Gives the tag table object of a tag
        - get_tag_type: gives the tag type
        - get_current_value: gives the current value of <scan, tag>
        - get_initial_value: gives the initial value of <scan, tag>
        - is_value_modified: to know if a value has been modified
        - set_value: sets the value of <scan, tag>
        - reset_value: resets the value of <scan, tag>
        - remove_value: removes the value of <scan, tag>
        - check_type_value: checks the type of a value
        - is_tag_list: to know if a tag has a list type
        - add_value: adds a value to <scan, tag>
        - get_scan: gives the path table object of a scan
        - get_scan_index: gives the index of a scan
        - add_scan: adds a scan
        - remove_scan: removes a scan
        - save_modifications: saves the pending modifications to the original database
    """

    def __init__(self, path):
        """
        Creates an API of the database instance
        :param path: Path of the database file, can be already existing, or not.
        """

        self.path = path
        self.classes = {}
        self.base = automap_base()

        # Creation the database file if it does not already exist
        if not os.path.exists(self.path):
            createDatabase(self.path)

        # TODO If the database file exists, check the schema to ensure the coherence

        # Temporary database file created that will be kept updated
        self.temp_folder = os.path.relpath(tempfile.mkdtemp())
        self.temp_file = os.path.join(self.temp_folder, "temp_database.db")
        shutil.copy(path, self.temp_file)

        # Database opened (temporary database file being a copy of the original database file)
        self.engine = create_engine('sqlite:///' + self.temp_file, listeners=[ForeignKeysListener()])

        # Metadata generated
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect(bind=self.engine)

        self.session_maker = sessionmaker(bind=self.engine)

        # Table classes generated, that convert the database into python objects, and allow the communication
        self.base.prepare(self.engine, reflect=True)
        for table in self.metadata.tables.values():
            table_name = table.name
            self.classes[table_name] = getattr(self.base.classes, table_name)

    """ TAGS """

    def add_tag(self, name, visible, origin, tag_type, unit, default_value, description):
        """
        Adds a tag to the database, if it does not already exist
        :param name: Tag name (str)
        :param visible: Tag visibility (True or False)
        :param origin: Tag origin (Raw or user)
        :param type: Tag type (string, integer, float, date, datetime, time, list_string, list_integer, list_float, list_date, list_datetime, or list_time)
        :param unit: Tag unit (ms, mm, degree, Hz/pixel, MHz, or None)
        :param default_value: Tag default value (str or None)
        :param description: Tag description (str or None)
        """

        # Parameters checked
        if type(name) is not str:
            return
        if type(visible) is not bool:
            return
        if origin not in [TAG_ORIGIN_USER, TAG_ORIGIN_RAW]:
            return
        if tag_type not in [TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_STRING, TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME, TAG_TYPE_DATETIME, TAG_TYPE_DATE, TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT, TAG_TYPE_TIME]:
            return
        if unit not in [TAG_UNIT_MHZ, TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL, TAG_UNIT_MM, TAG_UNIT_MS] and unit is not None:
            return
        if type(default_value) is not str and default_value is not None:
            return
        if type(description) is not str and description is not None:
            return

        # Tag added if it does not already exist
        if self.get_tag(name) is None:

            # Adding the tag in the tag table
            session = self.session_maker()
            tag = self.classes["tag"](name=name, visible=visible, origin=origin, type=tag_type, unit=unit,
                                      default_value=default_value, description=description)
            session.add(tag)
            session.commit()

            # Setting the column type of the tag values
            if tag_type is TAG_TYPE_INTEGER or tag_type is TAG_TYPE_LIST_INTEGER:
                column_type = Integer
            elif tag_type is TAG_TYPE_FLOAT or tag_type is TAG_TYPE_LIST_FLOAT:
                column_type = Float
            elif tag_type is TAG_TYPE_DATE or tag_type is TAG_TYPE_LIST_DATE:
                column_type = Date
            elif tag_type is TAG_TYPE_DATETIME or tag_type is TAG_TYPE_LIST_DATETIME:
                column_type = DateTime
            elif tag_type is TAG_TYPE_TIME or tag_type is TAG_TYPE_LIST_TIME:
                column_type = Time
            elif tag_type is TAG_TYPE_STRING or tag_type is TAG_TYPE_LIST_STRING:
                column_type = String

            if self.is_tag_list(name):
                # The tag has a list type: new table added

                # Tag tables initial and current definition
                tag_table_current = Table(name + "_current", self.metadata,
                                          Column("index", Integer, primary_key=True),
                                          Column("order", Integer, primary_key=True),
                                          Column("value", column_type, nullable=False),
                                          ForeignKeyConstraint(["index"], ["path.index"], ondelete="CASCADE",
                                                               onupdate="CASCADE"))
                tag_table_initial = Table(name + "_initial", self.metadata,
                                          Column("index", Integer, primary_key=True),
                                          Column("order", Integer, primary_key=True),
                                          Column("value", column_type, nullable=False),
                                          ForeignKeyConstraint(["index"], ["path.index"], ondelete="CASCADE",
                                                               onupdate="CASCADE"))
                # Both tables added
                tag_table_current.create()
                tag_table_initial.create()

                # Redefinition of the table classes
                self.base = automap_base(metadata=self.metadata)
                self.base.prepare()
                self.classes[name + "_initial"] = getattr(self.base.classes, name + "_initial")
                self.classes[name + "_current"] = getattr(self.base.classes, name + "_current")
            else:
                # The tag has a simple type: new column added to both initial and current tables

                # Column creation
                column = Column(name, column_type)
                column_type = column.type.compile(self.engine.dialect)

                # Tag column added to both initial and current tables
                session = self.session_maker()
                self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("initial", name.replace(" ", ""), column_type))
                self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % ("current", name.replace(" ", ""), column_type))

                # Redefinition of the table classes
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

        # Parameters checked
        if type(name) is not str:
            return
        if self.get_tag(name) is None:
            return

        is_tag_list = self.is_tag_list(name)
        session = self.session_maker()
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()

        if len(tags) is 1:

            # The tag exists, it is removed from the tag table
            session.delete(tags[0])
            session.commit()

            if is_tag_list:

                # The tag has a list type, both tag tables are removed
                self.classes[name + "_initial"].__table__.drop()
                self.classes[name + "_initial"] = None
                self.classes[name + "_current"].__table__.drop()
                self.classes[name + "_current"] = None

            else:
                # The tag has a simple type, the tag column is removed from both current and initial tables

                # Tag column removed from initial table
                columns = ""
                columns_list = []
                sql_table_create = CreateTable(self.classes["initial"].__table__)
                for column in sql_table_create.columns:
                    if name.replace(" ", "") in str(column):
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
        Gives the Tag object of a tag
        :param name: Tag name
        :return: The tag table object if the tag exists, None otherwise
        """

        session = self.session_maker()
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        session.close()
        if len(tags) is 1:
            return tags[0]
        return None

    def get_tag_type(self, name):
        """
        Gives the tag type if the tag exists, None otherwise
        :param name: Tag name
        :return: The tag type: In [string, integer, float, date, datetime, time, list_string, list_integer, list_float, list_date, list_datetime, list_time]
        """

        session = self.session_maker()
        tags = session.query(self.classes["tag"]).filter(self.classes["tag"].name == name).all()
        session.close()
        if len(tags) is 1:
            return tags[0].type
        return None

    def is_tag_list(self, tag):
        """
        To know if the given tag is a list
        :param tag: tag name
        :return: True if the tag is a list, False otherwise
        """

        tag_type = self.get_tag_type(tag)
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

    """ VALUES """

    def get_current_value(self, scan, tag):
        """
        Gives the current value of <scan, tag>
        :param scan: Scan name
        :param tag: Tag name
        :return: The current value of <scan, tag>
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None:

            if self.is_tag_list(tag):

                # The tag has a type list, the values are gotten from the tag current table
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

                # The tag has a simple type, the value is gotten from current table
                session = self.session_maker()
                values = session.query(self.classes["current"]).filter(
                    self.classes["current"].index == self.get_scan_index(scan)).all()
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
                # The tag has a type list, the values are gotten from the tag initial table
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

                # The tag has a simple type, the value is gotten from initial table
                session = self.session_maker()
                values = session.query(self.classes["initial"]).filter(
                    self.classes["initial"].index == self.get_scan_index(scan)).all()
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
        if self.get_tag(tag) is not None and self.check_type_value(new_value,
                                                                   self.get_tag_type(tag)) and self.get_scan_index(
                scan) is not None:

            if self.is_tag_list(tag):

                # The scan has a list type, the values are reset in the tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                for index in range(0, len(values)):
                    value_to_modify = values[index]
                    value_to_modify.value = new_value[index]
                session.commit()

            else:

                # The scan has a simple type, the values are reset in the tag column in current table
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

                # The scan has a list type, the values are reset in the tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                for index in range(0, len(values)):
                    value_to_modify = values[index]
                    value_to_modify.value = self.get_initial_value(scan, tag)[index]
                session.commit()

            else:

                # The scan has a simple type, the value is reset in the current table
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

                # The tag has a list type, the values are removed from tag current table
                session = self.session_maker()
                values = session.query(self.classes[tag + "_current"]).filter(
                    self.classes[tag + "_current"].index == self.get_scan_index(scan)).all()
                for value in values:
                    session.delete(value)
                session.commit()

            else:

                # The tag has a simple type, the value is removed from the tag column in the current table
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
        :param type: type that the value is supposed to have
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

    def add_value(self, scan, tag, value):
        """
        Adds a value for <scan, tag> (as initial and current)
        :param scan: scan name
        :param tag: tag name
        :param value: value
        """

        # Checking that the tag exists
        if self.get_tag(tag) is not None and self.check_type_value(value,
                                                                   self.get_tag_type(tag)) and self.get_scan_index(
                scan) is not None:

            if self.is_tag_list(tag):

                # The tag has a list type, it is added in the tag tables
                session = self.session_maker()
                for order in range(0, len(value)):
                    element = value[order]
                    current_to_add = self.classes[tag + "_current"](index=self.get_scan_index(scan), order=order,
                                                                    value=element)
                    initial_to_add = self.classes[tag + "_initial"](index=self.get_scan_index(scan), order=order,
                                                                    value=element)
                    session.add(current_to_add)
                    session.add(initial_to_add)
                session.commit()
            else:

                # The tag has a simple type, it is add it in both current and initial tables
                session = self.session_maker()
                scans_initial = session.query(self.classes["initial"]).filter(
                    self.classes["initial"].index == self.get_scan_index(scan)).all()
                scans_current = session.query(self.classes["current"]).filter(
                    self.classes["current"].index == self.get_scan_index(scan)).all()
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
        Gives the path table object of a scan
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

            # Thanks to the foreign key and on delete cascade, the scan is also removed from all other tables

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
        Overrides the instance closing to remove the temporary folder and temporary database file
        """

        shutil.rmtree(self.temp_folder)
