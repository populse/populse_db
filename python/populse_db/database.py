import os
import shutil
import tempfile
from datetime import date, time, datetime

from sqlalchemy import (create_engine, Column, String, Integer, Float,
                        MetaData, Date, DateTime, Time, Table,
                        ForeignKeyConstraint)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateTable
from sqlalchemy.engine import Engine
from sqlalchemy import event

from populse_db.database_model import (create_database, TAG_TYPE_INTEGER,
                                       TAG_TYPE_FLOAT, TAG_TYPE_TIME,
                                       TAG_TYPE_DATETIME, TAG_TYPE_DATE,
                                       TAG_TYPE_STRING, TAG_TYPE_LIST_DATE,
                                       TAG_TYPE_LIST_DATETIME,
                                       TAG_TYPE_LIST_FLOAT,
                                       TAG_TYPE_LIST_INTEGER,
                                       TAG_TYPE_LIST_STRING,
                                       TAG_TYPE_LIST_TIME, TAG_UNIT_MS,
                                       TAG_UNIT_MM, TAG_UNIT_HZPIXEL,
                                       TAG_UNIT_DEGREE, TAG_UNIT_MHZ,
                                       TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """
    Manages the pragmas during the database opening
    :param dbapi_connection:
    :param connection_record:
    """
    dbapi_connection.execute('pragma case_sensitive_like=ON')
    dbapi_connection.execute('pragma foreign_keys=ON')

class Database:

    """
    Database API

    attributes:
        - path: path of the database file
        - classes: list of table classes, generated automatically
        - base: database base
        - temp_folder: temporary folder containing the temporary
          database file
        - temp_file: temporary database file that will be kept updated
        - engine: database engine
        - metadata: database metadata
        - session_maker: session manager
        - unsaved_modifications: to know if there are unsaved
          modifications in the database

    methods:
        - add_tag: adds a tag
        - tag_type_to_column_type: gives the column type corresponding
          to a tag type
        - tag_name_to_column_name: gives the column tag name corresponding
          to the original tag name
        - remove_tag: removes a tag
        - get_tag: Gives the tag table object of a tag
        - get_tag_type: gives the tag type
        - get_tags_names: gives all tag names
        - get_tags: gives all tag table objects
        - reset_all_visibilities: puts all tag visibilities to False
        - get_visualized_tags: gives the visualized tags
        - set_tag_origin: sets the tag origin
        - set_tag_unit: sets the tag unit
        - set_tag_type: sets the tag type
        - set_tag_description: sets the tag description
        - set_tag_visibility: sets the tag visibility
        - get_current_value: gives the current value of <path, tag>
        - get_initial_value: gives the initial value of <path, tag>
        - is_value_modified: to know if a value has been modified
        - set_value: sets the value of <path, tag>
        - reset_value: resets the value of <path, tag>
        - remove_value: removes the value of <path, tag>
        - check_type_value: checks the type of a value
        - is_tag_list: to know if a tag has a list type
        - add_value: adds a value to <path, tag>
        - get_path: gives the path table object of a path
        - get_paths: gives all path table objects
        - get_paths_names: gives all path names
        - add_path: adds a path
        - remove_path: removes a path
        - get_paths_matching_constraints: gives the paths matching the
          constraints given in parameter
        - get_paths_matching_search: gives the paths matching the
          search
        - get_paths_matching_advanced_search: gives the paths matching
          the advanced search
        - get_paths_matching_tag_value_couples: gives the paths
          containing all <tag, value> given in parameter
        - save_modifications: saves the pending modifications to the
          original database file
        - has_unsaved_modifications: to know if there are unsaved
          modifications
        - tables_redefinition: redefines the model after schema update
    """

    def __init__(self, path):
        """
        Creates an API of the database instance
        :param path: Path of the database file, can be already
        existing, or not
        """

        self.path = path
        self.table_classes = {}

        # Creation the database file if it does not already exist
        if not os.path.exists(self.path):
            if not os.path.exists(os.path.dirname(self.path)):
                os.makedirs(os.path.dirname(self.path))
            create_database(self.path)

        # Temporary database file created that will be kept updated
        self.temp_folder = os.path.relpath(tempfile.mkdtemp())
        self.temp_file = os.path.join(self.temp_folder, "temp_database.db")
        shutil.copy(self.path, self.temp_file)

        # Database opened (temporary database file being a copy of the
        # original database file)
        self.engine = create_engine('sqlite:///' + self.temp_file)

        # Metadata generated
        self.update_table_classes()

        # Database schema checked
        if ("path" not in self.table_classes.keys() or
            "current" not in self.table_classes.keys() or
                "initial" not in self.table_classes.keys()):
            raise ValueError(
                'The database schema is not coherent with the API.')

        self.session_maker = sessionmaker(bind=self.engine)

        self.unsaved_modifications = False

    """ TAGS """

    def add_tag(self, name, visible, origin, tag_type, unit, default_value,
                description):
        """
        Adds a tag to the database, if it does not already exist
        :param name: Tag name (str)
        :param visible: Tag visibility (True or False)
        :param origin: Tag origin (Raw or user)
        :param type: Tag type (string, int, float, date, datetime,
                     time, list_string, list_int, list_float, list_date,
                     list_datetime, or list_time)
        :param unit: Tag unit (ms, mm, degree, Hz/pixel, MHz, or None)
        :param default_value: Tag default value (str or None)
        :param description: Tag description (str or None)
        """

        # Parameters checked
        if not isinstance(name, str):
            return
        if not isinstance(visible, bool):
            return
        if origin not in [TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN]:
            return
        if tag_type not in [TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_STRING,
                            TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_DATE,
                            TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME,
                            TAG_TYPE_DATETIME, TAG_TYPE_DATE,
                            TAG_TYPE_STRING, TAG_TYPE_INTEGER, TAG_TYPE_FLOAT,
                            TAG_TYPE_TIME]:
            return
        if unit not in [TAG_UNIT_MHZ, TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL,
                        TAG_UNIT_MM, TAG_UNIT_MS] and unit is not None:
            return
        if not isinstance(default_value, str) and default_value is not None:
            return
        if not isinstance(description, str) and description is not None:
            return
        if self.get_tag(name) is not None:
            return

        # Adding the tag in the tag table
        session = self.session_maker()
        tag = self.table_classes["tag"](name=name, visible=visible, origin=origin,
                                        type=tag_type, unit=unit,
                                        default_value=default_value,
                                        description=description)
        session.add(tag)

        if tag_type in [TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_STRING,
                        TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_TIME,
                        TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME]:
            # The tag has a list type: new tag tables added

            # Tag tables initial and current definition
            tag_table_current = Table(name + "_current", self.metadata,
                                      Column("index", Integer,
                                             primary_key=True),
                                      Column("order", Integer,
                                             primary_key=True),
                                      Column("value",
                                             self.tag_type_to_column_type(
                                                 tag_type),
                                             nullable=False),
                                      ForeignKeyConstraint(["index"],
                                                           ["path.index"],
                                                           ondelete="CASCADE",
                                                           onupdate="CASCADE"))
            tag_table_initial = Table(name + "_initial", self.metadata,
                                      Column("index", Integer,
                                             primary_key=True),
                                      Column("order", Integer,
                                             primary_key=True),
                                      Column("value",
                                             self.tag_type_to_column_type(
                                                 tag_type),
                                             nullable=False),
                                      ForeignKeyConstraint(["index"],
                                                           ["path.index"],
                                                           ondelete="CASCADE",
                                                           onupdate="CASCADE"))

            # Both tables added
            tag_table_current.create(self.engine)
            tag_table_initial.create(self.engine)

        else:
            # The tag has a simple type: new column added to both initial
            # and current tables

            # Column creation
            column = Column(name, self.tag_type_to_column_type(tag_type))
            column_type = column.type.compile(self.engine.dialect)

            # Tag column added to both initial and current tables
            session.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    "initial", self.tag_name_to_column_name(name),
                    column_type))
            session.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    "current", self.tag_name_to_column_name(name),
                    column_type))

        session.commit()

        self.unsaved_modifications = True

        # Redefinition of the table classes
        self.update_table_classes()

    def tag_type_to_column_type(self, tag_type):
        """
        Gives the column type corresponding to the tag type
        :param tag_type: Tag type
        :return: The column type given the tag type
        """

        # Parameter checked
        if tag_type not in [TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_STRING,
                            TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_DATE,
                            TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME,
                            TAG_TYPE_DATETIME, TAG_TYPE_DATE,
                            TAG_TYPE_STRING, TAG_TYPE_INTEGER,
                            TAG_TYPE_FLOAT, TAG_TYPE_TIME]:
            return None

        if tag_type == TAG_TYPE_INTEGER or tag_type == TAG_TYPE_LIST_INTEGER:
            return Integer
        elif tag_type == TAG_TYPE_FLOAT or tag_type == TAG_TYPE_LIST_FLOAT:
            return Float
        elif tag_type == TAG_TYPE_DATE or tag_type == TAG_TYPE_LIST_DATE:
            return Date
        elif (tag_type == TAG_TYPE_DATETIME or
              tag_type == TAG_TYPE_LIST_DATETIME):
            return DateTime
        elif tag_type == TAG_TYPE_TIME or tag_type == TAG_TYPE_LIST_TIME:
            return Time
        elif tag_type == TAG_TYPE_STRING or tag_type == TAG_TYPE_LIST_STRING:
            return String

    def tag_name_to_column_name(self, tag):
        """
        Transforms the tag name into a valid column name
        :return: Valid column name
        """

        return tag.replace(" ", "").replace("(", "").replace(")", "").replace(
            ",", "").replace(".", "").replace("/", "")
        # TODO remove numbers if it causes problems (beware that it's not
        #     the same way to do it in python2 and python3)
        # TODO check that it does not conflict with another close tag that
        #     would have the same column name

    def remove_tag(self, name):
        """
        Removes a tag
        :param name: Tag name
        """

        # Parameters checked
        if not isinstance(name, str):
            return
        if self.get_tag(name) is None:
            return

        is_tag_list = self.is_tag_list(name)
        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()

        if len(tags) is 1:

            # The tag exists, it is removed from the tag table
            session.delete(tags[0])
            session.commit()

            if is_tag_list:
                # The tag has a list type, both tag tables are removed

                self.table_classes[name + "_initial"].__table__.drop(self.engine)
                self.table_classes[name + "_current"].__table__.drop(self.engine)

                # Redefinition of the table classes
                self.update_table_classes()

            else:
                # The tag has a simple type, the tag column is removed from
                # both current and initial tables

                # Tag column removed from initial table
                columns = ""
                sql_table_create = CreateTable(
                    self.table_classes["initial"].__table__)
                for column in sql_table_create.columns:
                    if self.tag_name_to_column_name(name) in str(column):
                        column_to_remove = column
                    else:
                        columns += str(column).split(" ")[0] + ", "
                sql_table_create.columns.remove(column_to_remove)
                sql_query = str(sql_table_create)
                sql_query = sql_query[:21] + '_backup' + sql_query[21:]
                columns = columns[:-2]
                columns = columns.replace("index", "\"index\"")
                sql_query = sql_query.replace("index", "\"index\"")
                session.execute(sql_query)
                session.execute("INSERT INTO initial_backup SELECT " +
                                columns + " FROM initial")
                session.execute("DROP TABLE initial")
                sql_query = sql_query[:21] + sql_query[29:]
                session.execute(sql_query)
                session.execute("INSERT INTO initial SELECT " + columns +
                                " FROM initial_backup")
                session.execute("DROP TABLE initial_backup")

                # Tag column removed from current table
                columns = ""
                sql_table_create = CreateTable(
                    self.table_classes["current"].__table__)
                for column in sql_table_create.columns:
                    if self.tag_name_to_column_name(name) in str(column):
                        column_to_remove = column
                    else:
                        columns += str(column).split(" ")[0] + ", "
                sql_table_create.columns.remove(column_to_remove)
                sql_query = str(sql_table_create)
                sql_query = sql_query[:21] + '_backup' + sql_query[21:]
                columns = columns[:-2]
                columns = columns.replace("index", "\"index\"")
                sql_query = sql_query.replace("index", "\"index\"")
                session.execute(sql_query)
                session.execute("INSERT INTO current_backup SELECT " +
                                columns + " FROM current")
                session.execute("DROP TABLE current")
                sql_query = sql_query[:21] + sql_query[29:]
                session.execute(sql_query)
                session.execute("INSERT INTO current SELECT " + columns +
                                " FROM current_backup")
                session.execute("DROP TABLE current_backup")

                session.commit()
                self.update_table_classes()
                self.unsaved_modifications = True
        else:
            session.close()

    def get_tag(self, name):
        """
        Gives the tag table object of a tag
        :param name: Tag name
        :return: The tag table object if the tag exists, None otherwise
        """

        # Parameters checked
        if not isinstance(name, str):
            return None

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()
        session.close()
        if len(tags) is 1:
            return tags[0]
        return None

    def get_tags_names(self):
        """
        Gives the list of tags
        :return: List of tag names
        """

        tags_list = []
        session = self.session_maker()
        tags = session.query(self.table_classes["tag"].name).all()
        session.close()
        for tag in tags:
            tags_list.append(tag.name)
        return tags_list

    def get_tags(self):
        """
        Gives the list of tag table objects
        :return: List of tag table objects
        """

        tags_list = []
        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).all()
        session.close()
        for tag in tags:
            tags_list.append(tag)
        return tags_list

    def reset_tag_visibilities(self):
        """
        Resets all tags visibility to False
        """

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).all()
        for tag in tags:
            tag.visible = False
        session.commit()
        self.unsaved_modifications = True

    def get_visualized_tags(self):
        """
        Gives the list of visualized tags
        :return: List of visualized tags
        """

        tags_list = []
        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].visible == True).all()
        session.close()
        for tag in tags:
            tags_list.append(tag)
        return tags_list

    def set_tag_origin(self, name, origin):
        """
        Sets the tag origin
        :param name: Tag name
        :param origin: Tag origin (raw or user)
        """

        # Parameters checked
        if not isinstance(name, str):
            return
        if self.get_tag(name) is None:
            return
        if origin not in [TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN]:
            return

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()
        if len(tags) is 1:
            tag = tags[0]
            tag.origin = origin
            session.commit()
            self.unsaved_modifications = True
        else:
            session.close()

    def set_tag_type(self, name, tag_type):
        """
        Sets the tag type
        :param name: Tag name
        :param origin: Tag type (string, integer, float, date, datetime,
                       time, list_string, list_integer, list_float,
                       list_date, list_datetime, or list_time)
        """

        # Parameters checked
        if not isinstance(name, str):
            return
        if self.get_tag(name) is None:
            return
        if tag_type not in [TAG_TYPE_LIST_FLOAT, TAG_TYPE_LIST_STRING,
                            TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_DATE,
                            TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_TIME,
                            TAG_TYPE_DATETIME, TAG_TYPE_DATE,
                            TAG_TYPE_STRING, TAG_TYPE_INTEGER,
                            TAG_TYPE_FLOAT, TAG_TYPE_TIME]:
            return
        if self.get_tag(name).type == tag_type:
            return

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()
        if len(tags) is 1:
            tag = tags[0]
            tag.type = tag_type

            # Preparing column type
            column = Column("", self.tag_type_to_column_type(tag_type))
            column_type = column.type.compile(self.engine.dialect)

            # Column type set
            if self.is_tag_list(name):
                # The tag has a list type, both tag tables are updated

                # Initial tag table value column updated
                columns = ""
                sql_table_create = CreateTable(
                    self.table_classes[name + "_initial"].__table__)
                for column in sql_table_create.columns:
                    columns += str(column).split(" ")[0] + ", "
                sql_query = str(sql_table_create)
                temp_query = sql_query.split("\n\t")
                new_query = ""
                for row in temp_query:
                    words = row.split(" ")
                    if words[0] == "value":
                        words[1] = column_type + " NOT NULL,"
                        words = [words[0], words[1]]
                    new_query += ' '.join(words) + "\n\t"
                new_query = new_query[:-1]
                sql_query = new_query
                columns = columns[:-2]
                columns = columns.replace("index", "\"index\"")
                sql_query = sql_query.replace("index", "\"index\"")
                sql_query = sql_query.replace("initial", "initial_backup")
                session.execute(sql_query)
                session.execute("INSERT INTO \"" + name +
                                "_initial_backup\" SELECT " + columns +
                                " FROM \"" + name + "_initial\"")
                session.execute("DROP TABLE \"" + name + "_initial\"")
                sql_query = sql_query.replace("initial_backup", "initial")
                session.execute(sql_query)
                session.execute("INSERT INTO \"" + name + "_initial\" SELECT "
                                + columns + " FROM \"" + name +
                                "_initial_backup\"")
                session.execute("DROP TABLE \"" + name + "_initial_backup\"")

                # Current tag table value column updated
                columns = ""
                sql_table_create = CreateTable(
                    self.table_classes[name + "_current"].__table__)
                for column in sql_table_create.columns:
                    columns += str(column).split(" ")[0] + ", "
                sql_query = str(sql_table_create)
                temp_query = sql_query.split("\n\t")
                new_query = ""
                for row in temp_query:
                    words = row.split(" ")
                    if words[0] == "value":
                        words[1] = column_type + " NOT NULL,"
                        words = [words[0], words[1]]
                    new_query += ' '.join(words) + "\n\t"
                new_query = new_query[:-1]
                sql_query = new_query
                columns = columns[:-2]
                columns = columns.replace("index", "\"index\"")
                sql_query = sql_query.replace("index", "\"index\"")
                sql_query = sql_query.replace("current", "current_backup")
                session.execute(sql_query)
                session.execute(
                    "INSERT INTO \"" + name + "_current_backup\" SELECT " +
                    columns + " FROM \"" + name + "_current\"")
                session.execute("DROP TABLE \"" + name + "_current\"")
                sql_query = sql_query.replace("current_backup", "current")
                session.execute(sql_query)
                session.execute(
                    "INSERT INTO \"" + name + "_current\" SELECT " + columns +
                    " FROM \"" + name + "_current_backup\"")
                session.execute("DROP TABLE \"" + name + "_current_backup\"")

            else:
                # The tag has a simple type, both current and initial tables
                # are updated

                # Tag column updated from initial table
                columns = ""
                sql_table_create = CreateTable(
                    self.table_classes["initial"].__table__)
                for column in sql_table_create.columns:
                    columns += str(column).split(" ")[0] + ", "
                sql_query = str(sql_table_create)
                temp_query = sql_query.split("\n\t")
                new_query = ""
                for row in temp_query:
                    words = row.split(" ")
                    if words[0] == "\"" + name + "\"":
                        words[1] = column_type + ","
                    new_query += ' '.join(words) + "\n\t"
                new_query = new_query[:-1]
                sql_query = new_query
                sql_query = sql_query[:21] + '_backup' + sql_query[21:]
                columns = columns[:-2]
                columns = columns.replace("index", "\"index\"")
                sql_query = sql_query.replace("index", "\"index\"")
                session.execute(sql_query)
                session.execute("INSERT INTO initial_backup SELECT " +
                                columns + " FROM initial")
                session.execute("DROP TABLE initial")
                sql_query = sql_query[:21] + sql_query[29:]
                session.execute(sql_query)
                session.execute("INSERT INTO initial SELECT " + columns +
                                " FROM initial_backup")
                session.execute("DROP TABLE initial_backup")

                # Tag column updated from current table
                columns = ""
                sql_table_create = CreateTable(
                    self.table_classes["current"].__table__)
                for column in sql_table_create.columns:
                    columns += str(column).split(" ")[0] + ", "
                sql_query = str(sql_table_create)
                temp_query = sql_query.split("\n\t")
                new_query = ""
                for row in temp_query:
                    words = row.split(" ")
                    if words[0] == "\"" + name + "\"":
                        words[1] = column_type + ","
                    new_query += ' '.join(words) + "\n\t"
                new_query = new_query[:-2]
                sql_query = new_query
                sql_query = sql_query[:21] + '_backup' + sql_query[21:]
                columns = columns[:-2]
                columns = columns.replace("index", "\"index\"")
                sql_query = sql_query.replace("index", "\"index\"")
                session.execute(sql_query)
                session.execute("INSERT INTO current_backup SELECT " + columns
                                + " FROM current")
                session.execute("DROP TABLE current")
                sql_query = sql_query[:21] + sql_query[29:]
                session.execute(sql_query)
                session.execute("INSERT INTO current SELECT " + columns +
                                " FROM current_backup")
                session.execute("DROP TABLE current_backup")

            session.commit()
            self.update_table_classes()
            self.unsaved_modifications = True
        else:
            session.close()

    def set_tag_unit(self, name, unit):
        """
        Sets the tag unit
        :param name: Tag name
        :param origin: Tag unit (ms, mm, degree, Hz/pixel, MHz, or None)
        """

        # Parameters checked
        if not isinstance(name, str):
            return
        if self.get_tag(name) is None:
            return
        if unit not in [TAG_UNIT_MHZ, TAG_UNIT_DEGREE, TAG_UNIT_HZPIXEL,
                        TAG_UNIT_MM, TAG_UNIT_MS] and unit is not None:
            return
        if self.get_tag(name).unit == unit:
            return

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()
        if len(tags) is 1:
            tag = tags[0]
            tag.unit = unit
            session.commit()
            self.unsaved_modifications = True
        else:
            session.close()

    def set_tag_description(self, name, description):
        """
        Sets the tag description
        :param name: Tag name
        :param origin: Tag description (str)
        """

        # Parameters checked
        if not isinstance(name, str):
            return
        if self.get_tag(name) is None:
            return
        if not isinstance(description, str):
            return

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()
        if len(tags) is 1:
            tag = tags[0]
            tag.description = description
            session.commit()
            self.unsaved_modifications = True
        else:
            session.close()

    def set_tag_visibility(self, name, visible):
        """
        Sets the tag visibility
        :param name: Tag name
        :param visible: Tag new visibility (True or False)
        """

        # Parameters checked
        if not isinstance(name, str):
            return None
        if self.get_tag(name) is None:
            return
        if not isinstance(visible, bool):
            return

        session = self.session_maker()
        tags = session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()
        if len(tags) is 1:
            tag = tags[0]
            tag.visible = visible
            session.commit()
            self.unsaved_modifications = True
        else:
            session.close()

    def is_tag_list(self, tag):
        """
        To know if the given tag is a list
        :param tag: tag name
        :return: True if the tag is a list, False otherwise
        """

        # Parameters checked
        if not isinstance(tag, str):
            return False
        if self.get_tag(tag) is None:
            return False

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

    """ VALUES """

    def list_value_to_typed_value(self, value, tag_type):
        """
        Converts the subvalue of a list into a typed value
        :param value: List subvalue from the database
        :param tag_type: Value type
        :return: Typed subvalue
        """

        # Parameters checked
        if tag_type not in [TAG_TYPE_LIST_DATETIME, TAG_TYPE_LIST_DATE,
                            TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_STRING,
                            TAG_TYPE_LIST_TIME, TAG_TYPE_LIST_FLOAT]:
            return None

        if tag_type == TAG_TYPE_LIST_INTEGER:
            return int(value)
        elif tag_type == TAG_TYPE_LIST_STRING:
            return str(value)
        elif tag_type == TAG_TYPE_LIST_FLOAT:
            return float(value)
        elif tag_type == TAG_TYPE_LIST_DATETIME:
            return value
        elif tag_type == TAG_TYPE_LIST_DATE:
            return value
        elif tag_type == TAG_TYPE_LIST_TIME:
            return value

    def get_current_value(self, path, tag):
        """
        Gives the current value of <path, tag>
        :param path: path name
        :param tag: Tag name
        :return: The current value of <path, tag>
        """

        # Parameters checked
        if not isinstance(tag, str):
            return None
        if self.get_tag(tag) is None:
            return None
        if not isinstance(path, str):
            return None
        if self.get_path(path) is None:
            return None

        if self.is_tag_list(tag):
            # The tag has a type list, the values are gotten from the tag
            # current table

            session = self.session_maker()
            values = session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            session.close()
            if len(values) is 0:
                return None
            values_list = []
            for value in values:
                value_to_add = value.value
                tag_type = self.get_tag(tag).type
                value_to_add = self.list_value_to_typed_value(value_to_add,
                                                              tag_type)
                values_list.insert(value.order, value_to_add)
            return values_list

        else:
            # The tag has a simple type, the value is gotten from current
            # table

            session = self.session_maker()
            values = session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            session.close()
            if len(values) is 1:
                value = values[0]
                return getattr(value, self.tag_name_to_column_name(tag))
        return None

    def get_initial_value(self, path, tag):
        """
        Gives the initial value of <path, tag>
        :param path: path name
        :param tag: Tag name
        :return: The initial value of <path, tag>
        """

        # Parameters checked
        if not isinstance(tag, str):
            return None
        if self.get_tag(tag) is None:
            return None
        if not isinstance(path, str):
            return None
        if self.get_path(path) is None:
            return None

        if self.is_tag_list(tag):
            # The tag has a type list, the values are gotten from the tag
            # initial table

            session = self.session_maker()
            values = session.query(self.table_classes[tag + "_initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            session.close()
            if len(values) is 0:
                return None
            values_list = []
            for value in values:
                value_to_add = value.value
                tag_type = self.get_tag(tag).type
                value_to_add = self.list_value_to_typed_value(value_to_add,
                                                              tag_type)
                values_list.insert(value.order, value_to_add)
            return values_list

        else:
            # The tag has a simple type, the value is gotten from initial table

            session = self.session_maker()
            values = session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            session.close()
            if len(values) is 1:
                value = values[0]
                return getattr(value, self.tag_name_to_column_name(tag))
        return None

    def is_value_modified(self, path, tag):
        """
        To know if a value has been modified
        :param path: path name
        :param tag: tag name
        :return: True if the value has been modified, False otherwise
        """

        # Parameters checked
        if not isinstance(tag, str):
            return False
        if self.get_tag(tag) is None:
            return False
        if not isinstance(path, str):
            return False
        if self.get_path(path) is None:
            return False

        return (self.get_current_value(path, tag) !=
                self.get_initial_value(path, tag))

    def set_current_value(self, path, tag, new_value):
        """
        Sets the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        :param new_value: New value
        """

        # Parameters checked
        if not isinstance(tag, str):
            return
        if self.get_tag(tag) is None:
            return
        if not isinstance(path, str):
            return
        if self.get_path(path) is None:
            return
        if not self.check_type_value(new_value, self.get_tag(tag).type):
            return

        if self.is_tag_list(tag):
            # The path has a list type, the values are reset in the tag
            # current table

            session = self.session_maker()
            values = session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for index in range(0, len(values)):
                value_to_modify = values[index]
                value_to_modify.value = new_value[index]
            session.commit()
            self.unsaved_modifications = True

        else:
            # The path has a simple type, the values are reset in the tag
            # column in current table

            session = self.session_maker()
            values = session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, self.tag_name_to_column_name(tag), new_value)
            session.commit()
            self.unsaved_modifications = True

    def reset_current_value(self, path, tag):
        """
        Resets the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        :return True if the value has been reset, False otherwise
        """

        # Parameters checked
        if not isinstance(tag, str):
            return False
        if self.get_tag(tag) is None:
            return False
        if not isinstance(path, str):
            return False
        if self.get_path(path) is None:
            return False

        if self.is_tag_list(tag):
            # The path has a list type, the values are reset in the tag
            # current table

            session = self.session_maker()
            values = session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for index in range(0, len(values)):
                value_to_modify = values[index]
                value_to_modify.value = self.get_initial_value(path,
                                                               tag)[index]
            session.commit()
            self.unsaved_modifications = True

        else:
            # The path has a simple type, the value is reset in the current
            # table

            session = self.session_maker()
            values = session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, self.tag_name_to_column_name(tag),
                        self.get_initial_value(path, tag))
                session.commit()
                self.unsaved_modifications = True
            else:
                session.close()
                return False

        return True

    def remove_value(self, path, tag):
        """
        Removes the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        """

        # Parameters checked
        if not isinstance(tag, str):
            return
        if self.get_tag(tag) is None:
            return
        if not isinstance(path, str):
            return
        if self.get_path(path) is None:
            return

        if self.is_tag_list(tag):
            # The tag has a list type, the values are removed from both tag
            # current and initial tables

            # Tag current table
            session = self.session_maker()
            values = session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for value in values:
                session.delete(value)
            session.commit()

            # Tag initial table
            session = self.session_maker()
            values = session.query(self.table_classes[tag + "_initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for value in values:
                session.delete(value)
            session.commit()
            self.unsaved_modifications = True

        else:
            # The tag has a simple type, the value is removed from both
            # current and initial tables tag columns

            # Current table
            session = self.session_maker()
            values = session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, self.tag_name_to_column_name(tag), None)
            session.commit()

            # Initial table
            session = self.session_maker()
            values = session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, self.tag_name_to_column_name(tag), None)
            session.commit()
            self.unsaved_modifications = True

    def check_type_value(self, value, valid_type):
        """
        Checks the type of the value
        :param value: value
        :param type: type that the value is supposed to have
        :return: True if the value is valid, False otherwise
        """

        value_type = type(value)
        if valid_type is None:
            return False
        if value is None:
            return True
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
        if (valid_type in [TAG_TYPE_LIST_DATE, TAG_TYPE_LIST_DATETIME,
                           TAG_TYPE_LIST_TIME, TAG_TYPE_LIST_STRING,
                           TAG_TYPE_LIST_INTEGER, TAG_TYPE_LIST_FLOAT]
                and value_type == list):
            for value_element in value:
                if not self.check_type_value(value_element,
                                             valid_type.replace("list_", "")):
                    return False
            return True
        return False

    def new_value(self, path, tag, current_value, initial_value):
        """
        Adds a value for <path, tag> (as initial and current)
        :param path: path name
        :param tag: tag name
        :param current_value: current value
        :param initial_value: initial value
        """

        # Parameters checked
        if not isinstance(tag, str):
            return
        if self.get_tag(tag) is None:
            return
        if not isinstance(path, str):
            return
        if self.get_path(path) is None:
            return
        if not self.check_type_value(current_value, self.get_tag(tag).type):
            return
        if not self.check_type_value(initial_value, self.get_tag(tag).type):
            return

        if self.is_tag_list(tag):
            # The tag has a list type, it is added in the tag tables

            session = self.session_maker()

            # Initial value
            if initial_value is not None:

                for order in range(0, len(initial_value)):
                    element = initial_value[order]
                    initial_to_add = self.table_classes[tag + "_initial"](
                        index=self.get_path(path).index, order=order,
                        value=element)
                    session.add(initial_to_add)

            # Current value
            if current_value is not None:
                for order in range(0, len(current_value)):
                    element = current_value[order]
                    current_to_add = self.table_classes[tag + "_current"](
                        index=self.get_path(path).index, order=order,
                        value=element)
                    session.add(current_to_add)

            session.commit()
            self.unsaved_modifications = True
        else:
            # The tag has a simple type, it is add it in both current and
            # initial tables

            session = self.session_maker()
            paths_initial = session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            paths_current = session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(paths_initial) is 1 and len(paths_current) is 1:
                path_initial = paths_initial[0]
                path_current = paths_current[0]
                database_current_value = getattr(
                    path_current, self.tag_name_to_column_name(tag))
                database_initial_value = getattr(
                    path_initial, self.tag_name_to_column_name(tag))

                # We add the value only if it does not already exist
                if (database_current_value is None and
                        database_initial_value is None):
                    if initial_value is not None:
                        setattr(
                            path_initial, self.tag_name_to_column_name(tag),
                            initial_value)
                    if current_value is not None:
                        setattr(
                            path_current, self.tag_name_to_column_name(tag),
                            current_value)
                session.commit()
                self.unsaved_modifications = True
            else:
                session.close()

    """ PATHS """

    def get_path(self, path):
        """
        Gives the path table object of a path
        :param path: path name
        """

        if not isinstance(path, str):
            return None

        session = self.session_maker()
        paths = session.query(self.table_classes["path"]).filter(
            self.table_classes["path"].name == path).all()
        session.close()
        if len(paths) is 1:
            path = paths[0]
            return path
        return None

    def get_paths_names(self):
        """
        Gives the list of path names
        :param path: List of path names
        """

        paths_list = []
        session = self.session_maker()
        paths = session.query(self.table_classes["path"]).all()
        session.close()
        for path in paths:
            paths_list.append(path.name)
        return paths_list

    def get_paths(self):
        """
        Gives the list of path table objects
        :param path: List of path table objects
        """

        paths_list = []
        session = self.session_maker()
        paths = session.query(self.table_classes["path"]).all()
        session.close()
        for path in paths:
            paths_list.append(path)
        return paths_list

    def remove_path(self, path):
        """
        Removes a path
        :param path: path name
        """

        if not isinstance(path, str):
            return None
        if self.get_path(path) is None:
            return None

        session = self.session_maker()
        paths = session.query(self.table_classes["path"]).filter(
            self.table_classes["path"].name == path).all()
        if len(paths) is 1:
            path = paths[0]
            session.delete(path)
            session.commit()
            self.unsaved_modifications = True

            # Thanks to the foreign key and on delete cascade, the path is
            # also removed from all other tables

        else:
            session.close()

    def add_path(self, path, checksum):
        """
        Adds a path
        :param path: path path
        :param checksum: path checksum
        """

        # Adding the path in the Tag table
        session = self.session_maker()
        paths = session.query(self.table_classes["path"]).filter(
            self.table_classes["path"].name == path).all()
        if len(paths) is 0:
            path_to_add = self.table_classes["path"](name=path, checksum=checksum)
            session.add(path_to_add)
            session.commit()

            # Adding the index to both initial and current tables
            session = self.session_maker()
            initial = self.table_classes["initial"](index=self.get_path(path).index)
            current = self.table_classes["current"](index=self.get_path(path).index)
            session.add(current)
            session.add(initial)
            session.commit()
            self.unsaved_modifications = True
        else:
            session.close()

    """ UTILS """

    def get_paths_matching_search(self, search):
        """
        Returns the list of paths names matching the search
        :param search: search to match (str)
        :return: List of path names matching the search
        """

        if not isinstance(search, str):
            return None

        paths_matching = []

        # Itering over all values and finding matches
        session = self.session_maker()

        # Only the visible tags are taken into account
        for tag in self.get_visualized_tags():
            if not self.is_tag_list(tag.name):
                # The tag has a simple type, the tag column is used in the
                # current table

                values = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag.name)).like(
                                "%" + search + "%")).distinct().all()
                for value in values:
                    if value not in paths_matching:
                        paths_matching.append(value.name)
            else:
                # The tag has a list type, the tag current table is used

                for path in self.get_paths_names():
                    path_value = self.get_current_value(path, tag.name)
                    if (search in str(path_value) and
                            path not in paths_matching):
                        paths_matching.append(path)

        session.close()

        return paths_matching

    def get_paths_matching_constraints(self, tag, value, condition):
        """
        Gives the paths corresponding to the constraints
        :param tag: tag name
        :param value: value
        :param condition: condition
        :return: List of paths matching the constraints given in parameter
        """

        if not self.is_tag_list(tag):
            # The tag has a simple type, the tag column is used in the current
            # table

            session = self.session_maker()

            if (condition == "="):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)) ==
                    value).distinct().all()
            elif (condition == "!="):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    != value).distinct().all()
            elif (condition == ">="):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)) >=
                    value).distinct().all()
            elif (condition == "<="):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    <= value).distinct().all()
            elif (condition == ">"):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    > value).distinct().all()
            elif (condition == "<"):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    < value).distinct().all()
            elif (condition == "CONTAINS"):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).contains(
                        value)).distinct().all()
            elif (condition == "BETWEEN"):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).between(
                                value[0],
                                value[1])).distinct().all()
            elif (condition == "IN"):
                query = session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).in_(
                        value)).distinct().all()

            session.close()

            paths_list = []
            for path in query:
                if path.name not in paths_list:
                    paths_list.append(path.name)
            return paths_list

        else:
            # The tag has a list type, the tag current table is used

            paths_list = []
            for path in self.get_paths_names():
                current_value = self.get_current_value(path, tag)
                if condition == "=":
                    if str(current_value) == value:
                        paths_list.append(path)
                elif condition == "!=":
                    if str(current_value) != value:
                        paths_list.append(path)
                elif condition == ">=":
                    if str(current_value) >= value:
                        paths_list.append(path)
                elif condition == "<=":
                    if str(current_value) <= value:
                        paths_list.append(path)
                elif condition == ">":
                    if str(current_value) > value:
                        paths_list.append(path)
                elif condition == "<":
                    if str(current_value) < value:
                        paths_list.append(path)
                elif condition == "CONTAINS":
                    if value in str(current_value):
                        paths_list.append(path)
                elif condition == "IN":
                    if str(current_value) in value:
                        paths_list.append(path)
                elif condition == "BETWEEN":
                    if value[0] <= str(current_value) <= value[1]:
                        paths_list.append(path)
            return paths_list

    def get_paths_matching_advanced_search(self, links, fields, conditions,
                                           values, nots):
        """
        Gives the paths matching the advanced search
        :param links: Links (AND/OR)
        :param fields: Fields (tag name/All visualized tags)
        :param conditions: Conditions (=, !=, <, >, <=, >=, BETWEEN,
                           CONTAINS, IN)
        :param values: Values (str for =, !=, <, >, <=, >=, and
                       CONTAINS/list for BETWEEN and IN)
        :param nots: Nots (Empty or NOT)
        :return: List of path names matching all the constraints
        """

        if (not len(links) == len(fields) - 1 == len(conditions) - 1 ==
                len(values) - 1 == len(nots) - 1):
            return []
        for link in links:
            if link not in ["AND", "OR"]:
                return []
        fields_list = self.get_tags_names()
        fields_list.append("All visualized tags")
        for field in fields:
            if field not in fields_list:
                return []
        for condition in conditions:
            if condition not in ["=", "!=", "<", ">", "<=", ">=", "BETWEEN",
                                 "IN", "CONTAINS"]:
                return []
        for i in range(0, len(values)):
            value = values[i]
            if conditions[i] == "BETWEEN":
                if not isinstance(value, list) or len(value) != 2:
                    return []
            elif conditions[i] == "IN":
                if not isinstance(value, list):
                    return []
            else:
                if not isinstance(value, str):
                    return []
        for not_ in nots:
            if not_ not in ["", "NOT"]:
                return []

        queries = []  # list of paths of each query (row)
        for i in range(0, len(conditions)):
            queries.append([])
            if fields[i] != "All visualized tags":
                # Tag filter: Only those values are read

                queries[i] = self.get_paths_matching_constraints(fields[i],
                                                                 values[i],
                                                                 conditions[i])

            else:
                # No tag filter, all values are read

                for tag in self.get_visualized_tags():
                    queries[i] = list(set(queries[i]).union(set(
                        self.get_paths_matching_constraints(tag.name,
                                                            values[i],
                                                            conditions[i]))))

            # Putting negation if needed
            if (nots[i] == "NOT"):
                queries[i] = list(set(self.get_paths_names()) -
                                  set(queries[i]))

        # We start with the first row to put the link between the conditions
        # Links are made row by row, there is no priority like in SQL where AND
        # is stronger than OR
        result = queries[0]
        for i in range(0, len(links)):
            if (links[i] == "AND"):
                # If the link is AND, we do an intersection between the current
                # result and the next row
                result = list(set(result).intersection(set(queries[i + 1])))
            else:
                # If the link is OR, we do an union between the current result
                # and the next row
                result = list(set(result).union(set(queries[i + 1])))

        return result

    def get_paths_matching_tag_value_couples(self, tag_value_couples):
        """
        Checks if a path contains all the couples <tag, value> given in
        parameter
        :param tag_value_couples: List of couple <tag, value> to check
        :return: List of paths matching all the <tag, value> couples
        """

        couple_results = []
        for couple in tag_value_couples:
            tag = couple[0]
            value = couple[1]

            if not self.is_tag_list(tag):
                # The tag has a simple type, the tag column in the current
                # table is used

                session = self.session_maker()
                couple_query_result = session.query(
                    self.table_classes["path"].name).join(
                        self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)) == value)
                session.close()
                couple_result = []
                for query_result in couple_query_result:
                    couple_result.append(query_result.name)
            else:
                # The tag has a list type, the tag current table is used

                couple_result = []
                for path in self.get_paths_names():
                    path_value = self.get_current_value(path, tag)
                    if str(path_value) == value:
                        couple_result.append(path)

            couple_results.append(couple_result)

        # All the path lists are put together, with intersections
        # Only the paths with all <tag, value> are taken
        final_result = couple_results[0]
        for i in range(0, len(couple_results) - 1):
            final_result = list(set(final_result).intersection(
                set(couple_results[i + 1])))
        return final_result

    def save_modifications(self):
        """
        Saves the modifications by copying the updated temporary
        database into the original database file
        """

        shutil.copy(self.temp_file, self.path)
        self.unsaved_modifications = False

    def has_unsaved_modifications(self):
        """
        Knowing if the database has pending modifications that are
        unsaved
        :return: True if there are pending modifications to save,
                 False otherwise
        """

        return self.unsaved_modifications

    def update_table_classes(self):
        """
        Redefines the model after an update of the schema
        """

        self.table_classes.clear()
        self.base = automap_base()
        self.base.prepare(self.engine, reflect=True)
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect(bind=self.engine)
        for table in self.metadata.tables.values():
            table_name = table.name
            self.table_classes[table_name] = getattr(self.base.classes,
                                                     table_name)

    def __del__(self):
        """
        Overrides the instance closing to remove the temporary folder
        and temporary database file
        """

        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder)
