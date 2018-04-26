import os
from datetime import date, time, datetime

from sqlalchemy import (create_engine, Column, String, Integer, Float,
                        MetaData, Date, DateTime, Time, Table,
                        ForeignKeyConstraint)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.engine import Engine
from sqlalchemy import event

import time as time_exec

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
                                       TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN,
                                       LIST_TYPES, SIMPLE_TYPES, TYPE_TO_COLUMN)


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
        - save_modifications: saves the pending modifications
        - unsave_modifications: unsaves the pending modifications
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

        # Database opened
        self.engine = create_engine('sqlite:///' + self.path, connect_args={'check_same_thread': False})

        # Metadata generated
        self.update_table_classes()

        # Database schema checked
        if ("path" not in self.table_classes.keys() or
            "current" not in self.table_classes.keys() or
                "initial" not in self.table_classes.keys()):
            raise ValueError(
                'The database schema is not coherent with the API.')

        session_maker = sessionmaker(bind=self.engine)
        self.session = session_maker()

        self.unsaved_modifications = False

    """ TAGS """

    def add_tag(self, name, origin, tag_type, unit, default_value,
                description): # (0.05 sec on average)
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

        # Adding the tag in the tag table (0.003 sec on average)
        tag = self.table_classes["tag"](name=name, origin=origin,
                                        type=tag_type, unit=unit,
                                        default_value=default_value,
                                        description=description)

        self.session.add(tag)

        if tag_type in LIST_TYPES:
            # The tag has a list type: new tag tables added (0.04 sec on average)

            # Tag tables initial and current definition (0.00045 sec on average)
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

            # Both tables added (0.03 sec on average)
            current_query = CreateTable(tag_table_current)
            initial_query = CreateTable(tag_table_initial)

            # 0.03 seconds to execute those 2 queries
            self.session.execute(current_query)
            self.session.execute(initial_query)

            self.unsaved_modifications = True

            # Redefinition of the table classes
            self.update_table_classes()

        elif tag_type in SIMPLE_TYPES:
            # The tag has a simple type: new column added to both initial
            # and current tables (0.06 sec on average)

            # Column creation
            column = Column(name, self.tag_type_to_column_type(tag_type))
            column_type = column.type.compile(self.engine.dialect)

            # Tag column added to both initial and current tables (0.05 sec on average)
            self.session.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    "initial", self.tag_name_to_column_name(name),
                    column_type))
            self.session.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    "current", self.tag_name_to_column_name(name),
                    column_type))

            self.unsaved_modifications = True

            # Redefinition of the table classes
            self.update_table_classes()

    def add_tags(self, tags):
        """
        Add all the tags
        :param tags: List of tags to add (name, origin, tag_type, unit, default_value,
                description)
        """

        tag_rows = []

        for tag in tags:

            tag_name = tag[0]
            tag_type = tag[2]

            # Adding the tag in the tag table (0.003 sec on average)
            tag_row = self.table_classes["tag"](name=tag_name, origin=tag[1],
                                            type=tag_type, unit=tag[3],
                                            default_value=tag[4],
                                            description=tag[5])

            tag_rows.append(tag_row)

            if tag_type in LIST_TYPES:

                # Tag tables initial and current definition (0.00045 sec on average)
                tag_table_current = Table(tag_name + "_current", self.metadata,
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
                tag_table_initial = Table(tag_name + "_initial", self.metadata,
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

                # Both tables added (0.03 sec on average)
                current_query = CreateTable(tag_table_current)
                initial_query = CreateTable(tag_table_initial)

                # 0.03 seconds to execute those 2 queries
                self.session.execute(current_query)
                self.session.execute(initial_query)

            elif tag_type in SIMPLE_TYPES:

                # Columns creation
                column = Column(tag_name, self.tag_type_to_column_type(tag_type))
                column_type = column.type.compile(self.engine.dialect)
                tag_column_name = self.tag_name_to_column_name(tag_name)
                self.session.execute(
                    'ALTER TABLE %s ADD COLUMN %s %s;' % ("initial", tag_column_name, column_type))
                self.session.execute(
                    'ALTER TABLE %s ADD COLUMN %s %s;' % ("current", tag_column_name, column_type))

        self.session.add_all(tag_rows)

        self.unsaved_modifications = True

        # Redefinition of the table classes
        self.update_table_classes()

    def tag_type_to_column_type(self, tag_type):
        """
        Gives the column type corresponding to the tag type
        :param tag_type: Tag type
        :return: The column type given the tag type
        """

        return TYPE_TO_COLUMN[tag_type]

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

        is_tag_list = self.is_tag_list(name)
        tags = self.session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).all()

        if len(tags) is 1:

            # The tag exists, it is removed from the tag table
            self.session.delete(tags[0])

            if is_tag_list:
                # The tag has a list type, both tag tables are removed

                initial_query = DropTable(self.table_classes[name + "_initial"].__table__)
                self.session.execute(initial_query)
                current_query = DropTable(self.table_classes[name + "_current"].__table__)
                self.session.execute(current_query)

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
                self.session.execute(sql_query)
                self.session.execute("INSERT INTO initial_backup SELECT " +
                                columns + " FROM initial")
                self.session.execute("DROP TABLE initial")
                sql_query = sql_query[:21] + sql_query[29:]
                self.session.execute(sql_query)
                self.session.execute("INSERT INTO initial SELECT " + columns +
                                " FROM initial_backup")
                self.session.execute("DROP TABLE initial_backup")

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
                self.session.execute(sql_query)
                self.session.execute("INSERT INTO current_backup SELECT " +
                                columns + " FROM current")
                self.session.execute("DROP TABLE current")
                sql_query = sql_query[:21] + sql_query[29:]
                self.session.execute(sql_query)
                self.session.execute("INSERT INTO current SELECT " + columns +
                                " FROM current_backup")
                self.session.execute("DROP TABLE current_backup")

                self.update_table_classes()
                self.unsaved_modifications = True

    def get_tag(self, name):
        """
        Gives the tag table object of a tag
        :param name: Tag name
        :return: The tag table object if the tag exists, None otherwise
        """

        tag = self.session.query(self.table_classes["tag"]).filter(
            self.table_classes["tag"].name == name).first()
        return tag

    def get_tags_names(self):
        """
        Gives the list of tags
        :return: List of tag names
        """

        tags_list = []
        tags = self.session.query(self.table_classes["tag"].name).all()
        for tag in tags:
            tags_list.append(tag.name)
        return tags_list

    def get_tags(self):
        """
        Gives the list of tag table objects
        :return: List of tag table objects
        """

        tags_list = []
        tags = self.session.query(self.table_classes["tag"]).all()
        for tag in tags:
            tags_list.append(tag)
        return tags_list

    def is_tag_list(self, tag):
        """
        To know if the given tag is a list
        :param tag: tag name
        :return: True if the tag is a list, False otherwise
        """

        tag_type = self.get_tag(tag).type
        return tag_type in LIST_TYPES

    def is_tag_simple(self, tag):
        """
        To know if the given tag has a simple type
        :param tag: tag name
        :return: True if the tag has a simple type, False otherwise
        """

        tag_type = self.get_tag(tag).type
        return tag_type in SIMPLE_TYPES

    """ VALUES """

    def list_value_to_typed_value(self, value, tag_type):
        """
        Converts the subvalue of a list into a typed value
        :param value: List subvalue from the database
        :param tag_type: Value type
        :return: Typed subvalue
        """

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

        if self.is_tag_list(tag):
            # The tag has a type list, the values are gotten from the tag
            # current table

            values = self.session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
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

            values = self.session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
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

        if self.is_tag_list(tag):
            # The tag has a type list, the values are gotten from the tag
            # initial table

            values = self.session.query(self.table_classes[tag + "_initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
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

            values = self.session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
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

        return (self.get_current_value(path, tag) !=
                self.get_initial_value(path, tag))

    def set_current_value(self, path, tag, new_value):
        """
        Sets the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        :param new_value: New value
        """

        if self.is_tag_list(tag):
            # The path has a list type, the values are reset in the tag
            # current table

            values = self.session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for index in range(0, len(values)):
                value_to_modify = values[index]
                value_to_modify.value = new_value[index]
            self.unsaved_modifications = True

        else:
            # The path has a simple type, the values are reset in the tag
            # column in current table

            values = self.session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, self.tag_name_to_column_name(tag), new_value)
            self.unsaved_modifications = True

    def reset_current_value(self, path, tag):
        """
        Resets the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        :return True if the value has been reset, False otherwise
        """

        if self.is_tag_list(tag):
            # The path has a list type, the values are reset in the tag
            # current table

            values = self.session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for index in range(0, len(values)):
                value_to_modify = values[index]
                value_to_modify.value = self.get_initial_value(path,
                                                               tag)[index]
            self.unsaved_modifications = True

        else:
            # The path has a simple type, the value is reset in the current
            # table

            values = self.session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, self.tag_name_to_column_name(tag),
                        self.get_initial_value(path, tag))
                self.unsaved_modifications = True
            else:
                return False

        return True

    def remove_value(self, path, tag):
        """
        Removes the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        """

        if self.is_tag_list(tag):
            # The tag has a list type, the values are removed from both tag
            # current and initial tables

            # Tag current table
            values = self.session.query(self.table_classes[tag + "_current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for value in values:
                self.session.delete(value)

            # Tag initial table
            values = self.session.query(self.table_classes[tag + "_initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            for value in values:
                self.session.delete(value)
            self.unsaved_modifications = True

        else:
            # The tag has a simple type, the value is removed from both
            # current and initial tables tag columns

            # Current table
            values = self.session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            tag_column_name = self.tag_name_to_column_name(tag)
            if len(values) is 1:
                value = values[0]
                setattr(value, tag_column_name, None)

            # Initial table
            values = self.session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            if len(values) is 1:
                value = values[0]
                setattr(value, tag_column_name, None)
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
        if (valid_type in LIST_TYPES
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

        if self.is_tag_list(tag):
            # The tag has a list type, it is added in the tag tables

            # Initial value
            if initial_value is not None:

                for order in range(0, len(initial_value)):
                    element = initial_value[order]
                    initial_to_add = self.table_classes[tag + "_initial"](
                        index=self.get_path(path).index, order=order,
                        value=element)
                    self.session.add(initial_to_add)

            # Current value
            if current_value is not None:
                for order in range(0, len(current_value)):
                    element = current_value[order]
                    current_to_add = self.table_classes[tag + "_current"](
                        index=self.get_path(path).index, order=order,
                        value=element)
                    self.session.add(current_to_add)

            self.unsaved_modifications = True
        else:
            # The tag has a simple type, it is add it in both current and
            # initial tables

            paths_initial = self.session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).all()
            paths_current = self.session.query(self.table_classes["current"]).join(
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
                self.unsaved_modifications = True

    def new_values(self, values):
        """
        Adds all the values
        :param values (dictionary with path key and [tag, current_value, initial_value] as value)
        """

        tags_is_list = {}
        values_added = []

        for path in values:

            path_index = self.get_path(path).index
            path_initial = self.session.query(self.table_classes["initial"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).first()
            path_current = self.session.query(self.table_classes["current"]).join(
                self.table_classes["path"]).filter(
                self.table_classes["path"].name == path).first()

            path_values = values[path]

            for value in path_values:

                tag = value[0]
                current_value = value[1]
                initial_value = value[2]

                if tag in tags_is_list:
                    is_list = tags_is_list[tag]
                else:
                    is_list = self.is_tag_list(tag)
                    tags_is_list[tag] = is_list

                if is_list:

                    # Old values removed first
                    # Tag current table
                    current_values = self.session.query(self.table_classes[tag + "_current"]).join(
                        self.table_classes["path"]).filter(
                        self.table_classes["path"].name == path).all()
                    for value in current_values:
                        self.session.delete(value)

                    # Tag initial table
                    initial_values = self.session.query(self.table_classes[tag + "_initial"]).join(
                        self.table_classes["path"]).filter(
                        self.table_classes["path"].name == path).all()
                    for value in initial_values:
                        self.session.delete(value)

                    # List values added
                    if initial_value is not None and current_value is not None:
                        for order in range(0, len(initial_value)):
                            initial_to_add = self.table_classes[tag + "_initial"](
                                index=path_index, order=order,
                                value=initial_value[order])
                            current_to_add = self.table_classes[tag + "_current"](
                                index=path_index, order=order,
                                value=current_value[order])
                            values_added.append(initial_to_add)
                            values_added.append(current_to_add)

                else:

                    column_name = self.tag_name_to_column_name(tag)

                    setattr(path_initial, column_name,
                            initial_value)

                    setattr(path_current, column_name,
                                current_value)


        self.unsaved_modifications = True
        self.session.add_all(values_added)

    """ PATHS """

    def get_path(self, path):
        """
        Gives the path table object of a path
        :param path: path name
        """

        path = self.session.query(self.table_classes["path"]).filter(
            self.table_classes["path"].name == path).first()
        return path

    def get_paths_names(self):
        """
        Gives the list of path names
        :param path: List of path names
        """

        paths_list = []
        paths = self.session.query(self.table_classes["path"]).all()
        for path in paths:
            paths_list.append(path.name)
        return paths_list

    def get_paths(self):
        """
        Gives the list of path table objects
        :param path: List of path table objects
        """

        paths_list = []
        paths = self.session.query(self.table_classes["path"]).all()
        for path in paths:
            paths_list.append(path)
        return paths_list

    def remove_path(self, path):
        """
        Removes a path
        :param path: path name
        """

        paths = self.session.query(self.table_classes["path"]).filter(
            self.table_classes["path"].name == path).all()
        if len(paths) is 1:
            path = paths[0]
            self.session.delete(path)
            self.unsaved_modifications = True

            # Thanks to the foreign key and on delete cascade, the path is
            # also removed from all other tables

    def add_path(self, path, checksum):
        """
        Adds a path
        :param path: path path
        :param checksum: path checksum
        """

        # Adding the path in the Tag table
        paths = self.session.query(self.table_classes["path"]).filter(
            self.table_classes["path"].name == path).all()
        if len(paths) is 0:
            path_to_add = self.table_classes["path"](name=path, checksum=checksum)
            self.session.add(path_to_add)

            # Adding the index to both initial and current tables
            initial = self.table_classes["initial"](index=self.get_path(path).index)
            current = self.table_classes["current"](index=self.get_path(path).index)
            self.session.add(current)
            self.session.add(initial)
            self.unsaved_modifications = True

    def add_paths(self, paths):
        """
        Adds all paths
        :param paths: list of paths (path, checksum)
        """

        for path in paths:

            path_name = path[0]
            path_checksum = path[1]

            # Adding the path in the Tag table
            paths_query = self.session.query(self.table_classes["path"]).filter(
                self.table_classes["path"].name == path_name).first()
            if paths_query is None:
                path_to_add = self.table_classes["path"](name=path_name, checksum=path_checksum)
                self.session.add(path_to_add)

                path_index = self.get_path(path_name).index # get_path => 40%

                # Adding the index to both initial and current tables
                initial = self.table_classes["initial"](index=path_index)
                current = self.table_classes["current"](index=path_index)
                self.session.add(current)
                self.session.add(initial)

                self.unsaved_modifications = True

    """ UTILS """

    def get_paths_matching_search(self, search, tags):
        """
        Returns the list of paths names matching the search
        :param search: search to match (str)
        :param tags: List of tags taken into account
        :return: List of path names matching the search
        """

        paths_matching = []

        # Itering over all values and finding matches

        # Search in FileName
        values = self.session.query(self.table_classes["path"].name).filter(self.table_classes["path"].name.like("%" + search + "%")).distinct().all()
        for value in values:
            if value not in paths_matching:
                paths_matching.append(value.name)

        # Only the visible tags are taken into account
        for tag in tags:

            if not self.is_tag_list(tag):
                # The tag has a simple type, the tag column is used in the
                # current table

                values = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).like(
                                "%" + search + "%")).distinct().all()
                for value in values:
                    if value not in paths_matching:
                        paths_matching.append(value.name)
            else:
                # The tag has a list type, the tag current table is used

                for path in self.get_paths_names():
                    path_value = self.get_current_value(path, tag)
                    if (search in str(path_value) and
                            path not in paths_matching):
                        paths_matching.append(path)

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

            if (condition == "="):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)) ==
                    value).distinct().all()
            elif (condition == "!="):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    != value).distinct().all()
            elif (condition == ">="):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)) >=
                    value).distinct().all()
            elif (condition == "<="):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    <= value).distinct().all()
            elif (condition == ">"):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    > value).distinct().all()
            elif (condition == "<"):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag))
                    < value).distinct().all()
            elif (condition == "CONTAINS"):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).contains(
                        value)).distinct().all()
            elif (condition == "BETWEEN"):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).between(
                                value[0],
                                value[1])).distinct().all()
            elif (condition == "IN"):
                query = self.session.query(self.table_classes["path"].name).join(
                    self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)).in_(
                        value)).distinct().all()

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

                couple_query_result = self.session.query(
                    self.table_classes["path"].name).join(
                        self.table_classes["current"]).filter(
                    getattr(self.table_classes["current"],
                            self.tag_name_to_column_name(tag)) == value)
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
        Saves the modifications by committing the session
        """

        self.session.commit()
        self.unsaved_modifications = False

    def unsave_modifications(self):
        """
        Unsaves the modifications by rolling back the session
        """

        self.session.rollback()
        self.unsaved_modifications = False

    def has_unsaved_modifications(self):
        """
        Knowing if the database has pending modifications that are
        unsaved
        :return: True if there are pending modifications to save,
                 False otherwise
        """

        return self.unsaved_modifications

    def update_table_classes(self): # (0.015 sec on average)
        """
        Redefines the model after an update of the schema
        """

        self.table_classes.clear()
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare()
        for table in self.metadata.tables.values():
            self.table_classes[table.name] = getattr(self.base.classes,
                                                     table.name)