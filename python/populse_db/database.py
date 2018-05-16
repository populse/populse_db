import os
from datetime import date, time, datetime

from sqlalchemy import (create_engine, Column, String, Integer, Float,
                        MetaData, Date, DateTime, Time, Table,
                        ForeignKeyConstraint, event, or_, and_)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.engine import Engine

import hashlib

import re

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
                                       LIST_TYPES, SIMPLE_TYPES, TYPE_TO_COLUMN,
                                       ALL_TYPES, ALL_UNITS, INITIAL_TABLE,
                                       CURRENT_TABLE, TAG_TABLE)


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
        - enigne: string engine of the database
        - classes: list of table classes, generated automatically
        - base: database base
        - engine: database engine
        - metadata: database metadata
        - session_maker: session manager
        - unsaved_modifications: to know if there are unsaved
          modifications in the database
        - paths: Paths rows
        - tags: Tags rows
        - initial_paths: Initial paths rows

    methods:
        - add_tag: adds a tag
        - add_tags: adds a list of tags
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

    def __init__(self, string_engine):
        """
        Creates an API of the database instance
        :param string_engine: String engine of the database file, can be already
        existing, or not
        """

        self.string_engine = string_engine
        self.table_classes = {}
        self.names = {}

        # SQLite database: we create it if it does not exist
        if string_engine.startswith('sqlite'):
            db_file = re.sub("sqlite.*:///", "", string_engine)
            if not os.path.exists(db_file):
                parent_dir = os.path.dirname(db_file)
                if not os.path.exists(parent_dir):
                    os.makedirs(os.path.dirname(db_file))
                create_database(string_engine)

        # Database opened
        self.engine = create_engine(self.string_engine)

        # Metadata generated
        self.update_table_classes()

        # Database schema checked
        if (CURRENT_TABLE not in self.table_classes.keys() or
                INITIAL_TABLE not in self.table_classes.keys() or
                TAG_TABLE not in self.table_classes.keys()):
            raise ValueError(
                'The database schema is not coherent with the API.')

        self.session = scoped_session(sessionmaker(bind=self.engine, autocommit=False, autoflush=False))

        self.unsaved_modifications = False

        self.tags = {}
        self.paths = {}
        self.initial_paths = {}

    """ TAGS """

    def add_tags(self, tags):
        """
        Adds the list of tags
        :return: List of tags (name, origin, type, unit, default value, description)
        """

        for tag in tags:

            # Adding each tag
            self.add_tag(tag[0], tag[1], tag[2], tag[3], tag[4], tag[5], False)

        # Updating the table classes
        self.update_table_classes()
        self.session.flush()

    def add_tag(self, name, origin, tag_type, unit, default_value,
                description, update_base = True):
        """
        Adds a tag to the database, if it does not already exist
        :param name: Tag name (str)
        :param origin: Tag origin (Raw or user)
        :param type: Tag type (string, int, float, date, datetime,
                     time, list_string, list_int, list_float, list_date,
                     list_datetime, or list_time)
        :param unit: Tag unit (ms, mm, degree, Hz/pixel, MHz, or None)
        :param default_value: Tag default value (str or None)
        :param description: Tag description (str or None)
        :param update_base: Bool to know if the base must be updated (put False if in the middle of filling tags)
        """

        if not isinstance(name, str):
            raise ValueError("The tag name must be of type " + str(str) + ", but tag name of type " + str(type(name)) + " given")
        tag_row = self.get_tag(name)
        if tag_row != None:
            raise ValueError("A tag with the name " + str(name) + " already exists")
        if not origin in [TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN]:
            raise ValueError("The tag origin must be in " + str([TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN]) + ", but " + str(origin) + " given")
        if not tag_type in ALL_TYPES:
            raise ValueError("The tag type must be in " + str(ALL_TYPES) + ", but " + str(
                tag_type) + " given")
        if not unit in ALL_UNITS and unit is not None:
            raise ValueError("The tag unit must be in " + str(ALL_UNITS) + ", but " + str(
                unit) + " given")
        if not isinstance(default_value, str) and default_value is not None:
            raise ValueError(
                "The tag default value must be of type " + str(str) + " or None, but tag default value of type " + str(type(default_value)) + " given")
        if not isinstance(description, str) and description is not None:
            raise ValueError(
                "The tag description must be of type " + str(str) + " or None, but tag description of type " + str(
                    type(description)) + " given")

        # Adding the tag in the tag table (0.003 sec on average)
        tag = self.table_classes[TAG_TABLE](name=name, origin=origin,
                                        type=tag_type, unit=unit,
                                        default_value=default_value,
                                        description=description)

        self.session.add(tag)
        self.tags[name] = tag

        if tag_type in LIST_TYPES:
            # The tag has a list type: new tag tables added (0.04 sec on average)

            table_name = self.tag_name_to_column_name(name)

            # Tag tables initial and current definition (0.00045 sec on average)
            tag_table_current = Table(table_name + "_current", self.metadata,
                                      Column("name", String,
                                             primary_key=True),
                                      Column("order", Integer,
                                             primary_key=True),
                                      Column("value",
                                             self.tag_type_to_column_type(
                                                 tag_type),
                                             nullable=False))
            tag_table_initial = Table(table_name + "_initial", self.metadata,
                                      Column("name", String,
                                             primary_key=True),
                                      Column("order", Integer,
                                             primary_key=True),
                                      Column("value",
                                             self.tag_type_to_column_type(
                                                 tag_type),
                                             nullable=False))

            # Both tables added (0.03 sec on average)
            current_query = CreateTable(tag_table_current)
            initial_query = CreateTable(tag_table_initial)

            # 0.03 seconds to execute those 2 queries
            self.session.execute(current_query)
            self.session.execute(initial_query)

        elif tag_type in SIMPLE_TYPES:
            # The tag has a simple type: new column added to both initial
            # and current tables (0.06 sec on average)

            # Column creation
            column = Column(name, self.tag_type_to_column_type(tag_type))
            column_type = column.type.compile(self.engine.dialect)

            # Tag column added to both initial and current tables (0.05 sec on average)
            self.session.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    INITIAL_TABLE, "\"" + self.tag_name_to_column_name(name) + "\"",
                    column_type))
            self.session.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    CURRENT_TABLE, "\"" + self.tag_name_to_column_name(name) + "\"",
                    column_type))

            self.paths.clear()
            self.initial_paths.clear()

        self.unsaved_modifications = True

        # Redefinition of the table classes
        if update_base:
            self.update_table_classes()
            self.session.flush()

    def tag_type_to_column_type(self, tag_type):
        """
        Gives the column type corresponding to the tag type
        :param tag_type: Tag type
        :return: The column type given the tag type
        """

        return TYPE_TO_COLUMN[tag_type]

    def tag_name_to_column_name(self, tag):
        """
        Transforms the tag name into a valid and unique column name, by hashing the tag name
        :return: Valid column name
        """

        if tag in self.names:
            return self.names[tag]
        else:
            column_name = hashlib.md5(tag.encode('utf-8')).hexdigest()
            self.names[tag] = column_name
            return column_name

    def remove_tag(self, name):
        """
        Removes a tag
        :param name: Tag name (str)
        """

        if not isinstance(name, str):
            raise ValueError(
                "The tag name must be of type " + str(str) + ", but tag name of type " + str(type(name)) + " given")
        tag_row = self.get_tag(name)
        if tag_row is None:
            raise ValueError("The tag with the name " + str(name) + " does not exist")

        is_tag_list = self.is_tag_list(name)

        if is_tag_list:
            # The tag has a list type, both tag tables are removed

            table_name = self.tag_name_to_column_name(name)
            initial_query = DropTable(self.table_classes[table_name + "_initial"].__table__)
            self.session.execute(initial_query)
            current_query = DropTable(self.table_classes[table_name + "_current"].__table__)
            self.session.execute(current_query)

        else:
            # The tag has a simple type, the tag column is removed from
            # both current and initial tables

            # Tag column removed from initial table
            columns = ""
            sql_table_create = CreateTable(
                self.table_classes[INITIAL_TABLE].__table__)
            for column in sql_table_create.columns:
                if self.tag_name_to_column_name(name) in str(column):
                    column_to_remove = column
                else:
                    columns += str(column).split(" ")[0] + ", "
            sql_table_create.columns.remove(column_to_remove)
            sql_query = str(sql_table_create)
            sql_query = sql_query[:21] + '_backup' + sql_query[21:]
            columns = columns[:-2]
            self.session.execute(sql_query)
            self.session.execute("INSERT INTO initial_backup SELECT " +
                            columns + " FROM " + INITIAL_TABLE)
            self.session.execute("DROP TABLE " + INITIAL_TABLE)
            sql_query = sql_query[:21] + sql_query[29:]
            self.session.execute(sql_query)
            self.session.execute("INSERT INTO " + INITIAL_TABLE + " SELECT " + columns +
                            " FROM initial_backup")
            self.session.execute("DROP TABLE initial_backup")

            # Tag column removed from current table
            columns = ""
            sql_table_create = CreateTable(
                self.table_classes[CURRENT_TABLE].__table__)
            for column in sql_table_create.columns:
                if self.tag_name_to_column_name(name) in str(column):
                    column_to_remove = column
                else:
                    columns += str(column).split(" ")[0] + ", "
            sql_table_create.columns.remove(column_to_remove)
            sql_query = str(sql_table_create)
            sql_query = sql_query[:21] + '_backup' + sql_query[21:]
            columns = columns[:-2]
            self.session.execute(sql_query)
            self.session.execute("INSERT INTO current_backup SELECT " +
                            columns + " FROM " + CURRENT_TABLE)
            self.session.execute("DROP TABLE " + CURRENT_TABLE)
            sql_query = sql_query[:21] + sql_query[29:]
            self.session.execute(sql_query)
            self.session.execute("INSERT INTO " + CURRENT_TABLE + " SELECT " + columns +
                            " FROM current_backup")
            self.session.execute("DROP TABLE current_backup")

            self.paths.clear()
            self.initial_paths.clear()

        self.tags.pop(name, None)
        self.session.delete(tag_row)
        self.session.flush()
        self.update_table_classes()
        self.unsaved_modifications = True

    def get_tag(self, name):
        """
        Gives the tag row given a tag name
        :param name: Tag name
        :return: The tag row if the tag exists, None otherwise
        """

        if name in self.tags:
            return self.tags[name]
        else:
            tag = self.session.query(self.table_classes[TAG_TABLE]).filter(
                self.table_classes[TAG_TABLE].name == name).first()
            self.tags[name] = tag
            return tag

    def get_tags_names(self):
        """
        Gives the list of tags
        :return: List of tag names
        """

        tags_list = []
        tags = self.session.query(self.table_classes[TAG_TABLE].name).all()
        for tag in tags:
            tags_list.append(tag.name)
        return tags_list

    def get_tags(self):
        """
        Gives the list of tag table objects
        :return: List of tag table objects
        """

        tags_list = []
        tags = self.session.query(self.table_classes[TAG_TABLE]).all()
        for tag in tags:
            tags_list.append(tag)
        return tags_list

    def is_tag_list(self, tag):
        """
        To know if the given tag is a list
        :param tag: tag name
        :return: True if the tag is a list, False otherwise
        """

        tag = self.get_tag(tag)
        if tag != None:
            return tag.type in LIST_TYPES
        else:
            return None

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
        :param path: path name (str)
        :param tag: Tag name (str)
        :return: The current value of <path, tag> if it exists, None otherwise
        """

        tag_row = self.get_tag(tag)
        if tag_row is None:
            return None
        path_row = self.get_path(path)
        if path_row is None:
            return None

        if self.is_tag_list(tag):
            # The tag has a type list, the values are gotten from the tag
            # current table

            table_name = self.tag_name_to_column_name(tag)
            values = self.session.query(self.table_classes[table_name + "_current"]).filter(
                self.table_classes[table_name + "_current"].name == path).all()
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

            return getattr(path_row, self.tag_name_to_column_name(tag))

    def get_initial_value(self, path, tag):
        """
        Gives the initial value of <path, tag>
        :param path: path name
        :param tag: Tag name
        :return: The initial value of <path, tag> if it exists, None otherwise
        """

        tag_row = self.get_tag(tag)
        if tag_row is None:
            return None
        path_row = self.get_path(path)
        if path_row is None:
            return None

        if self.is_tag_list(tag):
            # The tag has a type list, the values are gotten from the tag
            # initial table

            table_name = self.tag_name_to_column_name(tag)
            values = self.session.query(self.table_classes[table_name + "_initial"]).filter(
                self.table_classes[table_name + "_initial"].name == path).all()
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

            initial_path_row = self.get_initial_path(path)
            return getattr(initial_path_row, self.tag_name_to_column_name(tag))

    def is_value_modified(self, path, tag):
        """
        To know if a value has been modified
        :param path: path name
        :param tag: tag name
        :return: True if the value has been modified, False otherwise
        """

        tag_row = self.get_tag(tag)
        if tag_row is None:
            return False
        path_row = self.get_path(path)
        if path_row is None:
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

        tag_row = self.get_tag(tag)
        if tag_row is None:
            raise ValueError("The tag with the name " + str(tag) + " does not exist")
        path_row = self.get_path(path)
        if path_row is None:
            raise ValueError("The path with the name " + str(path) + " does not exist")
        if not self.check_type_value(new_value, tag_row.type):
            raise ValueError("The value " + str(new_value) + " is invalid")

        if self.is_tag_list(tag):
            # The path has a list type, the values are reset in the tag
            # current table

            table_name = self.tag_name_to_column_name(tag)

            values = self.session.query(self.table_classes[table_name + "_current"]).filter(
                self.table_classes[table_name + "_current"].name == path).all()
            for index in range(0, len(values)):
                value_to_modify = values[index]
                value_to_modify.value = new_value[index]

        else:
            # The path has a simple type, the values are reset in the tag
            # column in current table


            setattr(path_row, self.tag_name_to_column_name(tag), new_value)

        self.session.flush()
        self.unsaved_modifications = True

    def reset_current_value(self, path, tag):
        """
        Resets the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        """
        tag_row = self.get_tag(tag)
        if tag_row is None:
            raise ValueError("The tag with the name " + str(tag) + " does not exist")
        path_row = self.get_path(path)
        if path_row is None:
            raise ValueError("The path with the name " + str(path) + " does not exist")

        if self.is_tag_list(tag):
            # The path has a list type, the values are reset in the tag
            # current table

            table_name = self.tag_name_to_column_name(tag)

            values = self.session.query(self.table_classes[table_name + "_current"]).filter(
                self.table_classes[table_name + "_current"].name == path).all()
            for index in range(0, len(values)):
                value_to_modify = values[index]
                value_to_modify.value = self.get_initial_value(path,
                                                               tag)[index]

        else:
            # The path has a simple type, the value is reset in the current
            # table

            setattr(path_row, self.tag_name_to_column_name(tag),
                        self.get_initial_value(path, tag))

        self.session.flush()
        self.unsaved_modifications = True

    def remove_value(self, path, tag):
        """
        Removes the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        """

        tag_row = self.get_tag(tag)
        if tag_row is None:
            raise ValueError("The tag with the name " + str(tag) + " does not exist")
        path_row = self.get_path(path)
        if path_row is None:
            raise ValueError("The path with the name " + str(path) + " does not exist")

        if self.is_tag_list(tag):
            # The tag has a list type, the values are removed from both tag
            # current and initial tables

            table_name = self.tag_name_to_column_name(tag)

            # Tag current table
            values = self.session.query(self.table_classes[table_name + "_current"]).filter(
                self.table_classes[table_name + "_current"].name == path).all()
            for value in values:
                self.session.delete(value)

            # Tag initial table
            values = self.session.query(self.table_classes[table_name + "_initial"]).filter(
                self.table_classes[table_name + "_initial"].name == path).all()
            for value in values:
                self.session.delete(value)

        else:
            # The tag has a simple type, the value is removed from both
            # current and initial tables tag columns

            tag_column_name = self.tag_name_to_column_name(tag)

            # Current table
            setattr(path_row, tag_column_name, None)

            # Initial table
            path_initial_row = self.get_initial_path(path)
            setattr(path_initial_row, tag_column_name, None)

        self.session.flush()
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

    def new_value(self, path, tag, current_value, initial_value, flush=True):
        """
        Adds a value for <path, tag> (as initial and current)
        :param path: path name
        :param tag: tag name
        :param current_value: current value
        :param initial_value: initial value
        :param flush: Bool to know if flush to do (Put False in the middle of adding values)
        """

        tag_row = self.get_tag(tag)
        if tag_row is None:
            raise ValueError("The tag with the name " + str(tag) + " does not exist")
        path_row = self.get_path(path)
        if path_row is None:
            raise ValueError("The path with the name " + str(path) + " does not exist")
        if not self.check_type_value(current_value, tag_row.type):
            raise ValueError("The current value " + str(current_value) + " is invalid")
        if not self.check_type_value(initial_value, tag_row.type):
            raise ValueError("The initial value " + str(initial_value) + " is invalid")

        table_name = self.tag_name_to_column_name(tag)

        if self.is_tag_list(tag):
            # The tag has a list type, it is added in the tag tables

            # Initial value
            if initial_value is not None:

                for order in range(0, len(initial_value)):
                    element = initial_value[order]
                    initial_to_add = self.table_classes[table_name + "_initial"](
                        name=path, order=order,
                        value=element)
                    self.session.add(initial_to_add)

                self.unsaved_modifications = True

            # Current value
            if current_value is not None:
                for order in range(0, len(current_value)):
                    element = current_value[order]
                    current_to_add = self.table_classes[table_name + "_current"](
                        name=path, order=order,
                        value=element)
                    self.session.add(current_to_add)

                if flush:
                    self.session.flush()
                self.unsaved_modifications = True

        else:
            # The tag has a simple type, it is add it in both current and
            # initial tables

            path_initial = self.get_initial_path(path)
            database_current_value = getattr(
                path_row, self.tag_name_to_column_name(tag))
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
                        path_row, self.tag_name_to_column_name(tag),
                        current_value)

                if flush:
                    self.session.flush()
                self.unsaved_modifications = True

            else:
                raise ValueError("The tuple <" + str(tag) + ", " + str(path) + "> already has a value")

    """ PATHS """

    def get_path(self, path):
        """
        Gives the path row of a path
        :param path: path name
        :return The path row if the path exists, None otherwise
        """

        if path in self.paths:
            return self.paths[path]
        else:
            path_row = self.session.query(self.table_classes[CURRENT_TABLE]).filter(
        self.table_classes[CURRENT_TABLE].name == path).first()
            self.paths[path] = path_row
            return path_row

    def get_initial_path(self, path):
        """
        Gives the initial path row of a path
        :param path: path name
        :return The initial path row if the path exists, None otherwise
        """

        if path in self.initial_paths:
            return self.initial_paths[path]
        else:
            path_row = self.session.query(self.table_classes[INITIAL_TABLE]).filter(
        self.table_classes[INITIAL_TABLE].name == path).first()
            self.initial_paths[path] = path_row
            return path_row

    def get_paths_names(self):
        """
        Gives the list of path names
        :param path: List of path names
        """

        paths_list = []
        paths = self.session.query(self.table_classes[CURRENT_TABLE]).all()
        for path in paths:
            paths_list.append(path.name)
        return paths_list

    def get_paths(self):
        """
        Gives the list of path table objects
        :param path: List of path table objects
        """

        paths_list = []
        paths = self.session.query(self.table_classes[CURRENT_TABLE]).all()
        for path in paths:
            paths_list.append(path)
        return paths_list

    def remove_path(self, path):
        """
        Removes a path
        :param path: path name
        """

        path_row = self.get_path(path)
        if path_row is None:
            raise ValueError("The path with the name " + str(path) + " does not exist")

        for table_class in self.table_classes:
            if table_class != TAG_TABLE:
                self.session.query(self.table_classes[table_class]).filter(
                    self.table_classes[table_class].name == path).delete()

        self.paths.pop(path, None)
        self.initial_paths.pop(path, None)
        self.session.flush()
        self.unsaved_modifications = True

    def add_path(self, path):
        """
        Adds a path
        :param path: file path
        """

        path_row = self.get_path(path)
        if path_row is not None:
            raise ValueError("A path with the name " + str(path) + " already exists")
        if not isinstance(path, str):
            raise ValueError(
                "The path name must be of type " + str(str) + ", but path name of type " + str(
                    type(path)) + " given")

        # Adding the index to both initial and current tables
        initial = self.table_classes[INITIAL_TABLE](name=path)
        current = self.table_classes[CURRENT_TABLE](name=path)
        self.session.add(current)
        self.session.add(initial)
        self.session.flush()
        self.paths[path] = current
        self.initial_paths[path] = initial
        self.unsaved_modifications = True

    """ UTILS """

    def get_paths_matching_search(self, search, tags):
        """
        Returns the list of paths names matching the search
        :param search: search to match (str)
        :param tags: List of tags taken into account
        :return: List of path names matching the search
        """

        if not isinstance(tags, list):
            return []
        if not isinstance(search, str):
            return []

        paths_matching = []
        simple_tags_filters = []

        # Iterating over all values and finding matches

        # Search in path name
        values = self.session.query(self.table_classes[CURRENT_TABLE].name)
        simple_tags_filters.append(self.table_classes[CURRENT_TABLE].name.like("%" + search + "%"))

        # Search for each tag
        for tag in tags:

            is_list = self.is_tag_list(tag)
            if is_list is False:
                # The tag has a simple type, the tag column is used in the
                # current table

                simple_tags_filters.append(getattr(self.table_classes[CURRENT_TABLE], self.tag_name_to_column_name(tag)).like("%" + search + "%"))

            elif is_list is True:
                # The tag has a list type, the tag current table is used

                simple_tags_filters.append(and_(self.table_classes[CURRENT_TABLE].name == self.table_classes[self.tag_name_to_column_name(tag) + "_current"].name, self.table_classes[self.tag_name_to_column_name(tag) + "_current"].value.like("%" + search + "%")))

        values = values.filter(or_(*simple_tags_filters)).distinct().all()
        for value in values:
            paths_matching.append(value.name)

        return paths_matching

    def get_paths_matching_constraints(self, tag, value, condition):
        """
        Gives the paths corresponding to the constraints
        :param tag: tag name
        :param value: value
        :param condition: condition
        :return: List of paths matching the constraints given in parameter
        """

        if tag == "FileName":

            if (condition == "="):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name == value).distinct().all()
            elif (condition == "!="):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name != value).distinct().all()
            elif (condition == ">="):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name >= value).distinct().all()
            elif (condition == "<="):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name <= value).distinct().all()
            elif (condition == ">"):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name > value).distinct().all()
            elif (condition == "<"):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name < value).distinct().all()
            elif (condition == "CONTAINS"):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name.contains(value)).distinct().all()
            elif (condition == "BETWEEN"):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name.between(value[0], value[1])).distinct().all()
            elif (condition == "IN"):
                values = self.session.query(self.table_classes[INITIAL_TABLE].name).filter(
                    self.table_classes[INITIAL_TABLE].name._in(value)).distinct().all()

            paths_list = []
            for path in values:
                if path.name not in paths_list:
                    paths_list.append(path.name)
            return paths_list

        elif not self.is_tag_list(tag):
            # The tag has a simple type, the tag column is used in the current
            # table

            if (condition == "="):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag)) ==
                    value).distinct().all()
            elif (condition == "!="):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag))
                    != value).distinct().all()
            elif (condition == ">="):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag)) >=
                    value).distinct().all()
            elif (condition == "<="):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag))
                    <= value).distinct().all()
            elif (condition == ">"):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag))
                    > value).distinct().all()
            elif (condition == "<"):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag))
                    < value).distinct().all()
            elif (condition == "CONTAINS"):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag)).contains(
                        value)).distinct().all()
            elif (condition == "BETWEEN"):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag)).between(
                                value[0],
                                value[1])).distinct().all()
            elif (condition == "IN"):
                query = self.session.query(self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
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
                                           values, nots, scans_list):
        """
        Gives the paths matching the advanced search
        :param links: Links (AND/OR)
        :param fields: Fields (tag name/List of tags/FileName)
        :param conditions: Conditions (=, !=, <, >, <=, >=, BETWEEN,
                           CONTAINS, IN)
        :param values: Values (Typed value for =, !=, <, >, <=, >=, and
                       CONTAINS/list for BETWEEN and IN)
        :param nots: Nots (Empty or NOT)
        :param scans_list: List of scans to take into account
        :return: List of path names matching all the constraints
        """

        if not isinstance(links, list) or not isinstance(fields, list) or not isinstance(conditions, list) or not isinstance(values, list) or not isinstance(nots, list) or not isinstance(scans_list, list):
            return []
        if (not len(links) == len(fields) - 1 == len(conditions) - 1 ==
                len(values) - 1 == len(nots) - 1):
            return []
        for link in links:
            if link not in ["AND", "OR"]:
                return []
        fields_list = self.get_tags_names()
        fields_list.append("FileName")
        for field in fields:
            if not isinstance(field, list) and field not in fields_list:
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
                field = fields[i]
                if field == "FileName":
                    if not isinstance(value, str):
                        return []
                elif not isinstance(field, list):
                    tag_type = self.get_tag(field).type
                    if not self.check_type_value(value, tag_type):
                        return []
        for not_ in nots:
            if not_ not in ["", "NOT"]:
                return []

        queries = []  # list of paths of each query (row)
        for i in range(0, len(conditions)):
            queries.append([])
            if not isinstance(fields[i], list):

                # Tag filter: Only those values are read

                queries[i] = self.get_paths_matching_constraints(fields[i],
                                                                 values[i],
                                                                 conditions[i])

            else:
                # No tag filter, all values are read

                queries[i] = list(set(queries[i]).union(set(
                    self.get_paths_matching_constraints("FileName",
                                                        values[i],
                                                        conditions[i]))))

                for tag in fields[i]:
                    queries[i] = list(set(queries[i]).union(set(
                        self.get_paths_matching_constraints(tag,
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

        # Removing scans if they are not taken into account
        result_copy = list(result)
        for scan in result_copy:
            if scan not in scans_list:
                result.remove(scan)

        return result

    def get_paths_matching_tag_value_couples(self, tag_value_couples):
        """
        Checks if a path contains all the couples <tag, value> given in
        parameter
        :param tag_value_couples: List of couple <tag(str), value(Typed)> to check
        :return: List of paths matching all the <tag, value> couples
        """

        if not isinstance(tag_value_couples, list) or not len(tag_value_couples) > 0:
            return []

        couple_results = []
        for couple in tag_value_couples:

            if not isinstance(couple, list) or len(couple) != 2:
                return []

            tag = couple[0]
            value = couple[1]

            tag_row = self.get_tag(tag)
            if tag_row is None:
                return []
            if not self.check_type_value(value, tag_row.type):
                return []

            couple_result = []
            is_list = self.is_tag_list(tag)

            if is_list is False:
                # The tag has a simple type, the tag column in the current
                # table is used

                couple_query_result = self.session.query(
                    self.table_classes[CURRENT_TABLE].name).filter(
                    getattr(self.table_classes[CURRENT_TABLE],
                            self.tag_name_to_column_name(tag)) == value)
                for query_result in couple_query_result:
                    couple_result.append(query_result.name)

            elif is_list is True:
                # The tag has a list type, the tag current table is used

                for path in self.get_paths_names():
                    path_value = self.get_current_value(path, tag)
                    if path_value == value:
                        couple_result.append(path)

            couple_results.append(couple_result)

        # All the path lists are put together, with intersections
        # Only the paths with all <tag, value> are taken
        final_result = couple_results[0]
        for i in range(0, len(couple_results) - 1):
            final_result = list(set(final_result).intersection(
                set(couple_results[i + 1])))
        return final_result

    def start_transaction(self):
        """
        Starts a new transaction
        """

        self.session.begin_nested()

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