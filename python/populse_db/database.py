import six
import os
import hashlib
import ast
import re
import copy
from datetime import date, time, datetime
import dateutil.parser
import hashlib
import ast
import re
import types

from sqlalchemy import (create_engine, Column, String,
                        MetaData, event, or_, and_, not_, Table, sql)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.engine import Engine

from populse_db.database_model import (create_database, TAG_TYPE_INTEGER,
                                       TAG_TYPE_FLOAT, TAG_TYPE_TIME,
                                       TAG_TYPE_DATETIME, TAG_TYPE_DATE,
                                       TAG_TYPE_STRING, TAG_TYPE_LIST_DATE,
                                       TAG_TYPE_LIST_DATETIME,
                                       TAG_TYPE_LIST_FLOAT,
                                       TAG_TYPE_LIST_INTEGER,
                                       TAG_TYPE_LIST_STRING,
                                       TAG_TYPE_LIST_TIME,
                                       TAG_ORIGIN_USER, TAG_ORIGIN_BUILTIN,
                                       LIST_TYPES, TYPE_TO_COLUMN, TAG_TYPE_BOOLEAN,
                                       ALL_TYPES, ALL_UNITS, PATH_TABLE, TAG_TABLE, INITIAL_TABLE)

from populse_db.filter import filter_parser, FilterToQuery

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
        - names: columns names

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
        - update_table_classes: redefines the model after schema update
    """

    # Some types (e.g. time, date and datetime) cannot be
    # serialized/deserialized into string with repr/ast.literal_eval.
    # This is a problem for storing the corresponding list_tags in
    # database. For the list types with this problem, we record in the
    # following dictionaries the functions that must be used to serialize
    # (in _list_item_to_string) and deserialize (in _string_to_list_item)
    # the list items.
    _list_item_to_string = {
        TAG_TYPE_LIST_DATE: lambda x: x.isoformat(),
        TAG_TYPE_LIST_DATETIME: lambda x: x.isoformat(),
        TAG_TYPE_LIST_TIME: lambda x: x.isoformat()
    }
    
    _string_to_list_item = {
        TAG_TYPE_LIST_DATE: lambda x: dateutil.parser.parse(x).date(),
        TAG_TYPE_LIST_DATETIME: lambda x: dateutil.parser.parse(x),
        TAG_TYPE_LIST_TIME: lambda x: dateutil.parser.parse(x).time(),
    }

    def __init__(self, string_engine, initial_table=False, path_caches=False):
        """
        Creates an API of the database instance
        :param string_engine: String engine of the database file, can be already existing, or not
        :param initial_table: To know if the initial table must be created, False by default
        :param path_caches: To know if the path caches must be used, False by default
        """

        self.string_engine = string_engine

        self.initial_table = initial_table
        self.paths_caches = path_caches

        # SQLite database: we create it if it does not exist
        if string_engine.startswith('sqlite'):
            self.db_file = re.sub("sqlite.*:///", "", string_engine)
            if not os.path.exists(self.db_file):
                parent_dir = os.path.dirname(self.db_file)
                if not os.path.exists(parent_dir):
                    os.makedirs(os.path.dirname(self.db_file))
                create_database(string_engine, self.initial_table)

        # Database opened
        self.engine = create_engine(self.string_engine)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.update_table_classes()

        # Database schema checked
        if (PATH_TABLE not in self.table_classes.keys() or
                TAG_TABLE not in self.table_classes.keys()):
            raise ValueError(
                'The database schema is not coherent with the API')
        if self.initial_table and INITIAL_TABLE not in self.table_classes.keys():
            raise ValueError(
                'The initial_table flag cannot be True if the database has been created without the initial_table flag')

        self.session = scoped_session(sessionmaker(bind=self.engine, autocommit=False, autoflush=False))

        self.unsaved_modifications = False

        tags = self.session.query(self.table_classes[TAG_TABLE])
        self.tags = dict((tag.name, tag) for tag in tags)
        
        self.names = {}

        if self.paths_caches:
            self.paths = {}
            if self.initial_table:
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
        self.session.flush()
        self.update_table_classes()

    def add_tag(self, name, origin, tag_type, unit, default_value,
                description, flush = True):
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
        :param flush: Bool to know if the base must be updated (put False if in the middle of filling tags)
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

        # Adding the tag in the tag table
        tag = self.table_classes[TAG_TABLE](name=name, origin=origin,
                                        type=tag_type, unit=unit,
                                        default_value=default_value,
                                        description=description)

        self.session.add(tag)
        self.tags[name] = tag

        # Columns creation
        if tag_type in LIST_TYPES:
            # String columns if it list type, as the str representation of the list will be stored
            column_type = String
        else:
            column_type = self.tag_type_to_column_type(tag_type)
        column = Column(self.tag_name_to_column_name(name), column_type)
        column_str_type = column.type.compile(self.engine.dialect)
        column_name = column.compile(dialect=self.engine.dialect)

        # Tag current and initial columns added added to path table

        path_query = str('ALTER TABLE %s ADD COLUMN %s %s' % (PATH_TABLE, column_name, column_str_type))
        self.session.execute(path_query)
        self.table_classes[PATH_TABLE].__table__.append_column(column)

        if self.initial_table:
            column_initial = Column(self.tag_name_to_column_name(name), column_type)
            initial_query = str('ALTER TABLE %s ADD COLUMN %s %s' % (INITIAL_TABLE, column_name, column_str_type))
            self.session.execute(initial_query)
            self.table_classes[INITIAL_TABLE].__table__.append_column(column_initial)

        if self.paths_caches:
            self.paths.clear()
            if self.initial_table:
                self.initial_paths.clear()

        self.unsaved_modifications = True

        # Redefinition of the table classes
        if flush:
            self.session.flush()
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
        Transforms the tag name into a valid and unique column name, by hashing the tag name
        :return: Valid column name
        """

        if tag in self.names:
            return self.names[tag]
        else:
            column_name = hashlib.md5(tag.encode('ascii')).hexdigest()
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

        column_name = self.tag_name_to_column_name(name)

        # Tag removed from path table
        old_path_table = Table(PATH_TABLE, self.metadata)
        select = sql.select([c for c in old_path_table.c if column_name not in c.name])

        remaining_columns = [copy.copy(c) for c in old_path_table.columns
                             if column_name not in c.name]

        path_backup_table = Table(PATH_TABLE + "_backup", self.metadata)
        for column in old_path_table.columns:
            if column_name not in str(column):
                path_backup_table.append_column(column.copy())
        self.session.execute(CreateTable(path_backup_table))

        insert = sql.insert(path_backup_table).from_select(
            [c.name for c in remaining_columns], select)
        self.session.execute(insert)

        self.metadata.remove(old_path_table)
        self.session.execute(DropTable(old_path_table))

        new_path_table = Table(PATH_TABLE, self.metadata)
        for column in path_backup_table.columns:
            new_path_table.append_column(column.copy())

        self.session.execute(CreateTable(new_path_table))

        select = sql.select([c for c in path_backup_table.c if column_name not in c.name])
        insert = sql.insert(new_path_table).from_select(
            [c.name for c in remaining_columns], select)
        self.session.execute(insert)

        self.session.execute(DropTable(path_backup_table))

        # Tag removed from initial table if initial values are used
        if self.initial_table:
            old_initial_table = Table(INITIAL_TABLE, self.metadata)
            select = sql.select([c for c in old_initial_table.c if column_name not in c.name])

            remaining_columns = [copy.copy(c) for c in old_initial_table.columns
                                 if column_name not in c.name]

            initial_backup_table = Table(INITIAL_TABLE + "_backup", self.metadata)

            for column in old_initial_table.columns:
                if column_name not in str(column):
                    initial_backup_table.append_column(column.copy())
            self.session.execute(CreateTable(initial_backup_table))

            insert = sql.insert(initial_backup_table).from_select(
                [c.name for c in remaining_columns], select)
            self.session.execute(insert)

            self.metadata.remove(old_initial_table)
            self.session.execute(DropTable(old_initial_table))

            new_initial_table = Table(INITIAL_TABLE, self.metadata)
            for column in initial_backup_table.columns:
                new_initial_table.append_column(column.copy())

            self.session.execute(CreateTable(new_initial_table))

            select = sql.select([c for c in initial_backup_table.c if column_name not in c.name])
            insert = sql.insert(new_initial_table).from_select(
                [c.name for c in remaining_columns], select)
            self.session.execute(insert)

            self.session.execute(DropTable(initial_backup_table))

        if self.paths_caches:
            self.paths.clear()
            if self.initial_table:
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
        return self.tags.get(name)

    def get_tags_names(self):
        """
        Gives the list of tags
        :return: List of tag names
        """
        return list(tag.name for tag in self.get_tags())

    def get_tags(self):
        """
        Gives the list of tag table objects
        :return: List of tag table objects
        """
        return self.tags.values()
    
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

        return TagRow(self, path_row)[tag]

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
        initial_path_row = self.get_initial_path(path)
        if initial_path_row is None:
            return None

        return TagRow(self, initial_path_row)[tag]

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

        new_value = self.python_to_column(tag_row.type, new_value)

        setattr(path_row.row, self.tag_name_to_column_name(tag), new_value)

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
        if not self.initial_table:
            raise ValueError("Impossible to reset values if the initial values are not activated, you can activate the flag initial_table when creating the Database instance")

        initial_value = self.get_initial_value(path, tag)

        if tag_row.type in LIST_TYPES:
            initial_value = str(initial_value)

        setattr(path_row.row, self.tag_name_to_column_name(tag), initial_value)

        self.session.flush()
        self.unsaved_modifications = True

    def remove_value(self, path, tag, flush=True):
        """
        Removes the value associated to <path, tag>
        :param path: path name
        :param tag: tag name
        :param flush: To know if flush to do (put False in the middle of removing values)
        """

        tag_row = self.get_tag(tag)
        if tag_row is None:
            raise ValueError("The tag with the name " + str(tag) + " does not exist")
        path_row = self.get_path(path)
        if path_row is None:
            raise ValueError("The path with the name " + str(path) + " does not exist")

        tag_column_name = self.tag_name_to_column_name(tag)

        setattr(path_row.row, tag_column_name , None)

        if self.initial_table:
            initial_path_row = self.get_initial_path(path)
            setattr(initial_path_row.row, tag_column_name, None)

        if flush:
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
        if valid_type == TAG_TYPE_BOOLEAN and value_type == bool:
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
                if not self.check_type_value(value_element, valid_type.replace("list_", "")):
                    return False
            return True
        return False

    def new_value(self, path, tag, current_value, initial_value=None, checks=True):
        """
        Adds a value for <path, tag> (as initial and current)
        :param path: path name
        :param tag: tag name
        :param current_value: current value
        :param initial_value: initial value
        :param checks: Bool to know if flush to do and value check (Put False in the middle of adding values, during import)
        """

        tag_row = self.get_tag(tag)
        path_row = self.get_path(path)
        if checks:
            if tag_row is None:
                raise ValueError("The tag with the name " + str(tag) + " does not exist")
            if path_row is None:
                raise ValueError("The path with the name " + str(path) + " does not exist")
            if not self.check_type_value(current_value, tag_row.type):
                raise ValueError("The current value " + str(current_value) + " is invalid")
            if not self.check_type_value(initial_value, tag_row.type):
                raise ValueError("The initial value " + str(initial_value) + " is invalid")
            if not self.initial_table and not initial_value is None:
                raise ValueError("Impossible to add an initial value if the initial values are not activated, you can activate the flag initial_table when creating the Database instance")

        column_name = self.tag_name_to_column_name(tag)
        database_current_value = getattr(
            path_row, column_name)

        if self.initial_table:
            path_initial_row = self.get_initial_path(path)
            database_initial_value = getattr(
                path_initial_row, column_name)
        else:
            database_initial_value = None

        # We add the value only if it does not already exist
        if (database_current_value is None and
                database_initial_value is None):
            if initial_value is not None:
                initial_value = self.python_to_column(tag_row.type, initial_value)
                setattr(
                    path_initial_row.row, column_name,
                    initial_value)
            if current_value is not None:
                current_value = self.python_to_column(tag_row.type, current_value)
                setattr(
                    path_row.row, column_name,
                    current_value)

            if checks:
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

        if self.paths_caches and path in self.paths:
            return self.paths[path]
        else:
            path_row = self.session.query(self.table_classes[PATH_TABLE]).filter(
        self.table_classes[PATH_TABLE].name == path).first()
            if path_row is not None:
                path_row = TagRow(self, path_row)
            if self.paths_caches:
                self.paths[path] = path_row
            return path_row

    def get_initial_path(self, path):
        """
        Gives the initial path row of a path
        :param path: path name
        :return The initial path row if the path exists, None otherwise
        """

        if not self.initial_table:
            raise ValueError("The initial values aren't activated, you can activate the flag initial_table when creating the Database instance")
        if self.paths_caches and path in self.initial_paths:
            return self.initial_paths[path]
        else:
            path_row = self.session.query(self.table_classes[INITIAL_TABLE]).filter(
                            self.table_classes[INITIAL_TABLE].name == path).first()
            if path_row is not None:
                path_row = TagRow(self, path_row)
            if self.paths_caches:
                self.initial_paths[path] = path_row
            return path_row

    def get_paths_names(self):
        """
        Gives the list of path names
        :param path: List of path names
        """

        paths_list = []
        paths = self.session.query(self.table_classes[PATH_TABLE]).all()
        for path in paths:
            paths_list.append(path.name)
        return paths_list

    def get_paths(self):
        """
        Gives the list of path table objects
        :param path: List of path table objects
        """

        paths_list = []
        paths = self.session.query(self.table_classes[PATH_TABLE]).all()
        for path in paths:
            paths_list.append(TagRow(self,path))
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

        if self.paths_caches:
            self.paths[path] = None
            self.initial_paths = None

        self.session.flush()
        self.unsaved_modifications = True

    def add_path(self, path, checks=True):
        """
        Adds a path
        :param path: file path
        :param checks: checks if the path already exists and flushes, put False in the middle of filling the table
        """

        if checks:
            path_row = self.get_path(path)
            if path_row is not None:
                raise ValueError("A path with the name " + str(path) + " already exists")
        if not isinstance(path, str):
            raise ValueError(
                "The path name must be of type " + str(str) + ", but path name of type " + str(
                    type(path)) + " given")

        # Adding the index to path table
        path_row = self.table_classes[PATH_TABLE](name=path)
        self.session.add(path_row)

        if self.paths_caches:
            path_row = TagRow(self, path_row)
            self.paths[path] = path_row

        # Adding the index to initial table if initial values are used
        if self.initial_table:
            initial_path_row = self.table_classes[INITIAL_TABLE](name=path)
            self.session.add(initial_path_row)

            if self.paths_caches:
                initial_path_row = TagRow(self, initial_path_row)
                self.initial_paths[path] = initial_path_row

        if checks:
            self.session.flush()

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
        for tag in tags:
            tag_row = self.get_tag(tag)
            if tag_row is None:
                return []

        paths_matching = []
        simple_tags_filters = []

        # Iterating over all values and finding matches

        values = self.session.query(self.table_classes[PATH_TABLE].name)

        # Search in path name
        simple_tags_filters.append(self.table_classes[PATH_TABLE].name.like("%" + search + "%"))

        # Search for each tag
        for tag in tags:

            simple_tags_filters.append(getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)).like("%" + search + "%"))

        values = values.filter(or_(*simple_tags_filters)).distinct().all()
        for value in values:
            paths_matching.append(value.name)

        return paths_matching

    def get_paths_matching_advanced_search(self, links, fields, conditions,
                                           values, nots, paths_list):
        """
        Gives the paths matching the advanced search
        :param links: Links (AND/OR)
        :param fields: Fields (List of tags) (FileName for search in name column)
        :param conditions: Conditions (=, !=, <, >, <=, >=, BETWEEN,
                           CONTAINS, IN, HAS VALUE, HAS NO VALUE)
        :param values: Values (Str value for =, !=, <, >, <=, >=, HAS VALUE, HAS NO VALUE, and
                       CONTAINS/list for BETWEEN and IN)
        :param nots: Nots (Empty or NOT)
        :param paths_list: List of paths to take into account
        :return: List of path names matching all the constraints
        """

        if not isinstance(links, list) or not isinstance(fields, list) or not isinstance(conditions, list) or not isinstance(values, list) or not isinstance(nots, list) or not isinstance(paths_list, list):
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
            for tag in field:
                if tag not in fields_list:
                    return []
        for condition in conditions:
            if condition not in ["=", "!=", "<", ">", "<=", ">=", "BETWEEN",
                                 "IN", "CONTAINS", "HAS VALUE", "HAS NO VALUE"]:
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
        for not_choice in nots:
            if not_choice not in ["", "NOT"]:
                return []

        query = self.session.query(self.table_classes[PATH_TABLE].name).filter(self.table_classes[PATH_TABLE].name.in_(paths_list))
        row_filters = []
        paths_matching = []

        # For each row of condition
        for i in range(0, len(conditions)):

            row_filter = []

            # For each tag to check
            for tag in fields[i]:

                if tag == "FileName":

                    if (conditions[i] == "="):
                        row_filter.append(self.table_classes[PATH_TABLE].name == values[i])
                    elif (conditions[i] == "!="):
                        row_filter.append(self.table_classes[PATH_TABLE].name != values[i])
                    elif (conditions[i] == "<="):
                        row_filter.append(self.table_classes[PATH_TABLE].name <= values[i])
                    elif (conditions[i] == "<"):
                        row_filter.append(self.table_classes[PATH_TABLE].name < values[i])
                    elif (conditions[i] == ">="):
                        row_filter.append(self.table_classes[PATH_TABLE].name >= values[i])
                    elif (conditions[i] == ">"):
                        row_filter.append(self.table_classes[PATH_TABLE].name > values[i])
                    elif (conditions[i] == "CONTAINS"):
                        row_filter.append(self.table_classes[PATH_TABLE].name.contains(
                                values[i]))
                    elif (conditions[i] == "BETWEEN"):
                        row_filter.append(self.table_classes[PATH_TABLE].name.between(
                                values[i][0], values[i][1]))
                    elif (conditions[i] == "IN"):
                        row_filter.append(self.table_classes[PATH_TABLE].name.in_(
                                values[i]))
                    elif (conditions[i] == "HAS VALUE"):
                        row_filter.append(
                            self.table_classes[PATH_TABLE].name != None)
                    elif (conditions[i] == "HAS NO VALUE"):
                        row_filter.append(
                            self.table_classes[PATH_TABLE].name == None)

                else:

                    if (conditions[i] == "="):
                        row_filter.append(
                            and_(getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) != None,
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) == values[i]))
                    elif (conditions[i] == "!="):
                        row_filter.append(
                            or_(getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) == None,
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) != values[i]))
                    elif (conditions[i] == "<="):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) <= values[i])
                    elif (conditions[i] == "<"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) < values[i])
                    elif (conditions[i] == ">="):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) >= values[i])
                    elif (conditions[i] == ">"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)) > values[i])
                    elif (conditions[i] == "CONTAINS"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)).like("%" + str(values[i]) + "%"))
                    elif (conditions[i] == "BETWEEN"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)).between(values[i][0], values[i][1]))
                    elif (conditions[i] == "IN"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE], self.tag_name_to_column_name(tag)).in_(values[i]))
                    elif (conditions[i] == "HAS VALUE"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE],
                                         self.tag_name_to_column_name(tag) + "_current") != None)
                    elif (conditions[i] == "HAS NO VALUE"):
                        row_filter.append(
                            getattr(self.table_classes[PATH_TABLE],
                                    self.tag_name_to_column_name(tag) + "_current") == None)

            # Putting OR condition between all row filters
            if len(row_filter) > 1:
                final_row_filter = or_(*row_filter)
            else:
                final_row_filter = row_filter[0]

            # Putting the negation if needed
            if nots[i] == "NOT":
                final_row_filter = not_(final_row_filter)

            row_filters.append(final_row_filter)

        # Row filters linked
        linked_filters = row_filters[0]

        for i in range(0, len(links)):
            if links[i] == "OR":
                linked_filters = or_(linked_filters, row_filters[i + 1])
            else:
                linked_filters = and_(linked_filters, row_filters[i + 1])

        query = query.filter(linked_filters).distinct().all()

        for result in query:
            paths_matching.append(result.name)

        return paths_matching

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
            is_list = tag_row.type in LIST_TYPES

            if is_list is False:
                # The tag has a simple type, the tag column in the current
                # table is used

                couple_query_result = self.session.query(
                    self.table_classes[PATH_TABLE].name).filter(
                    getattr(self.table_classes[PATH_TABLE],
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

    def update_table_classes(self):
        """
        Redefines the model after an update of the schema
        """

        self.table_classes = {}
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(engine=self.engine)

        for table in self.metadata.tables.values():
            self.table_classes[table.name] = getattr(self.base.classes, table.name)
    
    def filter_query(self, filter):
        """
        Given a filter string, return a query that can be used with
        filter_paths() to select paths.
        """
        
        tree = filter_parser().parse(filter)
        query = FilterToQuery(self).transform(tree)
        return query

    def filter_paths(self, filter_query):
        """
        Iterate over paths selected by filter_query. Each item yieled is a
        row of the path table returned by sqlalchemy. filter_query can be
        the result of self.filter_query() or a string containing a filter
        (in this case self.fliter_query() is called to get the actual query).
        """
        
        if isinstance(filter_query, six.string_types):
            filter_query = self.filter_query(filter_query)
        if filter_query is None:
            select = self.metadata.tables[PATH_TABLE].select()
            python_filter = None
        elif isinstance(filter_query, types.FunctionType):
            select = self.metadata.tables[PATH_TABLE].select()
            python_filter = filter_query
        elif isinstance(filter_query, tuple):
            sql_condition, python_filter = filter_query
            select = select = self.metadata.tables[PATH_TABLE].select(sql_condition)
        else:
            select = select = self.metadata.tables[PATH_TABLE].select(filter_query)
            python_filter = None
        for row in self.session.execute(select):
            row = TagRow(self, row)
            if python_filter is None or python_filter(row):
                yield row

    def python_to_column(self, tag_type, value):
        """
        Convert a python value into a suitable value to put in a
        database column.
        """
        if isinstance(value, list):
            return self.list_to_column(tag_type, value)
        else:
            return value
    
    def column_to_python(self, tag_type, value):
        """
        Convert a value of a database column into the corresponding
        Python value.
        """
        if tag_type.startswith('list_'):
            return self.column_to_list(tag_type, value)
        else:
            return value
            
    def list_to_column(self, tag_type, value):
        """
        Convert a python list value into a suitable value to put in a
        database column.
        """
        converter = self._list_item_to_string.get(tag_type)
        if converter is None:
            list_value = value
        else:
            list_value = [converter(i) for i in value]
        return repr(list_value)
    
    def column_to_list(self, tag_type, value):
        """
        Convert a value of a database column into the corresponding
        Python list value.
        """
        if value is None:
            return None
        list_value = ast.literal_eval(value)
        converter = self._string_to_list_item.get(tag_type)
        if converter is None:
            return list_value
        return [converter(i) for i in list_value]
        
class Undefined:
    pass

class TagRow:
    '''
    A TagRow is an object that makes it possible to access to attributes of
    a database row returned by sqlalchemy using the tag name. If the 
    attribute with the tag name is not found, it is hashed and search in the
    actual row. If found, it is stored in the TagRow instance.
    '''
    
    def __init__(self, database, row):
        self.database = database
        self.row = row
    
    def __getattr__(self, name):
        try:
            return getattr(self.row, name)
        except AttributeError as e:
            hashed_name = hashlib.md5(name.encode('utf-8')).hexdigest()
            result = getattr(self.row, hashed_name, Undefined)
            if result is Undefined:
                raise
            result = self.database.column_to_python(self.database.tags[name].type, result)
            setattr(self, hashed_name, result)
            return result
    
    def __getitem__(self, name):
        return getattr(self, name)
