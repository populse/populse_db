import six
import operator
import types
import ast
import datetime

from lark import Lark, Transformer

import dateutil.parser

import sqlalchemy
from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.sql.operators import ilike_op, like_op

from populse_db.database_model import (FIELD_TYPE_INTEGER,
                                       FIELD_TYPE_FLOAT, FIELD_TYPE_TIME,
                                       FIELD_TYPE_DATETIME, FIELD_TYPE_DATE,
                                       FIELD_TYPE_STRING,
                                       FIELD_TYPE_BOOLEAN)

# The grammar (in Lark format) used to parse filter strings
filter_grammar = '''
?start : filter

?filter : "ALL"i                         -> all
        | conditions
        | negation
        | "(" filter ")"
        | filter BOOLEAN_OPERATOR filter -> conditions

?conditions : condition (BOOLEAN_OPERATOR condition)*

                   
negation : "NOT"i condition
         | "NOT"i "(" filter ")"

BOOLEAN_OPERATOR : "AND"i
                 | "OR"i

CONDITION_OPERATOR : "=="i
                   | "!="i
                   | "<="i
                   | ">="i
                   | ">"i
                   | "<"i
                   | "IN"i
                   | "ILIKE"i
                   | "LIKE"i

condition : operand CONDITION_OPERATOR operand

?operand : literal
         | field_name

field_name.1 : FIELD_NAME
             | quoted_field_name

quoted_field_name.1 : QUOTED_FIELD_NAME


         
?literal.2 : KEYWORD_LITERAL -> keyword_literal
         | ESCAPED_STRING   -> string
         | SIGNED_NUMBER    -> number
         | DATE             -> date
         | TIME             -> time
         | DATETIME         -> datetime
         | list


DATE : INT "-" INT "-" INT
TIME : INT ":" INT (":" INT ("." INT)?)?
DATETIME : DATE "T" TIME

KEYWORD_LITERAL.2 : "NULL"i
                  | "TRUE"i
                  | "FALSE"i
                  
FIELD_NAME.1 : ("_"|LETTER) ("_"|LETTER|DIGIT)*
QUOTED_FIELD_NAME.1 : "{" /[^}]/* "}"

list : "[" [literal ("," literal)*] "]"

%import common.INT
%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.LETTER
%import common.DIGIT

%import common.WS
%ignore WS
'''

# The instance of the grammar parser is created only once
# then stored in _grammar_parser for later reuse
_grammar_parser = None

def filter_parser():
    '''
    Return a singleton instance of Lark grammar parser for filter expression
    '''
    global _grammar_parser
    if _grammar_parser is None:
        _grammar_parser = Lark(filter_grammar)
    return _grammar_parser

def literal_parser():
    '''
    Return an instance of Lark grammar parser for parsing only a literal
    value (int, string, list, date, etc.) from a filter expression. This
    is used for testing the parsing of literals.
    '''
    return Lark(filter_grammar, start='literal')


class FilterToQuery(Transformer):
    '''
    Transform the parsing of a filter expression into object(s) that can
    be used ot select items from a database. The produced object is one
    of the three following items :
    - An SqlAlchemy expression (that can be passed to select()) if the filter
      can be fully expressed with SQL in all SqlAlchemy engines.
    - Python function taking a path row from SqlAlchemy and returning True if
      the row is selected by the filter or False otherwise.
    - A tuple containing an SqlAlchemy expression and a boolean Python
      function if the filter can be expressed as a AND combination of these
      two elements.
    '''
    
    invalidCombinationMessage = ('Invalid combination of conditions on simple '
                                 'fields and on list fields')
    
    python_operators = {
        '==': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
        'and': operator.and_,
        'or': operator.or_,
        'ilike': ilike_op,
        'like': like_op,
    }
    
    keyword_literals = {
        'true': True,
        'false': False,
        'null': None,
    }
    
    python_type_to_tag_type = {
        type(None): None, 
        type(''): FIELD_TYPE_STRING,
        type(u''): FIELD_TYPE_STRING,
        int: FIELD_TYPE_INTEGER,
        float: FIELD_TYPE_FLOAT,
        datetime.time: FIELD_TYPE_TIME,
        datetime.datetime: FIELD_TYPE_DATETIME,
        datetime.date: FIELD_TYPE_DATE,
        bool: FIELD_TYPE_BOOLEAN,
    }
    def __init__(self, database, collection):
        super(FilterToQuery, self).__init__()
        self.database = database
        self.collection = collection
   
    @staticmethod
    def is_column(object):
        '''
        Check if an object is an SqlAlchemy column object
        '''
        return isinstance(object, AutomapBase)
    
    @staticmethod
    def is_list_field(field):
        return (isinstance(field, AutomapBase) and
                field.type.startswith('list_'))
    
    def all(self, items):
        return sqlalchemy.text('1')
    
    def conditions(self, items):
        stack = list(items)
        result = stack.pop(0)
        while stack:
            operator_str = stack.pop(0).lower()
            operator = self.python_operators[operator_str]
            right_operand = stack.pop(0)
            if isinstance(result, types.FunctionType):
                # Current condition is a Python function
                if not isinstance(right_operand, types.FunctionType):
                    # A Python condition can only be combined with another
                    # Python condition
                    raise ValueError(self.invalidCombinationMessage)
                result = lambda x, f1=result, f2=right_operand: operator(f1(x), f2(x))
            elif isinstance(result, tuple):
                # A combination of SQL + Python cannot be combined anymore
                raise ValueError(self.invalidCombinationMessage)
            else:
                # Current condition is a SqlAlchemy expression
                if isinstance(right_operand, types.FunctionType):
                    # Right operand is a Python function. Such a combination
                    # is allowed once with AND operator. In that case, the
                    # result is a tuple with the SqlAlchemy expression and 
                    # the Python function condition.
                    if operator_str != 'and':
                        raise ValueError('Combination of simple fields '
                            'conditions with list fields conditions is only '
                            'allowed with AND but not with %s' % operator_str)
                    result = (result, right_operand)
                elif isinstance(right_operand, tuple):
                    # A combination of SQL + Python cannot be combined anymore
                    raise ValueError(self.invalidCombinationMessage)
                else:
                    # Combine two SqlAlchemy expressions
                    result = operator(result, right_operand)
        return result
    
    def get_column(self, column):
        '''
        Return the SqlAlchemy Column object corresponding to
        a populse_db field object.
        '''
        return getattr(self.database.metadata.tables[self.collection].c,
                       self.database.field_name_to_column_name(self.collection, column.name))
    
    def get_column_value(self, python_value):
        '''
        Convert a Python value to a value suitable to put in a database column
        '''
        tag_type = self.find_field_type(python_value)
        column_value = self.database.python_to_column(tag_type, python_value)
        return column_value
    
    
    def find_field_type(self, value):
        if isinstance(value, list):
            if value:
                item_type = self.find_field_type(value[0])
                return 'list_' + item_type
            else:
                return type(None)
        else:
            return self.python_type_to_tag_type[type(value)]
    
    def condition(self, items):
        left_operand, operator, right_operand = items
        operator_str = str(operator).lower()
        if operator_str == 'in':

            if self.is_list_field(right_operand):
                if not isinstance(left_operand, six.string_types + (int, 
                                                                    float,
                                                                    bool,
                                                                    None.__class__,
                                                                    datetime.date,
                                                                    datetime.time,
                                                                    datetime.datetime)):
                    raise ValueError('Left operand of IN <list field> must be a '
                        'string, number, boolean, date, time or null but "%s" '
                        'was used' % str(left_operand))
                # Check if a single value is in a list tag

                # Cannot be done in SQL with SQLite => return a Python function
                return lambda x: x[right_operand.name] is not None and left_operand in x[right_operand.name]
            elif isinstance(right_operand, list):
                if self.is_column(left_operand):
                    # Check if a simple field value is in a list of values
                    # Can be done in SQL => return an SqlAlchemy expression
                    column = self.get_column(left_operand)
                    if None in right_operand:
                        right_operand.remove(None)
                        result = (column == None) | column.in_(right_operand)  
                    else:
                        result = column.in_(right_operand)
                    return result
                else:

                    raise ValueError('Left operand of IN <list> must be a '
                        'simple field but "%s" was used' % str(left_operand))
            else:
                raise ValueError('Right operand of IN must be a list or a '
                    'list field but "%s" was used' % str(right_operand))

        # Check if using an SQL expression is possible
        # This is always possible for operator == and != (using string
        # comparison for lists). Otherwise, it is only possible if no
        # list column is involved in the expression.
        do_sql = (operator_str in ('==', '!=')
                  or
                  (not self.is_list_field(left_operand)
                   and not self.is_list_field(right_operand)))
        operator = self.python_operators[operator_str]
        if do_sql:

            if self.is_column(left_operand):
                left_operand = self.get_column(left_operand)
            else:
                left_operand = self.get_column_value(left_operand)
            if self.is_column(right_operand):

                right_operand = self.get_column(right_operand)
            else:
                right_operand = self.get_column_value(right_operand)
            # Return SqlAlchemy expression of the condition
            return operator(left_operand, right_operand)
        
        # SQL is not possible : build and return a Python function for the
        # condition.
        if self.is_column(left_operand):
            if self.is_column(right_operand):
                python = lambda x: operator(x[left_operand.name],
                                            x[right_operand.name])
            else:
                python = lambda x: operator(x[left_operand.name],
                                            right_operand)
        else:
            if self.is_column(right_operand):
                python = lambda x: operator(left_operand,
                                            x[right_operand.name])
            else:
                raise ValueError('Either left or right operand of a condition'
                                 ' must be a field name')
        return python
    
    def negation(self, items):
        condition = items[0]
        if isinstance(condition, types.FunctionType):
            # Current condition is a Python function
            return lambda x, f=condition: not f(x)
        elif isinstance(condition, tuple):
            # A combination of SQL + Python cannot be combined anymore
            raise ValueError(self.invalidCombinationMessage)
        else:
          return ~ condition
    
    def string(self, items):
        return ast.literal_eval(items[0].replace('\n','\\n'))
    
    def number(self, items):
        return float(items[0])

    def date(self, items):
        return dateutil.parser.parse(items[0]).date()
    
    def time(self, items):
        return dateutil.parser.parse(items[0]).time()
    
    def datetime(self, items):
        return dateutil.parser.parse(items[0])
    
    def keyword_literal(self, items):
        return self.keyword_literals[items[0].lower()]

    def list(self, items):
        return items

    def field_name(self, items):
        field = items[0]
        # Check for literal due to a bug in Lark
        literal = self.keyword_literals.get(field.lower(), self)
        if literal is not self:
            return literal
        column = self.database.get_field(self.collection, field)
        if column is None:
            raise ValueError('No field named "%s"' % field)
        return column

    def quoted_field_name(self, items):
        return items[0][1:-1]
