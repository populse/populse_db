##########################################################################
# Populse_db - Copyright (C) IRMaGe/CEA, 2018
# Distributed under the terms of the CeCILL-B license, as published by
# the CEA-CNRS-INRIA. Refer to the LICENSE file or to
# http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
# for details.
##########################################################################

import ast
import datetime
import operator
import types

import dateutil.parser
import six
import sqlalchemy
import sqlalchemy.sql.operators as sql_operators
from lark import Lark, Transformer

import populse_db
from populse_db.database import Row

# The grammar (in Lark format) used to parse filter strings:
filter_grammar = '''
?start : filter

?filter : "ALL"i                         -> all
        | conditions
        | negation
        | "(" filter ")"
        | "(" filter ")" BOOLEAN_OPERATOR filter -> conditions

?conditions : condition (BOOLEAN_OPERATOR filter)*

                   
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
           | DATE            -> date
           | DATETIME        -> datetime
           | TIME            -> time
           | ESCAPED_STRING  -> string
           | SIGNED_NUMBER   -> number
           | list

DATE.2 : INT "-" INT "-" INT
TIME.2 : INT ":" INT (":" INT ("." INT)?)?
DATETIME.2 : DATE "T" TIME

KEYWORD_LITERAL : "NuLL"i
                | "TRUE"i
                | "FaLSE"i
                  
FIELD_NAME : ("_"|LETTER) ("_"|LETTER|DIGIT)*
QUOTED_FIELD_NAME : "{" /[^}]/* "}"

list : "[" [literal ("," literal)*] "]"

_STRING_INNER: /(.|\\n)*?/
_STRING_ESC_INNER: _STRING_INNER /(?<!\\\\)(\\\\\\\\)*?/
ESCAPED_STRING : "\\"" _STRING_ESC_INNER "\\""



%import common.INT
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
    :return: A singleton instance of Lark grammar parser for filter expression
    '''
    global _grammar_parser
    if _grammar_parser is None:
        _grammar_parser = Lark(filter_grammar, parser='lalr')
    return _grammar_parser


def literal_parser():
    '''
    This is used to test literals parsing

    :return: An instance of Lark grammar parser for parsing only a literal value (int, string, list, date, etc.) from a filter expression.

    '''
    return Lark(filter_grammar, parser='lalr', start='literal')


class FilterImplementationLimit(NotImplementedError):
    '''
    This exception is raised when a valid filter cannot
    be converted to a query for a specific implementation
    (for instance some list comparison operators cannot be
    used in SQL)
    '''


class FilterToQuery(Transformer):
    '''
    Transforms the parsing of a filter expression into object(s) that can
    be used ot select items from a database.

    The produced object is one of the three following items :

    - An SqlAlchemy expression (that can be passed to select()) if the filter
      can be fully expressed with SQL in all SqlAlchemy engines.

    - Python function taking a path row from SqlAlchemy and returning True if
      the row is selected by the filter or False otherwise.

    - A tuple containing an SqlAlchemy expression and a boolean Python
      function if the filter can be expressed as a AND combination of these
      two elements.
    '''

    keyword_literals = {
        'true': True,
        'false': False,
        'null': None,
    }

    def __init__(self, engine, collection):
        self.engine = engine
        self.collection = collection

    @staticmethod
    def is_field(object):
        '''
        Checks if an object is an SqlAlchemy column object
        '''
        return isinstance(object, Row)

    @staticmethod
    def is_list_field(field):
        return (isinstance(field, Row) and
                field.field_type.startswith('list_'))

    def all(self, items):
        return self.build_condition_all()

    def condition(self, items):
        left_operand, operator, right_operand = items
        operator_str = str(operator).lower()
        if operator_str == 'in':

            if self.is_list_field(right_operand):

                if isinstance(left_operand, six.string_types + (int,
                                                                float,
                                                                bool,
                                                                None.__class__,
                                                                datetime.date,
                                                                datetime.time,
                                                                datetime.datetime)):
                    return self.build_condition_literal_in_list_field(left_operand, right_operand)
                elif self.is_field(left_operand):
                    if self.is_list_field(left_operand):
                        raise ValueError('Cannot use operator IN with two list fields')
                    return self.build_condition_field_in_list_field(left_operand, right_operand)
                else:
                    raise ValueError('Left operand of IN <list field> must be a '
                                     'field, string, number, boolean, date, time or null but "%s" '
                                     'was used' % str(left_operand))
            elif isinstance(right_operand, list):
                if self.is_field(left_operand):
                    return self.build_condition_field_in_list(left_operand, right_operand)
                else:
                    raise ValueError('Left operand of IN <list> must be a '
                                     'simple field but "%s" was used' % str(left_operand))
            else:
                raise ValueError('Right operand of IN must be a list or a '
                                 'list field but "%s" was used' % str(right_operand))

        if self.is_field(left_operand):
            if self.is_field(right_operand):
                return self.build_condition_field_op_field(left_operand, operator_str, right_operand)
            else:
                return self.build_condition_field_op_value(left_operand, operator_str, right_operand)
        else:
            if self.is_field(right_operand):
                return self.build_condition_value_op_field(left_operand, operator_str, right_operand)
            else:
                raise ValueError('Either left or right operand of a condition'
                                 ' must be a field name')

    def negation(self, items):
        return self.build_condition_negation(items[0])

    def conditions(self, items):
        stack = list(items)
        result = stack.pop(0)
        while stack:
            operator_str = stack.pop(0).lower()
            right_operand = stack.pop(0)
            result = self.build_condition_combine_conditions(result, operator_str, right_operand)
        return result

    def string(self, items):
        return ast.literal_eval(items[0].replace('\n', '\\n'))

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
        # Checks for literal due to a bug in Lark
        literal = self.keyword_literals.get(field.lower(), self)
        if literal is not self:
            return literal
        field = self.engine.field(self.collection, field)
        if field is None:
            raise ValueError('No field named "%s"' % field)
        return field

    def quoted_field_name(self, items):
        return items[0][1:-1]
