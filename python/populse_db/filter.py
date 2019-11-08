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

KEYWORD_LITERAL : "NULL"i
                | "TRUE"i
                | "FALSE"i
                  
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



#class FilterToPythonQuery(FilterToQuery):
    #@staticmethod
    #def like_to_re(like_pattern):
        #return '^%s$' % re.escape(like_pattern).replace('%', '.*').replace('_', '.')

    #@staticmethod
    #def like(value, like_pattern):
        #re_pattern = like_to_re(like_pattern)
        #return bool(re.match(pattern, value))

    #@staticmethod
    #def ilike(value, like_pattern):
        #re_pattern = like_to_re(like_pattern)
        #return bool(re.match(pattern, value), flags=re.IGNORECASE)

    #python_operators = {
        #'==': operator.eq,
        #'!=': operator.ne,
        #'<': operator.lt,
        #'<=': operator.le,
        #'>': operator.gt,
        #'>=': operator.ge,
        #'and': operator.and_,
        #'or': operator.or_,
        #'like': like,
        #'ilike': ilike,
    #}

    #def build_condition_all(self):
        #return lambda x: True

    #def build_condition_literal_in_list_field(self, value, list_field):
        #'''
        #Builds a condition checking if a constant value is in a list field
        #'''
        #return (lambda x, lf=list_field.field_name, v=value:
                #x[lf] is not None and v in x[lf])

    #def build_condition_field_in_list_field(self, field, list_field):
        #'''
        #Builds a condition checking if a field value is in another
        #list field value
        #'''
        #return (lambda x, lf=list_field.name, f=field.name:
                #x[lf] is not None and x[f] in x[lf])

    #def build_condition_field_in_list(self, field, list_value):
        #'''
        #Builds a condition checking if a field value is a
        #constant list value
        #'''
        #return (lambda x, l=list_value, f=field.name:
                #x[f] in l)

    #def build_condition_field_op_field(self, left_field, operator_str, right_field):
        #operator = self.python_operators[operator_str]
        #return (lambda x, ln=left_field.name, rn=right_field.name, o=operator:
                #x[ln] is not None and x[rn] is not None and o(x[ln], x[rn]))

    #def build_condition_field_op_value(self, field, operator_str, value):
        #operator = self.python_operators[operator_str]
        #if value is None:
            #return lambda x, f=field.name, o=operator: o(x[f], None)
        #else:
            #return (lambda x, f=field.name, v=value, o=operator:
                    #x[f] is not None and o(x[f], v))

    #def build_condition_value_op_field(self, value, operator_str, field):
        #operator = self.python_operators[operator_str]
        #if value is None:
            #return lambda x, f=field.name, o=operator: o(None, x[f])
        #else:
            #return (lambda x, f=field.name, v=value, o=operator:
                    #x[f] is not None and o(v, x[f], ))

    #def build_condition_negation(self, condition):
        #return lambda x, f=condition: not f(x)

    #def build_condition_combine_conditions(self, left_condition, operator_str, right_condition):
        #operator = self.python_operators[operator_str]
        #return lambda x, f1=left_condition, f2=right_condition, o=operator: o(f1(x), f2(x))


#class FilterToMixedQuery(FilterToSqlQuery, FilterToPythonQuery):
    #def build_condition_literal_in_list_field(self, value, list_field):
        #if self.database.list_tables:
            #return FilterToSqlQuery.build_condition_literal_in_list_field(self, value, list_field)
        #else:
            #return FilterToPythonQuery.build_condition_literal_in_list_field(self, value, list_field)

    #def build_condition_field_in_list_field(self, field, list_field):
        #if self.database.list_tables:
            #return FilterToSqlQuery.build_condition_field_in_list_field(self, field, list_field)
        #else:
            #return FilterToPythonQuery.build_condition_field_in_list_field(self, field, list_field)

    #def build_condition_negation(self, condition):
        #if isinstance(condition, types.FunctionType):
            #return FilterToPythonQuery.build_condition_negation(self, condition)
        #elif isinstance(condition, tuple):
            #raise FilterImplementationLimit('Cannot use NOT on a SQL+Python query')
        #else:
            #return FilterToSqlQuery.build_condition_negation(self, condition)

    #def build_condition_combine_conditions(self, left_condition, operator_str, right_condition):
        #if isinstance(left_condition, types.FunctionType):
            #if not isinstance(right_condition, types.FunctionType):
                #raise FilterImplementationLimit('Cannot combine a Python query with a non-Python query')
            #return FilterToPythonQuery.build_condition_combine_conditions(self, left_condition, operator_str,
                                                                          #right_condition)
        #elif isinstance(left_condition, tuple):
            #raise FilterImplementationLimit('A query combining SQL + Python cannot be combined anymore')
        #else:
            ## Current condition is a SqlAlchemy expression
            #if isinstance(right_condition, types.FunctionType):
                ## Right operand is a Python function. Such a combination
                ## is allowed once with AND operator. In that case, the
                ## result is a tuple with the SqlAlchemy expression and 
                ## the Python function condition.
                #if operator_str != 'and':
                    #raise ValueError('Combination of simple fields '
                                     #'conditions with list fields conditions is only '
                                     #'allowed with AND but not with %s' % operator_str)
                #return (left_condition, right_condition)
            #elif isinstance(right_condition, tuple):
                #raise FilterImplementationLimit('A query combining SQL + Python cannot be combined anymore')
            #else:
                #return FilterToSqlQuery.build_condition_combine_conditions(self, left_condition, operator_str,
                                                                           #right_condition)


#class FilterToGuessedQuery(FilterToMixedQuery):
    #def transform(self, *args, **kwargs):
        #try:
            #return FilterToMixedQuery.transform(self, *args, **kwargs)
        #except FilterImplementationLimit:
            #transformer = FilterToPythonQuery(self.database, self.collection)
            #return transformer.transform(*args, **kwargs)


## Query_types
#QUERY_SQL = "sql"
#QUERY_PYTHON = "python"
#QUERY_MIXED = "mixed"
#QUERY_GUESS = "guess"

#_filter_to_query_classes = {
    #QUERY_SQL: FilterToSqlQuery,
    #QUERY_PYTHON: FilterToPythonQuery,
    #QUERY_MIXED: FilterToMixedQuery,
    #QUERY_GUESS: FilterToGuessedQuery,
#}
