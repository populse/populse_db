import six
import operator
import types

from lark import Lark, Transformer
import sqlalchemy
from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.sql.elements import TextClause
from populse_db.database_model import PATH_TABLE

# The grammar (in Lark format) used to parse filter strings
filter_grammar = '''
?filter : "ALL"                         -> all
        | conditions
        | negation
        | "(" filter ")"
        | filter BOOLEAN_OPERATOR filter -> conditions

?conditions : condition (BOOLEAN_OPERATOR condition)*

                   
negation : "NOT" condition
         | "NOT" "(" filter ")"

BOOLEAN_OPERATOR : "AND"
                 | "OR"

CONDITION_OPERATOR : "=="
                   | "!="
                   | "<"
                   | "<="
                   | ">"
                   | ">="
                   | "IN"

condition : operand CONDITION_OPERATOR operand
?operand : litteral
         | list
         | tag_name

         
litteral : ESCAPED_STRING   -> string
         | SIGNED_NUMBER    -> number
         | KEYWORD_LITTERAL -> keyword_litteral
         | DATE             -> date
         | TIME             -> time
         | DATETIME         -> datetime


DATE : INT "-" INT "-" INT
TIME : INT ":" INT (":" INT ("." INT)?)?
DATETIME : DATE "T" TIME

KEYWORD_LITTERAL : "TRUE"
                 | "FALSE"
                 | "NULL"

list : "[" [litteral ("," litteral)*] "]"

tag_name : CNAME

%import common.INT
%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.CNAME

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
        _grammar_parser = Lark(filter_grammar, start='filter')
    return _grammar_parser


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
                                 'tags and on list tags')
    
    python_operators = {
        '==': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
        'AND': operator.and_,
        'OR': operator.or_,
    }
    
    def __init__(self, database):
        super(FilterToQuery, self).__init__()
        self.database = database
   
    @staticmethod
    def is_tag(object):
        return isinstance(object, AutomapBase)
    
    @staticmethod
    def is_list_tag(tag):
        return (isinstance(tag, AutomapBase) and
                tag.type.startswith('list_'))
    
    def all(self, items):
        return sqlalchemy.text('1')
    
    def conditions(self, items):
        stack = list(items)
        result = stack.pop(0)
        while stack:
            operator_str = stack.pop(0)
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
                    if operator_str != 'AND':
                        raise ValueError('Combination of simple tags '
                            'conditions with list tags conditions is only '
                            'allowed with AND but not with %s' % operator_str)
                    result = (result, right_operand)
                elif isinstance(right_operand, tuple):
                    # A combination of SQL + Python cannot be combined anymore
                    raise ValueError(self.invalidCombinationMessage)
                else:
                    # Combine two SqlAlchemy expressions
                    result = operator(result, right_operand)
        return result
    
    def get_column(self, tag):
        '''
        Return the SqlAlchemy Column object corresponding to
        a populse_db tag object.
        '''
        return getattr(self.database.metadata.tables[PATH_TABLE].c, 
                       self.database.tag_name_to_column_name(tag.name)) 
    
    def condition(self, items):
        left_operand, operator, right_operand = items
        operator_str = str(operator)
        if operator_str == 'IN':
            if self.is_list_tag(right_operand):
                if not isinstance(left_operand, six.string_types + (float,)): #TODO date, datetime, bool, none
                    raise ValueError('Left operand of IN <list tag> must be a string or a number tag but "%s" was used' % str(left_operand))
                # Check if a single value is in a list tag
                # Cannot be done in SQL with SQLite => return a Python function
                return lambda x: left_operand in x[right_operand.name]
            elif isinstance(right_operand, list):
                if self.is_tag(left_operand):
                    # Check if a simple tag value is in a list of values
                    # Can be done in SQL => return an SqlAlchemy expression
                    return self.get_column(left_operand).in_(right_operand)
                else:
                    raise ValueError('Left operand of IN <list> must be a simple tag but "%s" was used' % str(left_operand))
            else:
                raise ValueError('Right operand of IN must be a list or a list tag but "%s" was used' % str(right_operand))
        
        # Check if using an SQL expression is possible
        # This is always possible for operator == and != (using string
        # comparison for lists). Otherwise, it is only possible if no
        # list tag is involved in the expression.
        do_sql = (operator_str in ('==', '!=') 
                  or 
                  (not self.is_list_tag(left_operand) 
                   and not self.is_list_tag(right_operand)))
        operator = self.python_operators[operator_str]
        if do_sql:
            if self.is_tag(left_operand):
                left_operand = self.get_column(left_operand) 
            if self.is_tag(right_operand):
                right_operand = self.get_column(right_operand)
            # Return SqlAlchemy expression of the condition
            return operator(left_operand, right_operand)
        
        # SQL is not possible : build and return a Python function for the
        # condition.
        if self.is_tag(left_operand):
            if self.is_tag(right_operand):
                python = lambda x: operator(x[left_operand.name],
                                            x[right_operand.name])
            else:
                python = lambda x: operator(x[left_operand.name],
                                            right_operand)
        else:
            if self.is_tag(right_operand):
                python = lambda x: operator(left_operand,
                                            x[right_operand.name])
            else:
                raise ValueError('Either left or right operand of a condition'
                                 ' must be a tag name')
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
        return items[0][1:-1]
    
    def number(self, items):
        return float(items[0])

    def tag_name(self, items):
        tag_name = items[0]
        tag = self.database.get_tag(tag_name)
        if tag is None:
            raise ValueError('No tag named "%s"' % tag_name)
        return tag
