import ast
import datetime

import dateutil.parser
from lark import Lark, Transformer

# The grammar (in Lark format) used to parse filter strings:
filter_grammar = """
?start : filter

?filter : conditions
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

condition : "ALL"i                         -> all
          | operand CONDITION_OPERATOR operand

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
           | empty_list

DATE.2 : INT "-" INT "-" INT
TIME.2 : INT ":" INT (":" INT ("." INT)?)?
DATETIME.2 : DATE "T" TIME

KEYWORD_LITERAL : "NuLL"i
                | "TRUE"i
                | "FaLSE"i

FIELD_NAME : ("_"|LETTER) ("_"|LETTER|DIGIT)*
QUOTED_FIELD_NAME : "{" /[^}]/* "}"

empty_list : "[]"
list : "[" literal ("," literal)* "]"

_STRING_INNER: /(.|\\n)*?/
_STRING_ESC_INNER: _STRING_INNER /(?<!\\\\)(\\\\\\\\)*?/
ESCAPED_STRING : "\\"" _STRING_ESC_INNER "\\""



%import common.INT
%import common.SIGNED_NUMBER
%import common.LETTER
%import common.DIGIT

%import common.WS
%ignore WS
"""

# The instance of the grammar parser is created only once
# then stored in _grammar_parser for later reuse
_grammar_parser = None


def filter_parser():
    """
    :return: A singleton instance of Lark grammar parser for filter expression
    """
    global _grammar_parser
    if _grammar_parser is None:
        _grammar_parser = Lark(filter_grammar, parser="lalr")
    return _grammar_parser


def literal_parser():
    """
    This is used to test literals parsing

    :return: An instance of Lark grammar parser for parsing only a literal
       value (int, string, list, date, etc.) from a filter expression. This
       is used for testing the parsing of these literals.
    """
    return Lark(filter_grammar, parser="lalr", start="literal")


def _list_to_sql(value):
    s = ",".join(f'"{x}"' if isinstance(x, str) else to_sql(x) for x in value)
    return f"'[{s}]'"


_type_to_sql = {
    type(None): lambda x: "NULL",
    str: lambda x: f"'{x}'",
    int: str,
    float: str,
    # datetime.time: lambda x: f"'{x.isoformat()}'",
    # datetime.datetime: lambda x: f"'{x.isoformat()}'",
    # datetime.date: lambda x: f"'{x.isoformat()}'",
    bool: lambda x: ("1" if x else "0"),
    list: _list_to_sql,
}


def to_sql(value):
    global _type_to_sql

    return _type_to_sql[type(value)](value)


class Field(str):
    pass


class FilterToSQL(Transformer):
    """
    Instance of this class are passed to Lark parser when parsing a document
    selection filter string in order to create an object that can be used to
    select items from the database. FilterToSQL implements methods that are
    common to all engines and does not produce anything because the query
    objectis specific to each engine. Therefore, engine class must use a
    subclass of FilterToSQL that implements the following methods:

        build_condition_all
        build_condition_literal_in_list_field
        build_condition_field_in_list_field
        build_condition_field_in_list
        build_condition_field_op_field
        build_condition_value_op_field
        build_condition_negation
        build_condition_combine_conditions
    """

    keyword_literals = {
        "true": True,
        "false": False,
        "null": None,
    }

    sql_operators = {
        "==": "IS",
        "!=": "IS NOT",
        "ilike": "LIKE",
    }

    no_list_operators = {">", "<", ">=", "<=", "like", "ilike"}

    def __init__(self, dbcollection):
        self.dbcollection = dbcollection

    def all(self, items):
        return self.build_condition_all()

    def condition(self, items):
        left_operand, operator, right_operand = items
        operator_str = str(operator).lower()
        if operator_str == "in":
            if isinstance(right_operand, Field):
                if left_operand is None or isinstance(
                    left_operand,
                    (
                        str
                        | int
                        | float
                        | bool
                        | datetime.date
                        | datetime.time
                        | datetime.datetime
                    ),
                ):
                    return self.build_condition_literal_in_list_field(
                        left_operand, right_operand
                    )
                elif isinstance(left_operand, Field):
                    return self.build_condition_field_in_list_field(
                        left_operand, right_operand
                    )
                else:
                    raise ValueError(
                        "Left operand of IN <list field> must be a "
                        f'field, string, number, boolean, date, time or null but "{left_operand}" '
                        "was used"
                    )
            elif isinstance(right_operand, list):
                if isinstance(left_operand, Field):
                    return self.build_condition_field_in_list(
                        left_operand, right_operand
                    )
                else:
                    raise ValueError(
                        "Left operand of IN <list> must be a "
                        f'simple field but "{left_operand}" was used'
                    )
            else:
                raise ValueError(
                    "Right operand of IN must be a list or a "
                    f'list field but "{right_operand}" was used'
                )

        if isinstance(left_operand, Field):
            if isinstance(right_operand, Field):
                return self.build_condition_field_op_field(
                    left_operand, operator_str, right_operand
                )
            else:
                return self.build_condition_field_op_value(
                    left_operand, operator_str, right_operand
                )
        else:
            if isinstance(right_operand, Field):
                return self.build_condition_value_op_field(
                    left_operand, operator_str, right_operand
                )
            else:
                raise ValueError(
                    "Either left or right operand of a condition must be a field name"
                )

    def negation(self, items):
        return self.build_condition_negation(items[0])

    def conditions(self, items):
        stack = list(items)
        result = stack.pop(0)
        while stack:
            operator_str = stack.pop(0).lower()
            right_operand = stack.pop(0)
            if result is None:
                if operator_str == "and":
                    result = right_operand
                    continue
                elif operator_str == "or":
                    result = None
                    continue
                left_operand = ["1"]
            else:
                left_operand = result
            if right_operand is None:
                if operator_str == "and":
                    result = left_operand
                    continue
                elif operator_str == "or":
                    result = None
                    continue
                right_operand = ["1"]
            result = self.build_condition_combine_conditions(
                result, operator_str, right_operand
            )
        return result

    def string(self, items):
        return ast.literal_eval(items[0].replace("\n", "\\n"))

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

    def empty_list(self, items):
        return []

    def list(self, items):
        return items

    def field_name(self, items):
        field = items[0]
        # Checks for literal due to a bug in Lark
        literal = self.keyword_literals.get(field.lower(), self)
        if literal is not self:
            return literal
        if field in self.dbcollection.fields:
            return Field(f"[{field}]")
        elif self.dbcollection.catchall_column:
            return Field(
                f"json_extract([{self.dbcollection.catchall_column}],'$.\"{field}\"')"
            )
        else:
            raise ValueError(
                f'Filter uses unknown field "{field}" in collection "{self.dbcollection.name}" that does not support it'
            )

    def quoted_field_name(self, items):
        return Field(items[0][1:-1])

    def build_condition_all(self):
        """
        Return a selection query that select all documents. This query
        is directly given to the engine and never combined with other
        queries.
        """
        return None

    def build_condition_literal_in_list_field(self, value, list_field):
        """
        Builds a condition checking if a constant value is in a list field

        :param value: Python literal

        :param list_field: field object as returned by Database.get_field

        """
        return [
            f"{list_field} IS NOT NULL AND "
            f"{to_sql(value)} IN (SELECT value FROM json_each({list_field}))"
        ]

    def build_condition_field_in_list_field(self, field, list_field):
        """
        Builds a condition checking if a field value is in another
        list field value

        :param field: field object as returned by Database.get_field
        :param list_field: field object as returned by Database.get_field
        """
        return [
            f"{list_field} IS NOT NULL AND "
            f"{field} IN (SELECT value FROM json_each({list_field}))"
        ]

    def build_condition_field_in_list(self, field, list_value):
        """
        Builds a condition checking if a field value is a
        constant list value

        :param field: field object as returned by Database.get_field
        :param list_value: Python list containing literals
        """
        if None in list_value:
            list_value.remove(None)
            where = [f"{field} IS NULL OR {field} "]
        else:
            where = [f"{field} "]
        if not list_value:
            return ["0"]
        elif len(list_value) == 1:
            where.append(f"IS {to_sql(list_value[0])}")
        else:
            where.append(f"IN ({','.join(to_sql(i) for i in list_value)})")
        return where

    def build_condition_field_op_field(self, left_field, operator_str, right_field):
        """
        Builds a condition comparing the content of two fields with an operator.

        :param left_field: field object as returned by Database.get_field
        :param operator: string containing one of the CONDITION_OPERATOR
                         defined in the grammar (in lowercase)
        :param right_field: field object as returned by Database.get_field
        """
        sql_operator = self.sql_operators.get(operator_str, operator_str)
        if operator_str == "ilike":
            return [f"UPPER({left_field}) {sql_operator} UPPER({right_field})"]
        else:
            return [f"{left_field} {sql_operator} {right_field}"]

    def build_condition_field_op_value(self, field, operator_str, value):
        """
        Builds a condition comparing the content of a field with a constant
        value using an operator.

        :param field: field object as returned by Database.get_field
        :param operator_str: string containing one of the CONDITION_OPERATOR
                             defined in the grammar (in lowercase)
        :param value: Python value (None, string number, boolean or date/time)
        """
        if isinstance(value, list):
            if operator_str in self.no_list_operators:
                raise ValueError(
                    f"operator {operator_str} cannot be used with value of list type"
                )
        if operator_str == "ilike":
            field = f"UPPER({field})"
            if isinstance(value, str):
                value = value.upper()
        else:
            field = f"{field}"
        sql_operator = self.sql_operators.get(operator_str, operator_str)
        return [f"{field} {sql_operator} {to_sql(value)}"]

    def build_condition_value_op_field(self, value, operator_str, field):
        """
        Builds a condition comparing a constant value with the content of a
        field withusing an operator.

        :param value: Python value (None, string number, boolean or date/time)
        :param operator_str: string containing one of the CONDITION_OPERATOR
                             defined in the grammar (in lowercase)
        :param field: field object as returned by Database.get_field
        """
        if isinstance(value, list):
            if operator_str in self.no_list_operators:
                raise ValueError(
                    f"operator {operator_str} cannot be used with value of list type"
                )
        if operator_str == "ilike":
            field = f"UPPER({field})"
            if isinstance(value, str):
                value = value.upper()
        sql_operator = self.sql_operators.get(operator_str, operator_str)
        return [f"{to_sql(value)} {sql_operator} {field}"]

    def build_condition_negation(self, condition):
        """
        Builds a condition inverting another condition.

        :param condition: condition object returned by one of the
                          build_condition_*() method (except
                          build_condition_all)
        """
        if condition is None:
            return ["0"]
        return ["NOT", "("] + condition + [")"]

    def build_condition_combine_conditions(
        self, left_condition, operator_str, right_condition
    ):
        """
        Builds a condition that combines two conditions with an operator.

        :param left_condition: condition object returned by one of the
                               build_condition_*() method (except
                               build_condition_all)
        :param operator_str: string containing one of the BOOLEAN_OPERATOR
                             defined in the grammar (in lowercase)
        :param right_condition: condition object returned by one of the
                                build_condition_*() method (except
                                build_condition_all)
        """
        return (
            ["("] + left_condition + [")", operator_str, "("] + right_condition + [")"]
        )
