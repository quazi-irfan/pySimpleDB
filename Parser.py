import string
from RelationalOp import *
import logging
db_logger = logging.getLogger('SimpleDB')

# SimpleDB support five types of tokens
# Single Char delimiters such as comma, period(because table.col needs to be interpreted as three tokens, table, period and column; SimpleDB doesn't do it)
# Integer constants such as 123
# String constants such as 'joe'
# Keywords, such as select, from and where
# Identifiers such as Student, X and glop_34a
class Tokenizer:
    EOF = -1
    DELIMITER = 0
    IntConstant = 1
    StringConstant = 2
    Keyword = 3
    Id = 4
    KEYWORD_LIST = ("select", "from", "where", "and", "insert", "into", "values", "delete", "update",
                "set", "create", "table", "int", "varchar", "view", "as", "index", "on")

    def __init__(self, input):
        self.input = input
        self.pos = 0
        self._tokenIndex = 0

    # Keywords and identifiers are case-insensitive
    def nextToken(self):
        if self.pos == len(self.input):
            return Tokenizer.EOF, Tokenizer.EOF

        while self.input[self.pos] in string.whitespace:
            self.pos += 1
            continue

        start = self.pos
        token_type = None
        token_value = None

        if self.pos < len(self.input) and self.input[self.pos] in string.ascii_letters:
            while self.pos < len(self.input) and self.input[self.pos] in set(string.ascii_letters + string.digits + '_'):
                self.pos += 1
            if self.input[start:self.pos].lower() in Tokenizer.KEYWORD_LIST:
                token_type, token_value = Tokenizer.Keyword, self.input[start:self.pos].lower()
            else:
                token_type, token_value = Tokenizer.Id, self.input[start:self.pos]

        elif self.pos < len(self.input) and self.input[self.pos] == "'":
            self.pos += 1 # skip the first quote since that is not part of the string
            start = self.pos # also move the start cursor
            while self.pos < len(self.input) and self.input[self.pos] != "'":
                self.pos += 1
            token_type, token_value = Tokenizer.StringConstant, self.input[start:self.pos]
            self.pos += 1 # also ignore the final quote

        elif self.pos < len(self.input) and self.input[self.pos] in string.digits:
            while self.pos < len(self.input) and self.input[self.pos] in string.digits:
                self.pos += 1
            token_type, token_value = Tokenizer.IntConstant, int(self.input[start:self.pos])

        elif self.pos < len(self.input) and self.input[self.pos] in (',', '.', '=', '(', ')'):
            self.pos += 1
            token_type, token_value = Tokenizer.DELIMITER, self.input[start:self.pos]

        self._tokenIndex += 1
        return token_type, token_value

class Lexer:
    def __init__(self, input_query):
        self.tokenizer = Tokenizer(input_query)
        self.token_type, self.token_value = self.tokenizer.nextToken()

    def matchDelim(self, target_delim):
        return self.token_value == target_delim

    def matchIntConstant(self):
        return self.token_type == Tokenizer.IntConstant

    def matchStringConstant(self):
        return self.token_type == Tokenizer.StringConstant

    def matchKeyword(self, target_keyword):
        return self.token_type == Tokenizer.Keyword and self.token_value == target_keyword

    def matchId(self):
        return self.token_type == Tokenizer.Id and self.token_value not in Tokenizer.KEYWORD_LIST


    def eatDelim(self, target_delim):
        if not self.matchDelim(target_delim):
            raise Exception("Expecting delimiter " + str(target_delim))
        self.token_type, self.token_value = self.tokenizer.nextToken()
        # delimiters are consumed

    def eatIntConstant(self):
        if not self.matchIntConstant():
            raise Exception("Expecting Int Constant")
        temp = self.token_value
        self.token_type, self.token_value = self.tokenizer.nextToken()
        return temp

    def eatStringConstant(self):
        if not self.matchStringConstant():
            raise Exception("Expecting String constant")
        temp = self.token_value
        self.token_type, self.token_value = self.tokenizer.nextToken()
        return temp

    def eatKeyword(self, target_keyword):
        if not self.matchKeyword(target_keyword):
            raise Exception("Expecting SQL keyword")
        temp = self.token_value
        self.token_type, self.token_value = self.tokenizer.nextToken()
        return temp

    def eatId(self):
        if not self.matchId():
            raise Exception("Expecting SQL Identifier")
        temp = self.token_value
        self.token_type, self.token_value = self.tokenizer.nextToken()
        return temp


class Parser:
    def __init__(self, input_query):
        self.lex: Lexer = Lexer(input_query)

    def field(self):
        return self.lex.eatId()

    def constant(self):
        # TODO: internally string and int are handled no differently
        if self.lex.matchStringConstant():
            return Constant(self.lex.eatStringConstant())
        else:
            return Constant(self.lex.eatIntConstant())
    def expression(self):
        if self.lex.matchId():
            return Expression(self.field())
        else:
            return Expression(self.constant())

    def term(self):
        temp_lhs = self.expression()
        self.lex.eatDelim('=')
        temp_rhs = self.expression()
        return Term(temp_lhs, temp_rhs)

    # predicate is one or more terms conjoined by AND operator
    def predicate(self):
        temp_pred = Predicate(self.term())
        if self.lex.matchKeyword('and'):
            self.lex.eatKeyword('and')
            temp_pred.conjoinWith(self.predicate())
        return temp_pred

    def query(self):
        self.lex.eatKeyword('select')
        temp_fields = self.selectList()
        self.lex.eatKeyword('from')
        temp_tables = self.tableList()
        temp_pred = Predicate()
        if self.lex.matchKeyword('where'):
            self.lex.eatKeyword('where')
            temp_pred = self.predicate()
        return {'fields':temp_fields, 'tables':temp_tables, 'predicate':temp_pred}

    def selectList(self):
        temp_list = []
        temp_list.append(self.field())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            temp_list.extend(self.selectList())
        return temp_list

    def tableList(self):
        temp_list = []
        temp_list.append(self.lex.eatId()) # self.lex.eatId() is equivalent to self.field()
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            temp_list.extend(self.tableList())
        return temp_list