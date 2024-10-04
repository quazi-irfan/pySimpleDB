import logging
db_logger = logging.getLogger('SimpleDB')

# SQL query is turned into relational algebra query
# Select - select rows from one table
# project - select columns from one table
# product - cross product between the rows of two tables

# Scan interface represents the output of relational algebra query
# Output of a query is a table
# therefore we access the output of a relational algebra query the same way we access a table

class Constant:
    def __init__(self, const_value):
        self.const_value = const_value

class Expression:
    def __init__(self, exp_value):
        self.exp_value = exp_value

    def evaluate(self, scan):
        if isinstance(self.exp_value, Constant):
            return self.exp_value.const_value
        else:
            return scan.getVal(self.exp_value)

class Term:
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def isSatisfied(self, scan):
        return self.lhs.evaluate(scan) == self.rhs.evaluate(scan)

    def reductionFactor(self, plan):
        pass

    def equatesWithConstant(self, field_name):
        pass

    def equatesWithField(self, const_value):
        pass

class Predicate:
    def __init__(self, term=None):
        self.terms = []
        if term:
            self.terms.append(term)

    def conjoinWith(self, pred):
        self.terms.extend(pred.terms)

    def isSatisfied(self, scan):
        for t in self.terms:
            if not t.isSatisfied(scan):
                return False
        return True

    def reductionFactor(self, plan):
        pass

    def equatesWithConstant(self, field_name):
        pass

    def equatesWithField(self, const_value):
        pass

class SelectScan:
    def __init__(self, scan, pred):
        self.scan = scan
        self.pred = pred

    def beforeFirst(self):
        self.scan.beforeFirst()

    def nextRecord(self):
        # continue fetching records until we get one that satisfies the predicate
        while self.scan.nextRecord():
            if self.pred.isSatisfied(self.scan):
                return True  # Found one
        return False  # No records passed the predicate

    def getInt(self, field_name):
        return self.scan.getInt(field_name)

    def getString(self, field_name):
        return self.scan.getString(field_name)

    # we push it up because only TabelScan has access to layout info to find if this variable is an int or str
    def getVal(self, field_name):
        return self.scan.getVal(field_name)

    def hasField(self, field_name):
        return self.scan.hasField(field_name)

    def closeRecordPage(self):
        self.scan.closeRecordPage()

class ProjectScan:
    def __init__(self, scan, *fields):
        self.scan = scan
        self.fields = fields

    def beforeFirst(self):
        self.scan.beforeFirst()

    def nextRecord(self):
        return self.scan.nextRecord()

    def getInt(self, field_name):
        if field_name in self.fields:
            return self.scan.getInt(field_name)
        else:
            raise Exception("Field not found")

    def getString(self, field_name):
        if field_name in self.fields:
            return self.scan.getString(field_name)
        else:
            raise Exception("Field not found")

    def getVal(self, field_name):
        if field_name in self.fields:
            return self.scan.getVal(field_name)
        else:
            raise Exception("Field not found")

    def hasField(self, field_name):
        return field_name in self.fields

    def closeRecordPage(self):
        self.scan.closeRecordPage()

class ProductScan:
    def __init__(self, scan1, scan2):
        self.scan1 = scan1
        self.scan2 = scan2
        scan1.nextRecord()

    def beforeFirst(self):
        self.scan1.beforeFirst()
        self.scan1.nextRecord()
        self.scan2.beforeFirst()

    def nextRecord(self):
        if self.scan2.nextRecord():
            return True
        else:
            self.scan2.beforeFirst()
            return self.scan1.nextRecord() and self.scan2.nextRecord()

    def getInt(self, field_name):
        if self.scan1.hasField(field_name):
            return self.scan1.getInt(field_name)
        elif self.scan2.hasField(field_name):
            return self.scan2.getInt(field_name)

    def getVal(self, field_name):
        if self.scan1.hasField(field_name):
            return self.scan1.getVal(field_name)
        else:
            return self.scan2.getVal(field_name)


    def getString(self, field_name):
        if self.scan1.hasField(field_name):
            return self.scan1.getString(field_name)
        elif self.scan2.hasField(field_name):
            return self.scan2.getString(field_name)

    def hasField(self, field_name):
        return self.scan1.hasField(field_name) or self.scan2.hasField(field_name)

    def closeRecordPage(self):
        self.scan1.closeRecordPage()
        self.scan2.closeRecordPage()