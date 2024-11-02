from Metadata import *
from Parser import *
import logging
db_logger = logging.getLogger('SimpleDB')

# In the book, all *Plan classes are implementing Plan interface
# Plan interface has the following methods;
#   open()
#   blocksAccessed()
#   recordsOutput()
#   distinctValues(field_name)
#   schema()
#       schema is used to verify type correctness and plan optimization
class TablePlan:
    def __init__(self, tx : Transaction, table_name, mm : MetadataMgr):
        self.tx = tx
        self.table_name = table_name
        self.layout = mm.getLayout(self.tx, self.table_name)
        self.table_stat = mm.getStatInfo(tx, self.table_name, self.layout)

    def open(self):
        return TableScan(self.tx, self.table_name, self.layout)

    def blocksAccessed(self):
        return self.table_stat['blocksAccessed']

    def recordsOutput(self):
        return self.table_stat['recordsOutput']

    # TODO: Update this once we have distinct values for each column name
    def distinctValues(self, field_name):
        return self.table_stat['distinctValues']

    def plan_schema(self):
        return self.layout.schema

class SelectPlan:
    def __init__(self, plan, pred):
        self.plan = plan
        self.pred = pred

    def open(self):
        temp_scan = self.plan.open()
        return SelectScan(temp_scan, self.pred)

    def blocksAccessed(self):
        return self.plan.blocksAccessed()

    def recordsOutput(self):
        return self.plan.recordsOutput()

    # TODO: Update this once we have distinct values for each column name
    def distinctValues(self, field_name):
        return self.plan.distinctValues()

    def plan_schema(self):
        return self.plan.plan_schema()

class ProjectPlan:
    def __init__(self, plan, *fields):
        self.plan = plan
        self.fields = fields
        self.schema: Schema = Schema()
        for field_name in self.fields:
            self.schema.addField(
                field_name,
                self.plan.plan_schema().field_info[field_name]['field_type'],
                self.plan.plan_schema().field_info[field_name]['field_byte_length']
            )

    def open(self):
        temp_scan = self.plan.open()
        return ProjectScan(temp_scan, *self.fields)

    def blocksAccessed(self):
        return self.plan.blocksAccessed()

    def recordsOutput(self):
        return self.plan.recordsOutput()

    # TODO: Update this once we have distinct values for each column name
    def distinctValues(self, field_name):
        return self.plan.distinctValues()

    def plan_schema(self):
        return self.schema

class ProductPlan:
    def __init__(self, plan1, plan2):
        self.plan1 = plan1
        self.plan2 = plan2
        self.schema = Schema()
        self.schema.field_info = {
            **self.plan1.plan_schema().field_info.copy(),
            **self.plan2.plan_schema().field_info.copy()
        }

    def open(self):
        temp_scan1 = self.plan1.open()
        temp_scan2 = self.plan2.open()
        return ProductScan(temp_scan1, temp_scan2)

    def blocksAccessed(self):
        pass

    def recordsOutput(self):
        pass

    # TODO: Update this once we have distinct values for each column name
    def distinctValues(self, field_name):
        pass

    def plan_schema(self):
        return self.schema

# implements basic query planning algorithme explained in 10.10
# All variants of QueryPlanner implements QueryPlanner interface
class BasicQueryPlanner:
    def __init__(self, mm):
        self.mm = mm

    def createPlan(self, tx, query_data):
        all_plans = []
        for table_name in query_data['tables']:
            all_plans.append(TablePlan(tx, table_name, self.mm))

        product_plan = all_plans[0]
        del all_plans[0]
        for t in all_plans:
            product_plan = ProductPlan(t, product_plan)

        select_plan = SelectPlan(product_plan, query_data['predicate'])

        return ProjectPlan(select_plan, *query_data['fields'])

class BetterQueryPlanner:
    pass

# All updates scanners implements UpdatePlanner interface
class BasicUpdatePlanner:
    def __init__(self, mm):
        self.mm = mm

class Planner:
    def __init__(self, query_planner, update_planner):
        self.query_planner = query_planner
        self.update_planner = update_planner

    def createQueryPlan(self, tx, query):
        parsed_query = Parser(query)
        # TODO: Verify Consistency of ACID; meaning that query is valid to run, such as type checking
        # TODO: Verify the user is authorized to run, table access permission

        # TODO: Query rewrite during optimization; such as using BetterQueryPlanner.createPlan()
        query_plan = self.query_planner.createPlan(tx, parsed_query.query())
        return query_plan