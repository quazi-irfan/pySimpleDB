from Record import *
import logging
db_logger = logging.getLogger('SimpleDB')


# scheme contains field name, type and byte length
# layout contains offset, additional padding
# record page uses layout into to write data to a block
# table scan uses record page to cycle through blocks in a file
# table manager uses table scan to bind table and layout in db metadata
class TableMgr:
    """Create and maintain table_catalog and field_catalog for each new table"""
    # Not using; but leaving as a reminder that all string can be of max 16 char long in the catalog tables
    # I am using 20 because some column lengths are long, such as len('field_byte_length') = 17
    max_name_length = 16

    def __init__(self, tx, init_table_catalog):
        self.tx = tx

        self.table_catalog_schema = Schema(
            ['table_name', 'str', 20],
            ['slot_size', 'int', 4]
        )
        self.table_catalog_layout = Layout(self.table_catalog_schema) # Used in getLayoutMetadata

        self.field_catalog_schema = Schema(
            ['table_name', 'str', 20],
            ['field_name', 'str', 20],
            ['field_type', 'str', 4],
            ['field_byte_length', 'int', 4],
            ['field_byte_offset', 'int', 4]
        )
        self.field_catalog_layout = Layout(self.field_catalog_schema) # Used in getLayoutMetadata

        # during db initialization we need to initialize two catalog tables files that holds the metadata of all other tables
        # these two catalog table contains their own metadata too
        if init_table_catalog:
            self.createTable(self.tx, 'table_catalog', self.table_catalog_schema)
            self.createTable(self.tx, 'field_catalog', self.field_catalog_schema)

    # Create a new table in the database
    #   This does not imply creating a new tbl file
    #   but this implies creating appropriate entries to the table_catalog and field_catalog
    def createTable(self, tx, new_table_name, new_sch):
        temp_layout = Layout(new_sch)

        # Add new table name and its slot size to the table_catalog table
        table_ts = TableScan(tx, 'table_catalog', self.table_catalog_layout)
        table_ts.nextEmptyRecord()
        table_ts.setString('table_name', new_table_name)
        table_ts.setInt('slot_size', temp_layout.slot_size)
        table_ts.closeRecordPage()

        # Add fields info of the new tables to field_catalog table
        field_ts = TableScan(tx, 'field_catalog', self.field_catalog_layout)
        for f in temp_layout.schema.getFields():
            field_ts.nextEmptyRecord()
            field_ts.setString('table_name', new_table_name)
            field_ts.setString('field_name', f)
            field_ts.setString('field_type', temp_layout.schema.field_info[f]['field_type'])
            field_ts.setInt('field_byte_length', temp_layout.schema.field_info[f]['field_byte_length'])
            field_ts.setInt('field_byte_offset', temp_layout.offset[f])
        field_ts.closeRecordPage()

    def getLayout(self, tx, table_name):
        # read table_catalog and field_catalog to generate the layout for a requested table
        schema_ts = TableScan(tx, 'field_catalog', self.field_catalog_layout)
        temp_sch = Schema()
        temp_offset = {}
        while schema_ts.nextRecord():
            if schema_ts.getString('table_name') == table_name:
                temp_sch.addField(schema_ts.getString('field_name'), schema_ts.getString('field_type'), schema_ts.getInt('field_byte_length'))
                temp_offset[schema_ts.getString('field_name')] = schema_ts.getInt('field_byte_offset')
        schema_ts.closeRecordPage()

        # Although Layout constructor auto initialize slot_size
        #   we are still getting the value from table_catalog to ensure all data in layout is coming from table
        temp_slot_size = None
        slot_ts = TableScan(tx, 'table_catalog', self.table_catalog_layout)
        while slot_ts.nextRecord():
            if slot_ts.getString('table_name') == table_name:
                temp_slot_size = slot_ts.getInt('slot_size')
        slot_ts.closeRecordPage()
        return Layout(temp_sch, temp_offset, temp_slot_size)

class ViewMgr:
    def __init__(self, tx, table_mgr, init_view_catalog):
        self.tx = tx
        self.table_mgr = table_mgr

        if init_view_catalog:
            view_catalog_schema = Schema(['view_name', 'str', 20], ['view_def', 'str', 100])
            self.table_mgr.createTable(self.tx, 'view_catalog', view_catalog_schema)

    def createView(self, tx, view_name, view_def):
        view_catalog_layout = self.table_mgr.getLayout(tx, 'view_catalog')
        ts: TableScan = TableScan(tx, 'view_catalog', view_catalog_layout)
        ts.nextEmptyRecord()
        ts.setString('view_name', view_name)
        ts.setString('view_def', view_def)
        ts.closeRecordPage()

    def getViewDef(self, tx, view_name):
        view_catalog_layout = self.table_mgr.getLayout(tx, 'view_catalog')
        ts: TableScan = TableScan(tx, 'view_catalog', view_catalog_layout)
        temp_view_def = None
        while ts.nextRecord():
            if ts.getString('view_name') == view_name:
                temp_view_def = ts.getString('view_def')
                break
        ts.closeRecordPage()
        return temp_view_def

# Replacing StatInfo with a python dictionary
# TODO: Why we need locks when updating statistics
class StatMgr:
    def __init__(self, tx, tm):
        self.tx = tx
        self.tm = tm

        self._refreshAll = threading.Lock()
        self._refreshOne = threading.Lock()
        self._numcalls = 0
        self.table_stats = {}
        self.refreshStatistics(self.tx)

    # This method needs layout parameter
    #   because it might need to open and parse the table to calculate the table statistics
    def getStatInfo(self, tx, table_name, table_layout):
        self._numcalls += 1
        if self._numcalls > 100:
            self.refreshStatistics(tx)

        if table_name not in self.table_stats:
            self.table_stats[table_name] = self.calcTableStats(tx, table_name, table_layout)

        return self.table_stats[table_name]

    # loop over all table name in table_catalog and calculate statistics for each table
    def refreshStatistics(self, tx):
        with self._refreshAll:
            self._numcalls = 0
            temp_table_catalog_layout = self.tm.getLayout(tx, 'table_catalog')
            ts = TableScan(tx, 'table_catalog', temp_table_catalog_layout)
            while ts.nextRecord():
                table_name = ts.getString('table_name')
                table_layout = self.tm.getLayout(tx, 'table_name')
                table_stat = self.calcTableStats(tx, table_name, table_layout)
                self.table_stats[table_name] = table_stat
            ts.closeRecordPage()

    # Read the entire table to build table statistics
    # distinct value is currently returns wildly inaccurate value
    # TODO: Once we add calculating distinct values for each fields; we also need to update TablePlan
    def calcTableStats(self, tx, table_name, table_layout):
        # TODO: why same thread was not able to acquire the same lock?
        with self._refreshOne:
            record_count = 0
            block_count = 0
            ts = TableScan(tx, table_name, table_layout)
            while ts.nextRecord():
                record_count += 1
                block_count = ts.rp.blk.block_number + 1
                # TODO: in another nested loop read through columns to calculate their distinct value statistic
            ts.closeRecordPage()
            return {'blocksAccessed': block_count, 'recordsOutput': record_count, 'distinctValues': (record_count + 1) / 3}

class IndexMgr:
    def __init__(self, tx, tm, sm, init_index_catalog):
        self.tx: Transaction = tx
        self.tm: TableMgr = tm
        self.sm: StatMgr = sm

        if init_index_catalog:
            index_sch = Schema(['index_name', 'str', 20], ['table_name', 'str', 20],['field_name', 'str', 20])
            self.index_layout = Layout(index_sch)
            tm.createTable(tx, 'index_catalog', index_sch)

    # Create an index over a particular column on a table
    def createIndex(self, tx, index_name, table_name, field_name):
        ts = TableScan(tx, 'index_catalog', self.index_layout)
        ts.nextEmptyRecord()
        ts.setString('index_name', index_name)
        ts.setString('table_name', table_name)
        ts.setString('field_name', field_name)
        ts.closeRecordPage()

    # returns all index from a table
    # For each column with index we will get an new entry in the dictionary
    # Return value is in form {field_name1: IndexInfo1; field_name2: IndexInfo2}
    def getIndexInfoTable(self, tx, table_name):
        all_index = {}
        ts = TableScan(tx, 'index_catalog', self.index_layout)
        while ts.nextRecord():
            if ts.getString('table_name') == table_name:
                temp_index_name = ts.getString('index_name')
                temp_field_name = ts.getString('field_name')
                index_info = IndexInfo(
                    tx,
                    temp_index_name,
                    temp_field_name,
                    ts.getString(''),
                    self.sm.getStatInfo(tx, table_name, self.tm.getLayout(tx, table_name))
                )
                all_index[temp_field_name] = index_info
        ts.closeRecordPage()

# IndexInfo provides statistical info about an index; similar to StatInfo(Page 201)
class IndexInfo:
    def __init__(self, tx, index_name, field_name, table_layout, table_stat):
        pass

    def open(self):
        pass

    # Estimate the cost of searching using this index
    # The cost is in the unit of required block access
    def blocksAccessed(self):
        pass

    # number of records in the index
    def recordsOutput(self):
        pass

# Facade class
class MetadataMgr:
    def __init__(self, tx, init_db):
        self.tx = tx
        self.table_mgr = TableMgr(self.tx, init_db)
        self.view_mgr = ViewMgr(self.tx, self.table_mgr, init_db)
        self.stat_mgr = StatMgr(self.tx, self.table_mgr)
        self.index_mgr = IndexMgr(self.tx, self.table_mgr, self.stat_mgr, init_db)

    def createTable(self, tx, table_name, schema):
        self.table_mgr.createTable(tx, table_name, schema)
    def getLayout(self, tx, table_name):
        return self.table_mgr.getLayout(tx, table_name)

    def createView(self, tx, view_name, view_def):
        self.view_mgr.createView(tx, view_name, view_def)
    def getViewDef(self, tx, view_name):
        return self.view_mgr.getViewDef(tx, view_name)

    def createIndex(self, tx, index_name, table_name, field_name):
        self.index_mgr.createIndex(tx, index_name, table_name, field_name)
    def getIndexInfo(self, tx, table_name):
        return self.index_mgr.getIndexInfoTable(tx, table_name)

    def getStatInfo(self, tx, table_name, table_layout):
        return self.stat_mgr.getStatInfo(tx, table_name, table_layout)