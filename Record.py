from Transaction import *
import logging
db_logger = logging.getLogger('SimpleDB')

# Q1 Why we pin inside record page? Wouldn't make sense to pin it inside Tx
# Q2 My system can't recover from log if we are not populating with empty bytearray(length) with length encoded during format

# In a dictionary, schema object holds the table schema
# It holds a list of (fieldname, type, length)
# Schema stores a list of triples (field_name, field_type, field_length)
# field_info = {field_name : (field_type, field_byte_length), ...}
# TODO: Perhaps Schema should also hold the default value for each field that can be during during RecordPage.format()
class Schema:
    """
    Maintains a dict capturing column info {field_name:{field_type, field_byte_length}}
    Note: This class does not add 4 bytes to the length of string data type to store the length.
    """

    def __init__(self, *field_data):
        self.field_info = {}
        self.pretty_str = ''
        if field_data:
            for f in field_data:
                self.addField(f[0], f[1], f[2])

    def addField(self, field_name, field_type, field_byte_length):
        self.pretty_str += 'name: ' + field_name + ' type: ' + field_type + ' byte_length: ' + str(field_byte_length) + '\n'
        self.field_info[field_name] = {
            'field_type': field_type,
            'field_byte_length': field_byte_length
        }

    def getFields(self):
        return self.field_info.keys()

    def __repr__(self):
        return self.pretty_str


# Layout hold record's field and slot side; field offset within a slot
class Layout:
    """
    This class contains self.schema, self.offset and self.slot_size

    Calculates field offset from provided schema
    slot size = status flag + field offset
    For fixed length records, slot size is used to move from one record to another
    For variable length record we need to use an ID table. (Fig 6.8)

    Precalculate offset and slot_size can be passed to avoid rebuilding them.
    """

    def __init__(self, schema, offset = None, slot_size = None):
        self.schema = schema

        # new table
        if not offset and not slot_size:
            self.offset = {} # Holds byte offset for all fields inside a record
            field_pos = 4 # starting at 4 byte mark because the first 4 byte is int empty(0)/full(1) flag for this slot
            for sk, sv in self.schema.field_info.items():
                self.offset[sk] = field_pos
                field_pos += (sv['field_byte_length'] if sv['field_type'] == 'int' else (sv['field_byte_length'] + 4))
            self.slot_size = field_pos
        # table exists (read metadata table)
        else:
            # we are reading everything from table_catalog and field_catalog table
            #   therefore no calculation is necessary
            self.offset = offset
            self.slot_size = slot_size

    def __repr__(self):
        return 'Layout :: \n' + str(self.schema) + 'Slot size: ' + str(self.slot_size)

# file is a sequence of blocks
# record files are a sequence of record pages/blocks
# record page contains sequence of slots
# slot contains status flag(0 for empty and 1 for full) + combined field length

# Record Page, Record Manager, class is responsible for interpreting the values in a record blocks/page.
# RM uses Layout(slot size) and Schema(record info) class to update record page

# Tables are a collection of field(columns) and records(row)
# Implementing 6.2.1, homogeneous, unspanned, fixed-length records

# beforeFirst -> firstRecord; move current_slot_index to -1
# insertAfter -> nextEmpty; move
class RecordPage: # Also called Record Manager in the Book
    """
    Record page writes to buffers in buffer pool using a transaction.
    This class uses schema + layout object to find field offset(of a record) and uses tx object write to those offset.

    RecordPgae.setInt(slot_number, field_name, field_value)
    """
    def __init__(self, tx, blk, layout):
        self.tx: Transaction = tx
        self.blk: Block = blk
        self.layout: Layout = layout
        self.tx.pin(blk) # TODO: Are we pinning here to ensure tx.get/set does not fail?

    def setInt(self, slot_index, field_name, field_value):
        """
        Use the tx to update the field of a record. Record the old value of the field in log.
        """
        blk_offset = (self.layout.slot_size * slot_index) + self.layout.offset[field_name]
        self.tx.setInt(self.blk, blk_offset, field_value, True)

    def setString(self, slot_index, field_name, field_value):
        blk_offset = slot_index * self.layout.slot_size + self.layout.offset[field_name]
        self.tx.setString(self.blk, blk_offset, field_value, True)

    def getInt(self, slot_index, field_name):
        blk_offset = (self.layout.slot_size * slot_index) + self.layout.offset[field_name]
        return self.tx.getInt(self.blk, blk_offset)

    def getString(self, slot_index, field_name):
        blk_offset = (slot_index * self.layout.slot_size) + self.layout.offset[field_name]
        return self.tx.getString(self.blk, blk_offset)

    # Mark the slot status field empty
    # TODO: Do we also need to clean the fields? How does undo interacts with slots that are marked empty?
    def delete(self, slot_index):
        self.tx.setInt(self.blk, slot_index * self.layout.slot_size, 0, True) # slot status (0 for empty and 1 for full)

    # Zero our all records in the record page
    def format(self):
        db_logger.info('Format')
        slot_index = 0
        while ((slot_index * self.layout.slot_size) + self.layout.slot_size) < self.tx.fm.block_size: # Check if there room for the next slot?
            self.tx.setInt(self.blk, slot_index * self.layout.slot_size, 0, False) # Set the slot status field 0 for empty
            for field_name in self.layout.schema.getFields():
                if self.layout.schema.field_info[field_name]['field_type'] == 'int':
                    self.tx.setInt(self.blk, slot_index * self.layout.slot_size + self.layout.offset[field_name], 0, False)
                else:
                    # TODO: If we are not putting anything then recovery doesn't know what to do - why can't we just read the byte array in that range and put it in log?
                    self.tx.setString(self.blk, slot_index * self.layout.slot_size + self.layout.offset[field_name], bytearray(self.layout.schema.field_info[field_name]['field_byte_length']).decode(), False)
            slot_index += 1
        db_logger.info('Formatted')

    # Facade function for insertAfter
    def nextEmpty(self, current_slot_index):
        """
        Returns the slot index of the next empty slot from current_slot_index in the current record page. If none found, return -1.

        This function excluded passed current_slot_index. Therefore use inserAfter(-1) to have this function return 0 for a newly formatted record page.
        """
        return self.insertAfter(current_slot_index)

    # next empty slot index with status flag set to empty(0)
    def insertAfter(self, slot_index): # Book
        slot_index += 1
        while ((slot_index * self.layout.slot_size) + self.layout.slot_size) <= self.tx.fm.block_size:
            if not self.tx.getInt(self.blk, slot_index * self.layout.slot_size):
                self.tx.setInt(self.blk, slot_index * self.layout.slot_size, 1, True) # Mark slot filled before returning it
                return slot_index
            slot_index += 1
        return -1

    # Facade function for nextAfter
    def nextUsed(self, current_slot_index):
        """
        Returns the slot index of the next full slot from current_slot_index in the current record page. If none found, return -1.

        This function excludes passed current_slot_index. Therefore use nextUsed(-1) to start search from the beginning of the record page.
        """
        return self.nextAfter(current_slot_index)

    # next used slot index with status flag set to full(1)
    def nextAfter(self, slot_index): # Book
        slot_index += 1
        while ((slot_index * self.layout.slot_size) + self.layout.slot_size) <= self.tx.fm.block_size:
            if self.tx.getInt(self.blk, slot_index * self.layout.slot_size):
                return slot_index
            slot_index += 1
        return -1

# Each record in file can be identified by block number and slot number
# These two values put together is called Record Identifier
class RecordID:
    def __init__(self, blk_num, slot_num):
        self.blk_num = blk_num
        self.slot_num = slot_num

    def __eq__(self, other):
        return self.blk_num == other.blk_num and self.slot_num == other.slot_num

    def __repr__(self):
        return "[block numer: " + str(self.blk_num) + ", slot number: " + str(self.slot_num) + "]"


# The whole table lives on a single table
# TableScan manages all records on a file
# During constructor it is using tx, layout and table_name to automatically pointing to the first record

# TableScan uses RecordPage to maintain reference to a single Block
# TableScan opens the first block 0 from a file
#   TableScan also need a transaction and layout information
#   The layout is passed to the RecordPage, TableScan use RecordPage object to move from one record to another
#   TableScan maintains a current_slot_index to know which slot it is reading/writing in that block
# Each record in a file can be identified by block number and slot number, these two combined is called RecordID
# next() moves the cursor forward; it also moves to the next block if there is no more record in the current block
class TableScan:
    """
    TableScan, like an iterator, maintains a cursor to a particular record in the table file,
        reads one block at a time from a table file,
        maintains the block as a record page, and
        provides convenient function to read/write record to table file.
    Keeps the current block in a RecordPage to easily write to it.

    RecordPgae.setInt(slot_number, field_name, field_value)
    TabelScane.steInt(field_name, field_value)
    """

    def __init__(self, tx, table_name, layout):
        """
        Open tbl_name file and read records at cursor.

        After init slot_index points to -1, and that needs to move to an actual record/slot index before reading/writing.
        """
        self.tx: Transaction = tx
        self.table_name = table_name
        self.file_name = self.table_name + '.tbl'
        self.layout: Layout = layout

        self.current_slot_index = -1 # TODO: Book is initializing this value to zero.
        self.rp: RecordPage = None
        if self.tx.size(self.file_name) > 0:
            self.moveToBlock(0)
        else:
            # table file does not exist
            self.moveToNewBlock()

    def moveToBlock(self, block_num):
        if self.rp is not None:
            self.tx.unpin(self.rp.blk)
        new_blk = Block(self.file_name, block_num)
        self.rp = RecordPage(self.tx, new_blk, self.layout)
        self.current_slot_index = -1

    def moveToNewBlock(self):
        if self.rp is not None:
            self.tx.unpin(self.rp.blk)
        new_blk = self.tx.append(self.file_name)
        self.rp = RecordPage(self.tx, new_blk, self.layout) # tx.pin(rp.blk) happening in RecordPage constructor
        self.rp.format()
        self.current_slot_index = -1

    # current_slot movement
    def nextRecord(self):
        self.current_slot_index = self.rp.nextAfter(self.current_slot_index)
        while self.current_slot_index < 0:
            if self.rp.blk.block_number == self.tx.size(self.file_name) - 1: #TODO this implies block count start at zero
                return False
            self.moveToBlock(self.rp.blk.block_number + 1) # Moving to new block may not get us a filled out record...
            self.current_slot_index = self.rp.nextAfter(self.current_slot_index) # ...so we continue
        return True

    def nextEmptyRecord(self):
        self.insert()

    # have current_slot_index point to next available empty slot
    def insert(self):
        self.current_slot_index = self.rp.insertAfter(self.current_slot_index)
        while self.current_slot_index < 0:
            # we reached at the end of current block
            if self.rp.blk.block_number == self.tx.size(self.file_name) - 1:
                # this was the final block in the file, therefore append a new block to our table file
                self.moveToNewBlock()
            else:
                # there are more blocks to this file, therefore move to the next block
                self.moveToBlock(self.rp.blk.block_number + 1)
            self.current_slot_index = self.rp.insertAfter(self.current_slot_index)

    def deleteRecord(self):
        self.rp.delete(self.current_slot_index)

    def firstRecord(self):
        self.beforeFirst()

    def beforeFirst(self):
        self.moveToBlock(0)

    # RecordID
    def moveToRecordID(self, rid : RecordID):
        self.moveToBlock(rid.blk_num)
        self.current_slot_index = rid.slot_num


    # operate on the current_slot_index
    def getInt(self, field_name):
        return self.rp.getInt(self.current_slot_index, field_name)

    def getString(self, field_name):
        return self.rp.getString(self.current_slot_index, field_name)

    def getVal(self, field_name):
        if self.layout.schema.field_info[field_name]['field_type'] == 'int':
            return self.getInt(field_name)
        else:
            return self.getString(field_name)

    def setInt(self, field_name, field_value):
        self.rp.setInt(self.current_slot_index, field_name, field_value)

    def setString(self, field_name, field_value):
        self.rp.setString(self.current_slot_index, field_name, field_value)

    def currentRecordID(self):
        return RecordID(self.rp.blk.block_number, self.current_slot_index)

    def hasField(self, field_name):
        return field_name in self.layout.offset.keys()

    def closeRecordPage(self):
        if self.rp:
            self.tx.unpin(self.rp.blk)


if __name__ == '__main__':
    fig = [6.11, 6.15, 6.18][2]

    if fig == 6.18:
        # Fig 6.18 TableScanTest
        fm: FileMgr = FileMgr('tabletest', 400)
        lm: LogMgr = LogMgr(fm, 'simpledb.log')
        bm: BufferMgr = BufferMgr(fm, lm, 2)
        tx: Transaction = Transaction(fm, lm, bm)
        sch: Schema = Schema()
        sch.addField('A', 'int', 4)
        sch.addField('B', 'str', 9)
        layout: Layout = Layout(sch)

        ts: TableScan = TableScan(tx, "T", layout)
        print('Insertion')
        rand_val = [49, 34, 40, 30, 1, 17, 18, 45, 27, 5, 7, 27, 43, 9, 31, 21, 2, 2, 28, 16, 44, 3, 14, 44, 47, 41, 22, 0,
                    23, 42, 3, 25, 3, 50, 29, 35, 28, 45, 50, 6, 49, 30, 18, 16, 42, 6, 8, 45, 11, 31]
        rand_count = 0
        ts.firstRecord()
        for i in range(50):
            ts.nextEmptyRecord()
            # temp_val = random.randint(0, 50)
            temp_val = rand_val[rand_count]
            rand_count += 1
            ts.setInt('A', temp_val)
            ts.setString('B', 'rec' + str(temp_val))
            print('inserting ' + str(ts.currentRecordID()) + '; ' + str(temp_val) + ' rec' + str(temp_val))

        print('Deletion')
        count = 0
        ts.firstRecord()
        while ts.nextRecord():
            a = ts.getInt('A')
            b = ts.getString('B')
            if a < 25:
                count += 1
                print('Deleting ' + str(ts.currentRecordID()) + ' ; value ' + str(a) + ' ' + b)
                ts.deleteRecord()

        print('Retained')
        ts.firstRecord()
        while ts.nextRecord():
            a = ts.getInt('A')
            b = ts.getString('B')
            print('Retained ' + str(ts.currentRecordID()) + ' ; value ' + str(a) + ' ' + b)

        ts.closeRecordPage()
        tx.commit()

        print(lm)

    elif fig == 6.15:
        # Fig 6.15 RecordTest; Testing RecordPage, Schema, Layout
        fm: FileMgr = FileMgr('recordtest', 400)
        lm: LogMgr = LogMgr(fm, 'simpledb.log')
        bm: BufferMgr = BufferMgr(fm, lm, 8)

        tx: Transaction = Transaction(fm, lm, bm)
        blk: Block = tx.append('testfile') # File doesn't exist; so make an empty file first and then append and empty block to it
        tx.pin(blk)
        sch: Schema = Schema()
        sch.addField('A', 'int', 4)
        sch.addField('B', 'str', 9)
        layout: Layout = Layout(sch)
        rp: RecordPage = RecordPage(tx, blk, layout)
        rp.format()

        # populate the record page with some value
        print("RecordPage init")
        rand_val = [49, 34, 40, 30, 1, 17, 18, 45, 27, 5, 7, 27, 43, 9, 31, 21, 2, 2, 28, 16, 44, 3, 14, 44, 47, 41, 22, 0,
                    23, 42, 3, 25, 3, 50, 29, 35, 28, 45, 50, 6, 49, 30, 18, 16, 42, 6, 8, 45, 11, 31]
        rand_count = 0
        next_empty_slot = rp.insertAfter(-1)
        while next_empty_slot >= 0:
            rec_val = rand_val[rand_count]
            rand_count += 1
            rp.setInt(next_empty_slot, 'A', rec_val)
            rp.setString(next_empty_slot, 'B', 'rec' + str(rec_val))
            print('Insert slot ' + str(next_empty_slot) + ' [' + str(rec_val) + ', rec' + str(rec_val) + ']')
            next_empty_slot = rp.insertAfter(next_empty_slot)

        # delete some records
        print("RecordPage deletion")
        next_used_slot = rp.nextAfter(-1)
        del_counter = 0
        while next_used_slot >= 0:
            a = rp.getInt(next_used_slot, 'A')
            b = rp.getString(next_used_slot, 'B')
            if rp.getInt(next_used_slot, 'A') < 25:
                del_counter += 1
                rp.delete(next_used_slot)
                print('Deleting slot ' + str(next_used_slot) + ' [' + str(a) + ',' + str(b) + ']')
            next_used_slot = rp.nextAfter(next_used_slot)

        # Print the record page, print the records that were not deleted
        print("RecordPage Retained")
        next_empty_slot = rp.nextAfter(-1)
        while next_empty_slot >= 0:
            a = rp.getInt(next_empty_slot, 'A')
            b = rp.getString(next_empty_slot, 'B')
            print('Retained slot ' + str(next_empty_slot) + ' [' + str(a) + ',' + str(b) + ']')
            next_empty_slot = rp.nextAfter(next_empty_slot)

        tx.unpin(blk)  # Not necessary as commit() unpins all pinned buffers
        tx.commit()

        print(lm)

    elif fig == 6.11:
        sch = Schema()
        sch.addField('cid', 'int', 4)
        sch.addField('title', 'str', 20)
        sch.addField('deptid', 'int', 4)

        layout = Layout(sch)
        # 4 byte for flag; 4 byte for cid; 24 byte for title; 4 byte for deptid = 36 byte
        print(layout)
        # for k, v in layout.schema.field_info.items():
        #     print(k, v['field_byte_length'], layout.offset[k])
        # print(layout.slot_size)