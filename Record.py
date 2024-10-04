from Transaction import *
import logging
db_logger = logging.getLogger('SimpleDB')

# Schema hold record(row's) schema
# Schema stores a list of triples (field_name, field_type, field_length)
# {field_name : (field_type, field_length)
class Schema:
    """Maintains a list of ``{field_name:{field_type, field_byte_length}}`` for each table"""

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

    def __str__(self):
        return self.pretty_str


# Layout hold record's field and slot side; field offset within a slot
class Layout:
    """Calculates field offset from schema info"""

    def __init__(self, schema, offset = None, slot_size = None):
        self.schema = schema

        if not offset and not slot_size:
            self.offset = {} # Holds byte offset for all fields inside a record
            field_pos = 4 # starting at 4 byte mark because the first 4 byte is int value representing is_slot_full flag
            for sk, sv in self.schema.field_info.items():
                self.offset[sk] = field_pos
                field_pos += (sv['field_byte_length'] if sv['field_type'] == 'int' else (sv['field_byte_length'] + 4))
            self.slot_size = field_pos
        else:
            # we are reading everything from table_catalog and field_catalog table
            #   therefore no calculation is necessary
            self.offset = offset
            self.slot_size = slot_size

    def __str__(self):
        return 'Layout :: \n' + str(self.schema) + 'Slot size: ' + str(self.slot_size)

# file is a sequence of blocks
# record files are a sequence of record pages/blocks
# record page contains sequence of slots
# slot are one byte + record

# RM is responsible for interpreting the values in a record blocks/page.
# RM uses Layout(slot size) and Schema(record info) class to update record page

# Tables are a collection of field(columns) and records(row)
# Record manager keeps the structure of the record(spanned/unspanned; homogeneous/nonhomogeneous) in the block
# Implementing 6.2.1, homogeneous, unspanned, fixed-length records
# block/page contains records, we call it record page

# beforeFirst -> firstRecord; move current_slot_index to -1
# insertAfter -> nextEmpty; move
class RecordPage: # Also being called Record Manager
    """Writes record data to a table block using layout"""
    def __init__(self, tx, blk, layout):
        self.tx: Transaction = tx
        self.blk: Block = blk
        self.layout: Layout = layout
        self.tx.pin(blk) # TODO: Are we pinning here to ensure tx.get/set does not fail?

    def setInt(self, slot_index, field_name, field_value):
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

    # Mark the slot empty; we are not operating per field, so field parameter is not needed
    def delete(self, slot_index):
        self.tx.setInt(self.blk, slot_index * self.layout.slot_size, 0, True)

    # Zero our all records in the record page
    def format(self):
        db_logger.info('Format')
        slot_index = 0
        while ((slot_index * self.layout.slot_size) + self.layout.slot_size) < self.tx.fm.block_size:
            self.tx.setInt(self.blk, slot_index * self.layout.slot_size, 0, False)
            for field_name in self.layout.schema.getFields():
                if self.layout.schema.field_info[field_name]['field_type'] == 'int':
                    self.tx.setInt(self.blk, slot_index * self.layout.slot_size + self.layout.offset[field_name], 0, False)
                else:
                    self.tx.setString(self.blk, slot_index * self.layout.slot_size + self.layout.offset[field_name], '', False) # TODO: If we are not putting anything then recovery doesn't know what to do
            slot_index += 1
        db_logger.info('Formatted')

    def nextEmpty(self, current_slot_index):
        return self.insertAfter(current_slot_index)

    # next empty slot index with empty flag set to 0
    def insertAfter(self, slot_index):
        slot_index += 1
        while ((slot_index * self.layout.slot_size) + self.layout.slot_size) <= self.tx.fm.block_size:
            if not self.tx.getInt(self.blk, slot_index * self.layout.slot_size):
                self.tx.setInt(self.blk, slot_index * self.layout.slot_size, 1, True) # Mark slot filled before returning it
                return slot_index
            slot_index += 1
        return -1

    def nextUsed(self, current_slot_index):
        return self.nextAfter(current_slot_index)

    # next used slot index with empty flag set to 1
    def nextAfter(self, slot_index):
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

    def __str__(self):
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
    """Access table file using the layout information"""

    def __init__(self, tx, table_name, layout):
        """Open tbl_name file and read records at cursor"""
        self.tx = tx
        self.table_name = table_name
        self.file_name = self.table_name + '.tbl'
        self.layout = layout

        self.current_slot_index = -1 # TODO: Book is initializing this value to zero.
        self.rp: RecordPage = None
        if self.tx.size(self.file_name):
            self.moveToBlock(0)
        else:
            # table file does not exist
            self.moveToNewBlock()

    def moveToNewBlock(self):
        if self.rp:
            self.tx.unpin(self.rp.blk)
        new_blk = self.tx.append(self.file_name)
        self.rp = RecordPage(self.tx, new_blk, self.layout) # tx.pin(rp.blk) happening in RecordPage constructor
        self.rp.format()
        self.current_slot_index = -1

    def moveToBlock(self, block_num):
        if self.rp:
            self.tx.unpin(self.rp.blk)
        new_blk = Block(self.file_name, block_num)
        self.rp = RecordPage(self.tx, new_blk, self.layout)
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