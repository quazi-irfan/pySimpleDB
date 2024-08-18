# each interactino with db is a transaction
# concurrency manager interleaves the regulation of transactions from different clients
# recovery manager write record of those transactions in a log so uncommited transactions can be recovered

# file system allows accessing raw disk in blocks.
# in db, each file is treated like a raw disk
# db access each file by virtual blocks(OR translates these blocks into physical blocks using file system)
# db reads block into pages
# db maintain a pool of pages in memory
# db read and write is done on those poled pages
# By only operating in memory, db can control when writing to disk is happening
# Each table, table index and db log are stored in a single file
# Postgresql uses 8kB as block size

import os

class Block:
    def __init__(self, file_name, block_number):
        self.file_name = file_name
        self.block_number = block_number

    # implement __eq__, __hash__, __str__

# FileMgr : readBlock(block_number, blockSize) and into a page
# here we are assuming one to one size corrospondence between block and page size

# We populate data in a bytearray and write it to file which triggers sys call to write
# f = open('all_tables', 'wb', buffering=0)
# f.write(page) # will trigger system write as there is no buffering
class Page:
    def __init__(self, data):
        # get either size to create an empty page, or data that needs to put in a page
        # bytearray(data) allocates new one in memory - usage is heavily controlled by Buffer manager
        # self.bb = data uses what is already in the memory
        # log manager(saves it log in memory, and dumps to this page) send bytearay; 
        # buffer manager send length of bytearray
        self.bb = data if isinstance(data, bytearray) else bytearray(data)

    # callee is reponsible ensuring there are required space for the data 
    def setData(self, start, data):
        if isinstance(data, int):
            data_bin = data.to_bytes(4, 'big') # chosing to convert integer to 4 bytes in big endian, which is the same way we write and read numbers
        elif isinstance(data, str): # for type str
            data_bin = data.encode('utf-8')
            data_bin_len = int.to_bytes(len(data_bin), 4, 'big') # size in byte is the same as the length of the string because I am hoping the string content will fall into ascii range
            data_bin = data_bin_len + data_bin
        else: # types i.e. bytes or bytearray
            data_bin_len = int.to_bytes(len(data), 4, 'big')
            data_bin = data_bin_len + data

        data_len = len(data_bin)
        # Do I need to create + new block to the file for data that exceeds boundary
        self.bb[start:start + data_len] = data_bin
        return data_len 
  

    def getStr(self, start):
        str_len = self.getInt(start)
        return self.bb[start+4 : start+4+str_len].decode()
    
    def getInt(self, start):
        return int.from_bytes(self.bb[start : start+4], 'big')

    def getByte(self, start):
        byte_len = self.getInt(start)
        return self.bb[start+4 : start+4+byte_len]

# Purpose of this class is to write a page to a block
# Read and trigger immediate disk operation( because buffering it set to 0) to ensure data is saved to disk
# TODO: readBlockToPage, writePageToBlock and appendEmptyBlock methods needs to be synchronized
class FileMgr:
    # https://stackoverflow.com/questions/1466000/difference-between-modes-a-a-w-w-and-r-in-built-in-open-function
    def __init__(self, db_name, block_size, buffer_size): #As of right now buffer_size is not being used
        self.db_name = db_name,
        self.block_size = block_size
        self.buffer_size = buffer_size

    def readBlockToPage(self, block, page):
        f = open(block.file_name, 'rb', buffering=0) # Does buffering has any effect on reading?
        f.seek(self.block_size * block.block_number)
        # Making sure we are only reading the block size of the file
        # We want to minimize the number of blocks we are reading from the disk
        # One way query optimize will make plan based on number of potential blocks we need to ready
        page.bb = bytearray(f.read(self.block_size))
        f.close()
    
    def writePageToBlock(self, block, page):
        f = open(block.file_name, 'r+b', buffering=0) # r is used because a prevents seek and w truncates the file
        f.seek(self.block_size * block.block_number)
        f.write(page.bb) 
        f.close()

    # Append a new block to the provided (log) file and return the block reference
    def appendEmptyBlock(self, fileName):
        f = open(fileName, 'ab', buffering=0) # How does append mode behave if file do not exists?
        new_block_number = self.length(fileName)
        f.seek(self.block_size * new_block_number) # seek doesn't with a mode
        temp_page = Page(self.block_size)
        f.write(temp_page.bb)
        f.close()
        return Block(fileName, new_block_number)

    def length(self, file_name):
        try :
            return os.path.getsize(file_name) // self.block_size 
        except:
            # trying to get number of block in a file that doesn't exist
            # context manager cleanup resources
            with open(file_name, 'wb', buffering=0):
                pass
            return 0

        
class LogMgr:
    def __init__(self, file_mgr, log_file):
        self.file_mgr = file_mgr
        self.log_file = log_file
        self.current_lsn = 0
        self.last_saved_lsn = 0

        self.log_page = Page(self.file_mgr.block_size)
        log_block_count = self.file_mgr.length(self.log_file)

        if log_block_count:
            # read last block of log file and put it in a page
            self.log_block = Block(self.log_file, log_block_count-1)
            self.file_mgr.readBlockToPage(self.log_block, self.log_page)
        else:
            # create new log, block and page 
            self.log_block = self.file_mgr.appendEmptyBlock(self.log_file)
            self.log_page.setData(0, self.file_mgr.block_size)
            self.file_mgr.writePageToBlock(self.log_block, self.log_page)
            
    # add b'log_record' to current log_page and return current_lsn
    def appendLog(self, log_record):
        boundary = self.log_page.getInt(0)
        bytes_needed = len(log_record) + 4 # for writing lengh of binary blob

        # check if there is room for the new log record on the current page
        if boundary - bytes_needed < 4: # first 4 bytes are reserved
            self.flushPage()
            self.log_block = self.file_mgr.appendEmptyBlock(self.log_file) # appendNewBlock()
            self.log_page = Page(self.file_mgr.block_size) # not present in the book
            self.log_page.setData(0, self.file_mgr.block_size) # at the beginning the page is empty
            boundary = self.log_page.getInt(0)
            self.file_mgr.writePageToBlock(self.log_block, self.log_page) # writing the newly created block/page immidiately emulates always having the latest block/page in log_block/log_page

        offset = boundary - bytes_needed 
        self.log_page.setData(offset, log_record) # ACTUAL WRITE
        self.log_page.setData(0, offset)
        self.current_lsn += 1
        return self.current_lsn

    # Log manager manually decides when to write the page to disk
    def flushPage(self, lsn=None):
        if not lsn:
            self.file_mgr.writePageToBlock(self.log_block, self.log_page)
            self.last_saved_lsn = self.current_lsn # because we will be flushing all logs from the single log page
            return

        if lsn > self.last_saved_lsn: # TODO: do we need >= instead?
            self.flushPage()

    # this is a stateful function; depends on what block log manager is currently working on
    def iterator(self):
        self.flushPage() # we flush one page, log manager is holding, to disk
        return LogIter(self.file_mgr, self.log_block) # Returning the current block


class LogIter:
    def __init__(self, fm, block):
        self.fm = fm
        self.block = block

    def __iter__(self):
        self.temp_page = Page(self.fm.block_size)
        fm.readBlockToPage(self.block, self.temp_page)
        self.current_offset = self.temp_page.getInt(0)
        return self # returning self because in each loop self.__next__ will be called

    def __next__(self):
        if self.current_offset >= self.fm.block_size: # reached at the end of the block
            self.block = Block(self.block.file_name, self.block.block_number-1) # Why -1? Doesn't block number start
            if self.block.block_number < 0:
                raise StopIteration()
            else:
                self.fm.readBlockToPage(self.block, self.temp_page)
                self.current_offset = self.temp_page.getInt(0)

        log_record = self.temp_page.getByte(self.current_offset) 
        self.current_offset = self.current_offset + len(log_record) + 4 # 4 bytes tho skip the length of the Byte blob
        return log_record

# pins page to block and tracks pin count
class Buffer:
    def __init__(self, fm, lm):
        self.fm = fm
        self.lm = lm

    def contents(self):
        pass

# BufferMgr does two things.
#   track changes to page(new data) and
#   (delay) write the modified page back to disk. Write happens when
#       1. page is getting pined to a diff block,
#       2. Recovery manager needs to write pages to prevent data loss
class BufferMgr:
    def __init__(self, fm, lm, num_buffers):
        self.fm = fm
        self.lm = lm
        self.num_buffers = num_buffers

        self.buffers = [Buffer(self.fm, self.lm) for _ in range(self.num_buffers)]

    def pin(self):
        pass

    def unpin(self):
        pass

# Each set of interaction with the database is a transactions
# Transaction is completed when it has committed or rolledback and released all locks
class Transaction:
    pass

# Fig 4.11 Testing Buffer
# fm = FileMgr('simpledb', 400, 8)
# lm = LogMgr(fm, 'tst_log')
# bm = BufferMgr(fm, lm, 3)
# buff1 = bm.pin()
# buff1 = bm.pin()
# exit()

# Fig 4.12 Testing Buffer Manager

# Fig 4.5 Testing Log Manager
fm = FileMgr('simpledb', 400, 8) # Kernel page size; usually 4096 bytes
lm = LogMgr(fm, 'tst_log')

def createLogRecord(s,i):
    temp_bytearray = bytearray(4 + len(s) + 4) # length of string + string + one number
    temp_page = Page(temp_bytearray) # creating page with desired size because
    pos = temp_page.setData(0, s)
    temp_page.setData(pos, i)
    lsn = lm.appendLog(temp_page.bb)
    return lsn


for i in range(1, 36):
    lsn = createLogRecord('record' + str(i), i + 100)
    print('Adding ' + '(lsn: ' + str(lsn) + '): \t' + 'record' + str(i) + str(i + 100))

for l in lm.iterator():
    temp_page = Page(l) # We have keep it in memory to parse its content
    record_str = temp_page.getStr(0)
    record_int = temp_page.getInt(4 + len(record_str)) # also need to add 4 byte for the recoded length of the string
    print('Reading:  ' + record_str + str(record_int))

for i in range(36, 71):
    lsn = createLogRecord('record' + str(i), i + 100)
    print('Adding ' + '(lsn: ' + str(lsn) + '): \t' + 'record' + str(i) + str(i + 100))

for l in lm.iterator():
    temp_page = Page(l) # We have keep it in memory to parse its content
    record_str = temp_page.getStr(0)
    record_int = temp_page.getInt(4 + len(record_str)) # also need to add 4 byte for the recoded length of the string
    print('Reading:  ' + record_str + str(record_int))

exit()

# 3.12 Testing file manager
# File for each table; many blocks(identified by id) for each file
# these files needs to be created inside a folded named $db
fm = FileMgr('simpledb', 400, 8) # Kernel page size; usually 4096 bytes
b1 = Block('testfile', 2)
p1 = Page(fm.block_size)
pos = 88 # position relative to the current block, so should always be between 0 <= block_size < 400
new_pos = pos + p1.setData(pos, 'abcdefghijklm')
p1.setData(new_pos, 345)
fm.writePageToBlock(b1, p1) # won't work because r+b is expecting the file to exists; in LogTest we are creating the empty file in appendEmptyBlock

temp_page = Page(fm.block_size)
fm.readBlockToPage(b1, temp_page)
print(temp_page.getStr(pos))
print(temp_page.getInt(new_pos))