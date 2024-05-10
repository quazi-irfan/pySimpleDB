# each interactino with db is a transaction
# concurrency manager interleaves the regulation of transactions from different clients
# recovery manager write record of those transactions in a log so uncommited transactions can be recovered

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
        # log manager(saves it log in memory, and dumps to this page) send bytearay; 
        # buffer manager send length of bytearray 
        self.bb = data if isinstance(data, bytearray) else bytearray(data)

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


class FileMgr:
    def __init__(self, db_name, block_size, buffer_size): #As of right now buffer_size is not being used
        self.db_name = db_name,
        self.block_size = block_size
        self.buffer_size = buffer_size

    def read(self, block, page):
        f = open(block.file_name, 'rb', buffering=0) # Does buffering has any effect on reading?
        f.seek(self.block_size * block.block_number)
        # Making sure we are only reading the block size of the file
        # We want to minimize the number of blocks we are reading from the disk
        # One way query optimize will make plan based on number of potential blocks we need to ready
        page.bb = bytearray(f.read(self.block_size))
        f.close()
    
    def write(self, block, page):
        f = open(block.file_name, 'r+b', buffering=0)
        f.seek(self.block_size * block.block_number)
        f.write(page.bb) 
        f.close()

    # Append a new block to the provided (log) file and return the block reference
    def append(self, fileName):
        f = open(fileName, 'a+b', buffering=0) # How does append mode behave if file do not exists?
        new_block_number = self.length(fileName)
        f.seek(self.block_size * new_block_number)
        temp_page = Page(self.block_size)
        f.write(temp_page.bb)
        f.close()
        return Block(fileName, new_block_number)

    def length(self, file_name):
        try :
            return os.path.getsize(file_name) // self.block_size 
        except:
            open(file_name, 'w+b', buffering=0)
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
            # read last block of log file
            self.log_block = Block(self.log_file, log_block_count-1)
            self.file_mgr.read(self.log_block, self.log_page)
        else:
            # create new log, block and page 
            self.log_block = self.file_mgr.append(self.log_file)
            self.log_page.setData(0, self.file_mgr.block_size)
            self.file_mgr.write(self.log_block, self.log_page)
            
    def append(self, log_record):
        boundary = self.log_page.getInt(0)
        bytes_needed = len(log_record) + 4 # for writing lenght of binary blob
        if boundary - bytes_needed < 4: # first 4 bytes are reserved
            self.flush() 
            self.log_block = self.file_mgr.append(self.log_file) # appendNewBlock()
            self.log_page = Page(self.file_mgr.block_size) # not present in the book
            self.log_page.setData(0, self.file_mgr.block_size)
            boundary = self.log_page.getInt(0)
            self.file_mgr.write(self.log_block, self.log_page) # writing the newly created block/page immidiately emulates always having the latest block/page in log_block/log_page

        offset = boundary - bytes_needed 
        self.log_page.setData(offset, log_record) # ACTUAL WRITE
        self.log_page.setData(0, offset)
        self.current_lsn += 1
        return self.current_lsn

    def flush(self):
        self.file_mgr.write(self.log_block, self.log_page)
        self.last_saved_lsn = self.current_lsn

    # this is a stateful function; depends on what block log manager is currently working on
    def iterator(self):
        self.flush()
        return LogIter(self.file_mgr, self.log_block) # Returning the current block


class LogIter:
    def __init__(self, fm, block):
        self.fm = fm
        self.block = block


    def __iter__(self):
        self.temp_page = Page(self.fm.block_size)
        fm.read(self.block, self.temp_page)
        self.current_offset = self.temp_page.getInt(0)

        return self

    def __next__(self):
        if self.current_offset >= self.fm.block_size:
            self.block = Block(self.block.file_name, self.block.block_number-1)
            if self.block.block_number < 0:
                raise StopIteration()
            else:
                fm.read(self.block, self.temp_page)
                self.current_offset = self.temp_page.getInt(0)

        log_record = self.temp_page.getByte(self.current_offset) 
        self.current_offset = self.current_offset + len(log_record) + 4 # 4 bytes tho skip the length of the Byte blob
        return log_record 


class Buffer:
    def __init__(self, fm, lm):
        self.fm = fm
        self.lm = lm

    def contents(self):
        pass

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

fm = FileMgr('simpledb', 400, 8)
lm = LogMgr(fm, 'tst_log')
bm = BufferMgr(fm, lm, 3) 
buff1 = bm.pin() 
exit()

fm = FileMgr('simpledb', 400, 8) # Kernel page size; usually 4096 bytes
lm = LogMgr(fm, 'tst_log')

def createLogRecord(s,i):
    temp_bytearray = bytearray(4 + len(s) + 4)
    temp_page = Page(temp_bytearray)
    pos = temp_page.setData(0, s)
    temp_page.setData(pos, i) 
    lsn = lm.append(temp_page.bb)
    return lsn

for i in range(1, 36):
    lsn = createLogRecord('record' + str(i), i + 100)
    print('lsn created' + str(lsn))

for l in lm.iterator():
    temp_page = Page(l) # We have keep it in memory to parse its content
    record_str = temp_page.getStr(0)
    record_int = temp_page.getInt(4 + len(record_str)) # also need to add 4 byte for the recoded length of the string
    print('Reading record ' + record_str + str(record_int))

for i in range(36, 71):
    lsn = createLogRecord('record' + str(i), i + 100)
    print('lsn created' + str(lsn))

for l in lm.iterator():
    temp_page = Page(l) # We have keep it in memory to parse its content
    record_str = temp_page.getStr(0)
    record_int = temp_page.getInt(4 + len(record_str)) # also need to add 4 byte for the recoded length of the string
    print('Reading record ' + record_str + str(record_int))

exit()

# file for each table; many blocks(identified by id) for each file
# these files needs to be created inside a folded named $db
fm = FileMgr('simpledb', 400, 8) # Kernel page size; usually 4096 bytes
b1 = Block('testfile', 2) 
p1 = Page(fm.block_size)
pos = 88 # position relative to the current block, so should always be between 0 <= block_size < 400
new_pos = pos + p1.setData(pos, 'abcdefghijklm')
p1.setData(new_pos, 345)
fm.write(b1, p1)

temp_page = Page(fm.block_size)
fm.read(b1, temp_page)
print(temp_page.getStr(pos))
print(temp_page.getInt(new_pos))
        
