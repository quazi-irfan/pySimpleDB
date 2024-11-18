import os
import threading
import logging
import math
db_logger = logging.getLogger('SimpleDB')

# hexdump -C testfile Each line contains 16 bytes; each group of represents 1 byte
# A 4 byte int will take up 00 00 00 00
# int value 4 will be encoded as 00 00 00 04
# int value 10 will be encoded as 00 00 00 0a
# int value -1 will be encoded as ff ff ff ff

# cmp -l bin1 bin2 to compare two binary files
# file system allows accessing raw disk in blocks.
# in db, each file is treated like a raw disk
# db access each file by virtual blocks(OR translates these blocks into physical blocks using file system)
# db reads block into pages
# db maintain a pool of pages in memory
# db read and write is done on those poled pages
# By only operating in memory, db can control when writing to disk is happening
# Each table, table index and db log are stored in a single file
# Postgresql uses 8kB as block size

# Block index start at 0
class Block:
    """
    Block represents raw block of a file.

    It contains file name and block number.
    """
    def __init__(self, file_name, block_number):
        self.file_name = file_name
        self.block_number = block_number # Block number -1 is a special marker block to prevent conflicting file modification by concurrent transactions

    def __eq__(self, other):
        return self.file_name == other.file_name and self.block_number == other.block_number

    # called by repr function
    def __repr__(self):
        return "[file name: " + self.file_name + ", block num: " + str(self.block_number) + "]"

    def __hash__(self):
        return hash((self.file_name, self.block_number))

    # When printed directly
    def __str__(self):
        return "[file: " + self.file_name + ", block: " + str(self.block_number) + "]"

    # def __str__(self):
    #     return "[file name: " + self.file_name + ", block num: " + str(self.block_number) + "]"

    # implement __eq__, __hash__, __str__


# FileMgr : readBlock(block_number, blockSize) and into a page
# here we are assuming one to one size corrospondence between block and page size

# We populate data in a bytearray and write it to file which triggers sys call to write
# f = open('all_tables', 'wb', buffering=0)
# f.write(page) # will trigger system write as there is no buffering
class Page:
    """
    Page is in-memory blocks. Blocks are read as a byte array into pages.

    This class provides convenience function to read and write to the byte array.
    """
    # Page(int) trigger allocation of bytearray(length) in memory
    def __init__(self, data):
        # get either size to create an empty page, or data that needs to put in a page
        # bytearray(data) allocates new one in memory - usage is heavily controlled by Buffer manager
        # self.bb = data uses what is already in the memory
        # log manager(saves it log in memory, and dumps to this page) send bytearay;
        # buffer manager send length of bytearray
        self.bb = data if isinstance(data, bytearray) else bytearray(data)

    # Write the data at an offset; int is written as is, but str and byte gets its size appended at the beginning
    # callee is responsible to ensure there are required space for the data
    # If there is not enough room to write the data in the current page;
    #   we will need to append a new block; similar to LogMgr.appendLog(b'log_record')
    def setData(self, start, data):
        """
        TODO: Error bound needed. Otherwise,
        x = bytearray(5)
        x[2:10] = b'abcefabcd'
        len(x) # 11

        (0).to_bytes(4, byteorder='big', signed=True)               b'\x00\x00\x00\x00'
        ...                                                         ...
        (2147483647).to_bytes(4, byteorder='big', signed=True)      b'\x7f\xff\xff\xff'  (little endian b'\x7f\xff\xff\xff')
        (-2147483648).to_bytes(4, byteorder='big', signed=True)     b'\x80\x00\x00\x00'
        ...                                                         ...
        (-1).to_bytes(4, byteorder='big', signed=True)              b'\xff\xff\xff\xff'
        """
        if isinstance(data, int):
            data_bin = data.to_bytes(4,byteorder='big', signed=True)  # choosing to convert integer to 4 bytes in big endian, which is the same way we write and read numbers
        elif isinstance(data, str):  # for type str
            data_bin = data.encode('utf-8')
            data_bin_len = int.to_bytes(len(data_bin), 4,byteorder='big', signed=True)  # size in byte is the same as the length of the string because I am hoping the string content will fall into ascii range
            data_bin = data_bin_len + data_bin
        else:  # TODO: Is else always expecting bytearray?
            data_bin_len = int.to_bytes(len(data), 4, byteorder='big', signed=True)
            data_bin = data_bin_len + data

        data_len = len(data_bin)
        # Do I need to create + new block to the file for data that exceeds boundary?
        self.bb[start:start + data_len] = data_bin
        return data_len

    def getStr(self, start):
        str_len = self.getInt(start)
        return self.bb[start + 4: start + 4 + str_len].decode()

    def getInt(self, start):
        return int.from_bytes(self.bb[start: start + 4], byteorder='big', signed=True) # TODO When reading string/bytes length signed=False needs to be set

    def getByte(self, start):
        """I think only log manager reads and write raw bytes"""
        byte_len = self.getInt(start)
        return self.bb[start + 4: start + 4 + byte_len]


# Purpose of this class is to write a page to a block
# Read and trigger immediate disk operation( because buffering it set to 0) to ensure data is saved to disk
class FileMgr:
    """
    Any interaction with file system is handled by this class.
    Interaction includes reading/writing file block into page. It also includes getting the number of blocks in a file and appending if needed.
    For example when log manager is writing to log file or buffer manager is flushing all buffers of a transaction.

    TODO Caution: This class overwrites existing files in getFileHandle. Meaning, file handles are lost when db server die. But then how does server restart is hande gracefully?
    """
    # https://stackoverflow.com/questions/1466000/difference-between-modes-a-a-w-w-and-r-in-built-in-open-function
    def __init__(self, db_name, block_size):
        self.db_name = db_name
        self.db_exists = os.path.isdir(self.db_name)
        if not self.db_exists:
            os.mkdir(os.getcwd() + '/' + self.db_name)

        os.chdir(os.getcwd() + '/' + self.db_name)

        self._fileHandles = {}
        self.block_size = block_size
        self._lock = threading.Lock()

    def readBlockToPage(self, block, page):
        with self._lock:
            f = self.getFileHandle(block.file_name)
            f.seek(self.block_size * block.block_number)
            # Making sure we are only reading the block size of the file
            # We want to minimize the number of blocks we are reading from the disk
            # One way query optimize will make plan based on number of potential blocks we need to ready

            # (I think I am emulating the Java version with this if statement here)
            # if we are reading 10th block of an empty file; we return a zeroed out page
            file_content = bytearray(f.read(self.block_size))
            #TODO: find out why this conditional statement was introduced? Current setup follows the book.
            # if file_content:
            page.bb = file_content
            # else:
            #     page.bb = bytearray(self.block_size)

    # if file does not exist we create a new one
    def writePageToBlock(self, block, page):
        with self._lock:
            db_logger.info('Disk write of ' + str(block))
            f = self.getFileHandle(block.file_name)  # r is used because a prevents seek and w truncates the file
            f.seek(self.block_size * block.block_number)
            f.write(page.bb)

    # Append a new block to the provided (log) file and return the block reference
    def appendEmptyBlock(self, fileName): # TODO: if we are adding removeBlock(to pair to logging block removal) then it makes sense to rename this to appendBlock
        with self._lock:
            f = self.getFileHandle(fileName)
            new_block_number = self.length(fileName)
            f.seek(self.block_size * new_block_number)
            temp_page = Page(self.block_size)
            f.write(temp_page.bb)
        return Block(fileName, new_block_number)

    def removeBlock(self, fileName, block):
        """
        Call this function form tx.remove/removeBlock
        """
        pass

    def length(self, file_name):
        """return the length of file in terms of block. Access through transaction to ensure thread safety."""
        f = self.getFileHandle(file_name)
        return math.ceil(f.seek(0, os.SEEK_END) / self.block_size)

    def getFileHandle(self, file_name):
        if file_name not in self._fileHandles:
            if not os.path.exists(file_name):
                open(file_name, 'wb', buffering=0).close()
            self._fileHandles[file_name] = open(file_name, 'r+b', buffering=0) # Disable buffering
        return self._fileHandles[file_name]

# 3.12 Testing file manager
# File for each table; many blocks(identified by id) for each file
# these files needs to be created inside a folded named $db
if __name__ == '__main__':
    fm = FileMgr('filetest', 400)  # Kernel page size; usually 4096 bytes
    b1 = Block('testfile', 2)
    p1 = Page(fm.block_size)
    pos = 88  # position relative to the current block, so should always be between 0 <= block_size < 400
    new_pos = pos + p1.setData(pos, 'abcdefghijklm')
    p1.setData(new_pos, 345)
    fm.writePageToBlock(b1, p1)  # won't work because r+b is expecting the file to exists; in LogTest we are creating the empty file in appendEmptyBlock

    temp_page = Page(fm.block_size)
    fm.readBlockToPage(b1, temp_page)
    print(temp_page.getStr(pos))
    print(temp_page.getInt(new_pos))