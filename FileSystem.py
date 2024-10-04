import os
import threading
import logging
db_logger = logging.getLogger('SimpleDB')

class Block:
    def __init__(self, file_name, block_number):
        self.file_name = file_name
        self.block_number = block_number

    def __eq__(self, other):
        return self.file_name == other.file_name and self.block_number == other.block_number

    # called by repr function
    def __repr__(self):
        return "[file name: " + self.file_name + ", block num: " + str(self.block_number) + "]"

    def __hash__(self):
        return hash((self.file_name, self.block_number))

    # When printed directly
    def __str__(self):
        return "[file name: " + self.file_name + ", block num: " + str(self.block_number) + "]"

    # def __str__(self):
    #     return "[file name: " + self.file_name + ", block num: " + str(self.block_number) + "]"

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

    # Write the data at an offset; int is written as is, but str and byte gets its size appended at the beginning
    # callee is responsible to ensure there are required space for the data
    # If there is not enough room to write the data in the current page;
    #   we will need to append a new block; similar to LogMgr.appendLog(b'log_record')
    def setData(self, start, data):
        if isinstance(data, int):
            data_bin = data.to_bytes(4,
                                     'big')  # chosing to convert integer to 4 bytes in big endian, which is the same way we write and read numbers
        elif isinstance(data, str):  # for type str
            data_bin = data.encode('utf-8')
            data_bin_len = int.to_bytes(len(data_bin), 4,
                                        'big')  # size in byte is the same as the length of the string because I am hoping the string content will fall into ascii range
            data_bin = data_bin_len + data_bin
        else:  # types i.e. bytes or bytearray
            data_bin_len = int.to_bytes(len(data), 4, 'big')
            data_bin = data_bin_len + data

        data_len = len(data_bin)
        # Do I need to create + new block to the file for data that exceeds boundary?
        self.bb[start:start + data_len] = data_bin
        return data_len

    def getStr(self, start):
        str_len = self.getInt(start)
        return self.bb[start + 4: start + 4 + str_len].decode()

    def getInt(self, start):
        return int.from_bytes(self.bb[start: start + 4], 'big')

    def getByte(self, start):
        byte_len = self.getInt(start)
        return self.bb[start + 4: start + 4 + byte_len]


# Purpose of this class is to write a page to a block
# Read and trigger immediate disk operation( because buffering it set to 0) to ensure data is saved to disk
class FileMgr:
    # https://stackoverflow.com/questions/1466000/difference-between-modes-a-a-w-w-and-r-in-built-in-open-function
    def __init__(self, db_name, block_size):
        import os
        self.db_exists = os.path.isdir(db_name)
        if not self.db_exists:
            os.mkdir(os.getcwd() + '/' + db_name)

        os.chdir(os.getcwd() + '/' + db_name)
        # TODO: remove any leftover table?

        self.block_size = block_size
        self._lock = threading.Lock()

    def readBlockToPage(self, block, page):
        with self._lock:
            self.length(
                block.file_name)  # TODO: hack to create an emptyÎ file if none exists; this needs to be replaced with a list of file handle that cache opened file handles
            f = open(block.file_name, 'rb', buffering=0)  # Does buffering has any effect on reading?
            f.seek(self.block_size * block.block_number)
            # Making sure we are only reading the block size of the file
            # We want to minimize the number of blocks we are reading from the disk
            # One way query optimize will make plan based on number of potential blocks we need to ready

            # (I think I am emulating the Java version with this if statement here)
            # if we are reading 10th block of an empty file; we return a zeroed out page
            file_content = bytearray(f.read(self.block_size))
            if file_content:
                page.bb = file_content
            else:
                page.bb = bytearray(self.block_size)
            f.close()

    # if file does not exist we create a new one
    def writePageToBlock(self, block, page):
        with self._lock:
            db_logger.info('Disk write of ' + str(block))
            f = open(block.file_name, 'r+b', buffering=0)  # r is used because a prevents seek and w truncates the file
            f.seek(self.block_size * block.block_number)
            f.write(page.bb)
            f.close()

    # Append a new block to the provided (log) file and return the block reference
    def appendEmptyBlock(self, fileName):
        with self._lock:
            f = open(fileName, 'ab', buffering=0)  # How does append mode behave if file do not exists?
            new_block_number = self.length(fileName)
            f.seek(self.block_size * new_block_number)  # seek doesn't with a mode
            temp_page = Page(self.block_size)
            f.write(temp_page.bb)
            f.close()
        return Block(fileName, new_block_number)

    def length(self, file_name):
        """return the length of file in terms of block. Access through transaction to ensure thread safety."""
        try:
            return os.path.getsize(file_name) // self.block_size
        except:
            # trying to get number of block in a file that doesn't exist
            # context manager cleanup resources
            with open(file_name, 'wb', buffering=0):
                pass
            return 0  # New file don't have any block in it