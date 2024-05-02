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
        elif isinstance(data, str):
            data_bin = data.encode('utf-8')
            data_bin_len = int.to_bytes(len(data_bin), 4, 'big') 
            data_bin = data_bin_len + data_bin
        else:
            raise Exception('Page does not allow inserting ' + type(data) + ' type yet.')
            
        data_len = len(data_bin)
        self.bb[start:start + data_len] = data_bin
        return data_len 


    def getStr(self, start):
        str_len = self.getInt(start)
        return self.bb[start+4 : start+4+str_len].decode()
    
    def getInt(self, start):
        return int.from_bytes(self.bb[start : start+4], 'big')


class FileMgr:
    def __init__(self, db_name, block_size, buffer_size):
        self.db_name = db_name,
        self.block_size = block_size
        self.buffer_size = buffer_size

    def read(self, block, page):
        f = open(block.file_name, 'rb', buffering=0) # does buffering has any effect on reading?
        f.seek(self.block_size * block.block_number)
        # Making sure we are only reading the block size of the file
        # We want to minimize the number of blocks we are reading from the disk
        # One way query optimize will make plan based on number of potential blocks we need to ready
        page.bb = f.read(self.block_size) 
    
    def write(self, block, page):
        f = open(block.file_name, 'w+b', buffering=0)
        f.seek(self.block_size * block.block_number)
        f.write(page.bb) # only writing the block size of the file

    def append(self, fileName):
        f = open(fileName, 'a+b', buffering=0) # How does append mode behave if file does not exists?
        f.seek(self.block_size * self.length(fileName))
        temp_page = Page(self.block_size)
        f.write(temp_page.bb)

    def length(self, fileName):
        import os
        return os.path.getsize(fileName) // self.block_size
        

fm = FileMgr('simpledb', 400, 8) # Kernel page size; usually 4096 bytes

# file for each table; many blocks(identified by id) for each file
# these files needs to be created inside a folded named $db
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



        
