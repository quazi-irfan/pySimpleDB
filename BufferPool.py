from FileSystem import *
import time
import logging
from typing import List
db_logger = logging.getLogger('SimpleDB')

# Writing log page can happen in following instances
#   Appending new log to page, but the current page is full
#   Commiting transaction trigger two separate log flush
#       flushing associated buffer trigger flushing associated logs first
#       After buffers associated a transaction is flushed, final transaction commit log is also flushed to disk
class LogMgr:
    """
    DB uses a single instance of log manger to write to the log file. Log manager does three things,

    - appends blocks to log file if there is not enough blocks to write
    - maintains a page which is a copy of the last block of the log file,
    - appends log records(byte array) to the log page, and
    - provides an iterator to go over the log file from most recent to the least recent records
    """
    # Needs access to file manager because if no log file is present we make one with given block size
    def __init__(self, file_mgr, log_file):
        self._lock = threading.Lock()

        self.file_mgr: FileMgr = file_mgr
        self.log_file = log_file

        self.current_lsn = 0 # Start at 0, but gets set to 1 after adding the first record
        self.last_saved_lsn = 0  # last_flushed_lsn

        # LogMgr caches the last block from the log file to minimize disk seek
        self.log_page = Page(self.file_mgr.block_size)
        log_block_count = self.file_mgr.length(self.log_file)

        if log_block_count == 0:
            # Log file is empty with zero blocks
            self.log_block = self.file_mgr.appendEmptyBlock(self.log_file)
            self.log_page.setData(0, self.file_mgr.block_size)
            self.file_mgr.writePageToBlock(self.log_block, self.log_page)
        else:
            # Log file is not empty; therefore read the last block to a page
            self.log_block = Block(self.log_file, log_block_count - 1)
            self.file_mgr.readBlockToPage(self.log_block, self.log_page)


    # add b'log_record' to current log_page and return current_lsn
    # Only constrain is log_record must fit inside a single log page
    def appendLog(self, log_record):
        """Append log record to current log page. If log record does not fit in the current page, append a new block."""
        with self._lock:
            boundary = self.log_page.getInt(0)
            bytes_needed = len(log_record) + 4  # for writing length of binary blob

            # check if there is room for the new log record on the current page
            if boundary - bytes_needed < 4:  # first 4 bytes are reserved
                self.flushPage()
                self.log_block = self.file_mgr.appendEmptyBlock(self.log_file)  # appendNewBlock() function in the book
                self.log_page = Page(self.file_mgr.block_size)  # not present in the book
                self.log_page.setData(0, self.file_mgr.block_size)  # at the beginning the page is empty
                boundary = self.log_page.getInt(0)
                self.file_mgr.writePageToBlock(self.log_block, self.log_page)  # writing the newly created block/page immidiately emulates always having the latest block/page in log_block/log_page

            offset = boundary - bytes_needed
            self.log_page.setData(offset, log_record)
            self.log_page.setData(0, offset)  # Update offset for the next write
            self.current_lsn += 1
            return self.current_lsn

    # Log manager manually decides when to write the page to disk
    # flushPage takes optional log serial number to determine if there is any new log to warrant a disk write
    def flushPage(self, at_lsn=None):
        """Flush all record until at_lsn"""
        # flush log page
        if at_lsn is None:
            self.file_mgr.writePageToBlock(self.log_block, self.log_page)
            self.last_saved_lsn = self.current_lsn  # because we will be flushing all logs from the single log page
            return

        # flush log page only if there are un-flushed log records
        if self.last_saved_lsn < at_lsn:
            self.flushPage()

    # this is a stateful function; depends on what block log manager is currently working on
    def iterator(self):
        """Get an iterator that is pointing to the last block"""
        self.flushPage()  # we flush the log page to ensure iteration goes over all log records
        return LogIter(self.file_mgr, self.log_block)  # Start at the current block and read backward until the first block


class LogIter:
    def __init__(self, fm, block):
        self.fm = fm
        self.block = block

    def __iter__(self):
        self.temp_page = Page(self.fm.block_size)
        self.fm.readBlockToPage(self.block, self.temp_page)
        self.current_offset = self.temp_page.getInt(0)
        return self  # returning self because in each loop self.__next__ will be called

    # log records are read in reverse order in which they were written
    # when there are no more records, move to the next block
    def __next__(self):
        if self.current_offset >= self.fm.block_size:  # reached at the end of the block
            self.block = Block(self.block.file_name, self.block.block_number - 1)  # TODO: Why -1? Doesn't block number start
            if self.block.block_number < 0:
                raise StopIteration()
            else:
                self.fm.readBlockToPage(self.block, self.temp_page)
                self.current_offset = self.temp_page.getInt(0)

        log_record = self.temp_page.getByte(self.current_offset)
        self.current_offset = self.current_offset + len(log_record) + 4  # 4 bytes tho skip the length of the Byte blob
        return log_record


# pins page to block and tracks pin count
class Buffer:
    """
    Every buffer has some associated information,
     - Which transaction modified it and the associated log serial number.
     - How many transaction are currently pinning a buffer
     - When the buffer was unpinned

    assignToBlock use by buffer manager's pin method

    flushDirtyBufferWithLog use by both buffer(when new blog is beign assigned) and buffer manager(when all buffers are being flushed)

    setModified use by transaction when log entry is generated during updating the buffer
    """
    def __init__(self, fm, lm):
        self.fm: FileMgr = fm
        self.lm: LogMgr = lm

        self.block = None
        self.page = Page(fm.block_size)

        self.lsn = -1 # TODO: I think this should be part of buffer pool class because this value do not get reset once new buffer is pinned
        self.txnum = -1
        self.pin_count = 0
        self.time_pinned = time.time_ns()

    # TODO when we might call it as setMod(x, 0)
    def setModified(self, txnum, lsn):  # once Transaction sets data, it updates the txnum that updated the buffer, and pos lsn if it was loggable activity
        """Used by Transaction class """
        self.txnum = txnum
        # Sometimes modification to buffer is not logged, i.e. when formatting a new block
        # In that case transaction call this method with lsn set to -1
        if lsn >= 0:
            self.lsn = lsn

    def assignToBlock(self, block):
        """Used internally when buffer manager pins a block"""
        self.flushDirtyBufferWithLog()
        self.block = block
        self.fm.readBlockToPage(block, self.page)  # save the requested block to the Buffer's page
        # why we are not incrementing pin count anytime we are reading a block;
        # because, pin count zero could also mean all clients that was using this buffer no longer need it anymore
        self.pin_count = 0
        self.time_pinned = time.time_ns()

    def flushDirtyBufferWithLog(self):
        """
        Flush buffer only if it was modified by a transaction

        We flush the log before we flush the buffer.
        Since our log is append only, this setup works as a WAL - write ahead log.
        In the event we fail to flush the buffer WAL can be used to reconstruct the buffer.
        """
        if self.txnum >= 0:
            # write ahead log; anytime we are about to flush a buffer; FLUSH THE LOG FIRST
            # This ensures Page 116, (b) doesn't happen when buffer on disk has the data but log do not
            # this line flush (write-ahead) log upto the lsn that modified this buffer
            self.lm.flushPage(self.lsn)  # WRITE AHEAD LOG

            self.fm.writePageToBlock(self.block, self.page)
            self.txnum = -1
        # else nothing has happened yet, therefore there is nothing to flush

    def pin(self):
        self.pin_count += 1

    def unpin(self):
        self.pin_count -= 1

    def __repr__(self):
        return f"[Block: ({str(self.block.file_name)},{str(self.block.block_number)}), lsn: {str(self.lsn)}, txnum: {str(self.txnum)}, pin_count: {str(self.pin_count)},time_pinned: {str(self.time_pinned)}]"

# BufferMgr pins Block(which returns a Buffer ref); The Buffer ref is used to unpin the buffer
# BufferMgr does two things.
#   track changes to page(new data) and
#   (delay) write the modified page back to disk. Write happens when
#       1. page is getting pined to a diff block,
#       2. Recovery manager needs to write pages to prevent data loss

# BufferMgr allows multiple clients to access the buffer pool
# Client ask BufferMgr to pin a block to page
# block is already in a page
#   - and that buffer pinned
#   - and that buffer is not pinned
# block is not in any page (we have to evict a page)
#   - all buffer is the buffer pool is pinned
#   - at least one buffer is the buffer pool is not pinned
class BufferMgr:
    """
    Buffer Manager maintains a pool of buffers.
    buffer_ref = bm.pin(Block())
    buffer_ref.pin()
    bm.unpin(buffer_ref)

    Buffers flush can happen in two occasion,
     - Different block will get pinned
     - Recovery manager needs to flush the block during transaction commit
    """
    # lm gets passed to Buffer class to flush dirty log block
    # fm gets passed to Buffer class to write buffer to page

    WAIT_TIME = 10

    def __init__(self, fm, lm, num_buffers):
        self._condition = threading.Condition()  # Condition is event and lock combined

        self.fm = fm
        self.lm = lm
        self.num_buffers = num_buffers

        # Here are are initiating buffer, but it doesn't do much since the block and page information are filled out later bm is pinning
        self.buffer_pool: List[Buffer] = [Buffer(self.fm, self.lm) for _ in range(self.num_buffers)]
        self.pool_availability = self.num_buffers

    def flushAll(self, at_txnum):
        """Flush all pages modified by a given transaction"""
        with self._condition:
            for b in self.buffer_pool:
                if b.txnum == at_txnum:
                    b.flushDirtyBufferWithLog()

    # takes buffer; returns nothing
    def unpin(self, target_buffer: Buffer):
        db_logger.info('Unpinning ' + str(target_buffer.block))
        with self._condition:
            target_buffer.unpin()
            if not target_buffer.pin_count > 0:
                self.pool_availability += 1  # No client is using it. New request to pin is now eligible to replace this buffer
                self._condition.notify_all()  # wakes up thread waiting on the condition variable; but the lock is not yet released
        # Lock is released after we exit the context manager
        db_logger.info('Unpinned ' + str(target_buffer.block))

    # takes block; returns buffer
    def pin(self, target_block):
        db_logger.info('Pinning ' + str(target_block))
        with self._condition:
            b = self.tryToPin(target_block)
            start = time.time()
            while not b and (time.time() - start) < BufferMgr.WAIT_TIME:  # not b part is a escape hatch
                # Release lock + current thread is put to sleep.
                # This thread auto wake up after 2 sec and try to pin block again
                # This thread also wake up notify_all() is called
                self._condition.wait(2.0)
                b = self.tryToPin(target_block) # This line is hit once every 2 second or when notify_all() is called
            # we tried to pin a few times, and it has been over 10 seconds
            # this can be None because multiple threads are racing to get a buffer
            if not b:
                # More in Ch 14 Buffer Utilization
                raise Exception("Buffer pinning failed because buffer pool is full. Aborted transaction should be rolled back.")
        db_logger.info('Pinned ' + str(target_block))
        return b

    def tryToPin(self, target_block):
        b = self.findExistingBuffer(target_block)  # check if the requested block is already present in the buffer pool
        if not b:
            db_logger.info('Not in buffer pool ' + str(target_block))
            b = self.chooseUnpinnedBuffer()  # requested block is not already in the buffer pool; so find an unpinned buffer
            if not b:
                return None  # requested block is neither in buffer pool nor we have any unpinned buffer
            b.assignToBlock(target_block)  # DISK WRITE (Page 89, final paragraph); found an unpinned buffer; replace its page with requested block

        # if block was already in buffer pool with pin_count,
        #   non-zero; we do not lose pool availability yet because someone else was already using it
        #   zero; we still will lose pool availability because we are about to pin the buffer
        if b.pin_count == 0:
            self.pool_availability -= 1

        b.pin()
        return b

    # check if the requested block is already present in the buffer pool
    def findExistingBuffer(self, target_block):
        for b in self.buffer_pool:
            if b.block and (b.block == target_block):
                return b
        return None

    # requested block is not already in the buffer pool; so find an unpinned buffer
    # https://ksiresearch.org/seke/seke22paper/paper141.pdf
    def chooseUnpinnedBuffer(self):
        current_time = time.time_ns()
        time_delta = -1
        target_buffer_index = None
        for i in range(len(self.buffer_pool)):
            # TODO: Found a buffer that was never used; Could it actually take place?
            # if not self.buffer_pool[i].time_unpinned:
            #     return self.buffer_pool[i]

            # TODO: pin_count = 0 implies no tx pinned any block to this buffer yet
            # Select the buffer index that was least recently used
            if self.buffer_pool[i].pin_count == 0 and current_time - self.buffer_pool[i].time_pinned >= time_delta:
                target_buffer_index = i
                time_delta = current_time - self.buffer_pool[i].time_pinned

        if target_buffer_index is not None:
            db_logger.info('Replacing buffer at index ' + str(target_buffer_index)) # TODO: How to make db_logger work when BufferPool run with __main__
            return self.buffer_pool[target_buffer_index]
        else:
            return None

if __name__ == '__main__':
    fig = [4.5, 4.11, 4.12, 401][3]

    if fig == 4.12:
        # Fig 4.12 Testing Buffer Manager
        fm: FileMgr = FileMgr('simpledb', 400)
        lm: LogMgr = LogMgr(fm, 'simpledb.log')
        bm: BufferMgr = BufferMgr(fm, lm, 3)
        buff = []
        buff.append(bm.pin(Block('testfile', 0)))
        buff.append(bm.pin(Block('testfile', 1)))
        buff.append(bm.pin(Block('testfile', 2)))
        bm.unpin(buff[1])  # unpin testfile, 1
        buff[1] = None
        buff.append(bm.pin(Block('testfile', 0)))  # no effect
        buff.append(bm.pin(Block('testfile', 1)))  # pin testfile, 1 again
        print('Available buffer count: ' + str(bm.pool_availability))
        try:
            print("Attempting to pin block 3...")
            buff.append(bm.pin(Block('testfile', 3))) # Fail pin
        except Exception as e:
            print("Exception: " + str(e))
        bm.unpin(buff[2])  # unpin testfile, 2
        buff[2] = None
        buff.append(bm.pin(Block('testfile', 3)))  # Success pin testfile, 3

        print("Final buffer allocation.")
        for i in range(len(buff)):
            if buff[i]:
                print('buff[' + str(i) + '] pinned to block ' + str(buff[i].block))

    elif fig == 4.11:
        # Fig 4.11 Testing Buffer
        fm = FileMgr('simpledb', 400)
        lm = LogMgr(fm, 'simpledb.log')
        bm = BufferMgr(fm, lm, 3)
        buff1 = bm.pin(Block('testfile', 1))
        n = buff1.page.getInt(80)  # returns 0
        buff1.page.setData(80, n + 1)
        buff1.setModified(1, 0)  # transaction 1, lsn 0; we do this otherwise buffer can't be flushed without tx number (flushDirtyBufferWithLog)
        print('The new value is ', n + 1)
        bm.unpin(buff1)  # we do not immediately write it back to disk because some other client might pin it again

        # Pulling in three blocks will evict buff1 since size of buffer pool is 3
        buff2 = bm.pin(Block('testfile', 2))
        buff3 = bm.pin(Block('testfile', 3))
        buff4 = bm.pin(Block('testfile', 4))

        bm.unpin(buff2)
        buff11 = bm.pin(Block('testfile', 1))
        buff11.page.setData(80, 9999)
        buff11.setModified(1, 0)
        buff11.unpin()  # This modification won't get written to disk because there is nothing forcing buffer manager to do so

    elif fig == 4.5:
        # Fig 4.5 Testing Log Manager
        fm = FileMgr('simpledb', 400)  # Kernel page size; usually 4096 bytes
        lm = LogMgr(fm, 'simpledb.log')

        def createLogRecord(s, i):
            temp_bytearray = bytearray(4 + len(s) + 4)  # length of string + string + one number
            temp_page = Page(temp_bytearray)  # creating page with desired size because
            pos = temp_page.setData(0, s)
            temp_page.setData(pos, i)
            lsn = lm.appendLog(temp_page.bb) # We are only writing to log file in this test
            return lsn


        for i in range(1, 36):
            lsn = createLogRecord('record' + str(i), i + 100)
            print('Adding ' + '(lsn: ' + str(lsn) + '): \t' + 'record' + str(i) + str(i + 100))

        for l in lm.iterator():
            temp_page = Page(l)  # We have keep it in memory to parse its content
            record_str = temp_page.getStr(0)
            record_int = temp_page.getInt(
                4 + len(record_str))  # also need to add 4 byte for the recoded length of the string
            print('Reading:  ' + record_str + str(record_int))

        for i in range(36, 71):
            lsn = createLogRecord('record' + str(i), i + 100)
            print('Adding ' + '(lsn: ' + str(lsn) + '): \t' + 'record' + str(i) + str(i + 100))

        for l in lm.iterator():
            temp_page = Page(l)  # We have keep it in memory to parse its content
            record_str = temp_page.getStr(0)
            record_int = temp_page.getInt(
                4 + len(record_str))  # also need to add 4 byte for the recoded length of the string
            print('Reading:  ' + record_str + str(record_int))

    elif fig == 401:
        # BufferPool LRU test
        fm = FileMgr('simpledb', 400)
        lm = LogMgr(fm, 'simpledb.log')
        bm = BufferMgr(fm, lm, 3)

        buff1 = bm.pin(Block('testfile', 1))
        buff2 = bm.pin(Block('testfile', 2))
        buff3 = bm.pin(Block('testfile', 3))
        print([buff for buff in bm.buffer_pool])
        buff3.unpin()
        buff2.unpin()
        buff4 = bm.pin(Block('testfile', 4)) # will replace least recently used buff3
        print([buff for buff in bm.buffer_pool])
        buff1.unpin()
        buff5 = bm.pin(Block('testfile', 5)) # will replace least recently used buff2
        print([buff for buff in bm.buffer_pool])
