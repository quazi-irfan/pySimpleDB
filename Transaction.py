# https://www.amazon.com/Principles-Transaction-Processing-Kaufmann-Management/dp/1558606238
# https://www.amazon.com/Transaction-Processing-Concepts-Techniques-Management/dp/1558601902
# https://www.amazon.com/Concurrency-Control-Recovery-Database-Systems/dp/0201107155
# https://www.cs.purdue.edu/homes/bb/cs542-06Spr-bb/SCDU-Papa-79.pdf

from BufferPool import *
import time
import logging
db_logger = logging.getLogger('SimpleDB')

# Original MVCC Paper https://dl.acm.org/doi/abs/10.1145/356842.356846
# TODO: MVCC implementation; Read only tx can see
# Modification of Transaction.commit() for write query
#   Record when write tx ends
#   Add that timestamp to the COMMIT log record
#   Add that same timestamp to each block the tx have a lock on
# Modification on BufferList.pin() for read query
#   Record when read tx start as t
#   Copy the current block(be in from disk or in-memory) to a new page
#   Get a list of transaction that commited after time t
#   Get a list of incomplete transaction started after time t
#   Undo any changes made by any of these two above type of transactions

# In the book, this was originally an interface with op(), txNumber() and undo() specific
# Classes extending this interface also have
#   static writeToLog() to build log byte array
#   Constructor to parse log byte array and extract txnum, block info, block offset, value
#       These parsed values are to be used by aforementioned op(), txNumber() and undo() method
class LogRecord:
    """
    Collection of static utility function to read and write to log file.

    writeToLog(dict) writes to log file
    createLogRecord(bytearray) returns the values from log records in a tuple
    undo(tx, [log params]) undo changes made to a buffer
    toString(bytearray) returns a string representation of a log record
    """
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5

    # write log(byte array) from log parameters; return lsn
    #   writeToLog(lm=lm, op=LogRecord.SETINT, txnum=10, blk_file='log.file', blk_num=10, blk_offset=80, value=100)
    #   will generate appropriate log byte array and call lm.appendLog
    # Equivalent to book's static method writeToLog of SetStringRecord class
    @staticmethod
    def writeToLog(**log_param):
        if log_param['op'] == LogRecord.CHECKPOINT:
            op_offset = 0

            temp_page = Page(op_offset + 4)
            temp_page.setData(op_offset, log_param['op'])
            db_logger.info('Logging ' + LogRecord.toString(temp_page.bb))
            return log_param['lm'].appendLog(temp_page.bb)
        elif log_param['op'] == LogRecord.START or log_param['op'] == LogRecord.COMMIT or log_param['op'] == LogRecord.ROLLBACK:
            op_offset = 0
            txnum_offset = op_offset + 4

            temp_page = Page(txnum_offset + 4)
            temp_page.setData(op_offset, log_param['op'])
            temp_page.setData(txnum_offset, log_param['txnum'])
            db_logger.info('Logging ' + LogRecord.toString(temp_page.bb))
            return log_param['lm'].appendLog(temp_page.bb)

        # Log Record generation per data type
        elif log_param['op'] == LogRecord.SETSTRING:
            op_offset = 0
            txnum_offset = op_offset + 4
            blk_file_offset = txnum_offset + 4
            blk_num_offset = blk_file_offset + len(log_param['blk_file']) + 4
            blk_offset_offset = blk_num_offset + 4
            old_value_offset = blk_offset_offset + 4

            temp_page = Page(old_value_offset + len(log_param['old_val']) + 4)
            temp_page.setData(op_offset, log_param['op'])
            temp_page.setData(txnum_offset, log_param['txnum'])
            temp_page.setData(blk_file_offset, log_param['blk_file'])
            temp_page.setData(blk_num_offset, log_param['blk_num'])
            temp_page.setData(blk_offset_offset, log_param['blk_offset'])
            temp_page.setData(old_value_offset, log_param['old_val'])
            db_logger.info('Logging ' + LogRecord.toString(temp_page.bb))
            return log_param['lm'].appendLog(temp_page.bb)
        elif log_param['op'] == LogRecord.SETINT:
            op_offset = 0
            txnum_offset = op_offset + 4
            blk_file_offset = txnum_offset + 4
            blk_num_offset = blk_file_offset + len(log_param['blk_file']) + 4
            blk_offset_offset = blk_num_offset + 4
            old_value_offset = blk_offset_offset + 4

            temp_page = Page(old_value_offset + 4)
            temp_page.setData(op_offset, log_param['op'])
            temp_page.setData(txnum_offset, log_param['txnum'])
            temp_page.setData(blk_file_offset, log_param['blk_file'])
            temp_page.setData(blk_num_offset, log_param['blk_num'])
            temp_page.setData(blk_offset_offset, log_param['blk_offset'])
            temp_page.setData(old_value_offset, log_param['old_val'])
            db_logger.info('Logging ' + LogRecord.toString(temp_page.bb))
            return log_param['lm'].appendLog(temp_page.bb)

    # extract log parameters from log byte array
    # Used when iterating over binary log file, such as rollback and recovery
    # Equivalent: static method of LogRecord interface that returns instances such as SetStringRecord
    @staticmethod
    def createLogRecord(log_bytearray):
        temp_page = Page(log_bytearray)
        op = temp_page.getInt(0)
        if op == LogRecord.START or op == LogRecord.COMMIT or op == LogRecord.ROLLBACK:
            txnum = temp_page.getInt(4)
            return op, txnum
        elif op == LogRecord.CHECKPOINT:
            return op, -1 # checkpoint returns a dummy txnum, which is -1
        elif op == LogRecord.SETINT or op == LogRecord.SETSTRING:
            txnum = temp_page.getInt(4)
            blk_file = temp_page.getStr(8)
            blk_num = temp_page.getInt(8 + (len(blk_file) + 4))
            blk_offset = temp_page.getInt(8 + (len(blk_file) + 4) + 4)
            if op == LogRecord.SETINT:
                old_val = temp_page.getInt(8 + (len(blk_file) + 4) + 4 + 4)
            else:
                old_val = temp_page.getStr(8 + (len(blk_file) + 4) + 4 + 4)
            return op, txnum, blk_file, blk_num, blk_offset, old_val
        else:
            pass # TODO: Read log byte array to append block

    @staticmethod
    def undo(tx, *log_data):
        # TODO: We do not need to call LogRecord.createLogRecord; instead we can pass the log parameters using splat operator
        op, txnum, blk_file, blk_num, blk_offset, old_val = log_data

        # setInt will look for buffer in the pinned buffer list; this is in turns using BM to pin buffer
        temp_blk = Block(blk_file, blk_num)
        tx.pin(temp_blk)
        if op == LogRecord.SETINT:
            tx.setInt(temp_blk, blk_offset, old_val, False)
        elif op == LogRecord.SETSTRING:
            tx.setString(temp_blk, blk_offset, old_val, False)
        else:
            pass # TODO: byte type and block append
        tx.unpin(temp_blk)

    # extract log parameters from log byte array to build human-readable form
    @staticmethod
    def toString(log_bytearray):
        temp_page = Page(log_bytearray)
        op = temp_page.getInt(0)
        if op == LogRecord.CHECKPOINT:
            return '<CHECKPOINT>'
        elif op == LogRecord.START:
            txnum = temp_page.getInt(4)
            return '<START, ' + str(txnum) + '>'
        elif op == LogRecord.COMMIT:
            txnum = temp_page.getInt(4)
            return '<COMMIT, ' + str(txnum) + '>'
        elif op == LogRecord.ROLLBACK:
            txnum = temp_page.getInt(4)
            return '<ROLLBACK, ' + str(txnum) + '>'
        elif op == LogRecord.SETINT:
            txnum = temp_page.getInt(4)
            blk_file = temp_page.getStr(4 + 4)
            blk_num = temp_page.getInt(4 + 4 + (4 + len(blk_file)))
            blk_offset = temp_page.getInt(4 + 4 + (4 + len(blk_file)) + 4)
            old_val = temp_page.getInt(4 + 4 + (4 + len(blk_file)) + 4 + 4)
            return '<SETINT, ' + str(txnum) + ', ' + blk_file + ', ' + str(blk_num) + ', ' + str(blk_offset) + ', ' + str(old_val) + '>'
        elif op == LogRecord.SETSTRING:
            txnum = temp_page.getInt(4)
            blk_file = temp_page.getStr(4 + 4)
            blk_num = temp_page.getInt(4 + 4 + (4 + len(blk_file)))
            blk_offset = temp_page.getInt(4 + 4 + (4 + len(blk_file)) + 4)
            old_val = temp_page.getStr(4 + 4 + (4 + len(blk_file)) + 4 + 4)
            return '<SETSTRING, ' + str(txnum) + ', ' + blk_file + ', ' + str(blk_num) + ', ' + str(blk_offset) + ', ' + str(old_val) + '>'

class RecoveryMgr:
    """
    RecoveryMgr is responsible for logging that provides atomicity and durability for the database.
    Each tx gets it own RecoveryMgr instance. Multiple tx use their own RecoveryMgr to write to the same log file.
    All interation with the log file is done through the static utility function by LogRecord class.

    Each transaction gets its own recovery manager. That class is responsible for generating all log record for that transaction.
    For example, recovery manager constructor write the START tx log record.
    Other methods such as commit, rollback, recovery, setInt and setString write corresponding log records.

    During recovery(during startup) database does UNDO only recovery that goes over the log page once and undo any changes made by an incomplete tx.
    This is assuming any tx with commit/rollback records was completed successfuly.
    This also assumes any tx without commit/rollback records was not completed successfully, and changed made by this tx needs to be undone.

    (Pargraph 1 in page 113)
    UNDO only recovery requires guarantee that committed/roll-backed transaction flushed a COMMIT/ROLLBACK log in the log file.
    This is ensured by writing and flushing COMMIT/ROLLBACK message(before flushing buffer) as part of commit/rollback operation; implemented in RecoveryMgr.commit()

    (Pargraph 1 in page 116)
    UNDO only modification requires all changes to buffer has a corresponding update log entry.
    There can not some changes to a buffer without a corresponding log record.
    This is ensured by flushing log before flushing buffer, or known as write-ahead-log; implemented in Buffer.flushDirtyBufferWithLog()

    (Paragraph 2 in section 5.3.5 in page 115)
    It is inefficient to flush log and flush buffer with every change.
    To solve this, we maintain in memory log and buffers in-memory until,
    - buffer needed to be swapped out by a new block request (flush both log and buffer Buffer.assignToBlock()
    - log page is full (flush log only) LogMgr.appendLog()
    """
    def __init__(self, tx, txnum, lm, bm):
        self.tx: Transaction = tx
        self.txnum = txnum # TODO: redundant, txnum should be accessible from tx
        self.lm: LogMgr = lm
        self.bm: BufferMgr = bm

        LogRecord.writeToLog(lm=self.lm, op=LogRecord.START, txnum=self.txnum)

    # Undo only recovery explained in Fig 5.7
    # Undo only recovery algorithm forces all buffer to disk before writing(and flushing) the commit log
    # So, when recovering we can go backward and only undo changes if the transaction was not complete(commit/rollback)
    def commit(self):
        """
        (5.4.3.1 Undo Only recovery; following three lines corresponds three steps of fig 5.7)
        We guarantee durability by flushing a COMMIT log as part of commit operation.
        Encountering COMMIT log implies all previous logs entries have been flushed to disk because flushing by lsn implies all prior lsn have been flushed.
        """
        # Although this line will call lm.flushPage multiple times, not all will incur disk write because lsn is compared against last_saved_lsn
        self.bm.flushAll(self.txnum)
        # writeToLog writes to memory; flushPage writes it to disk
        lsn = LogRecord.writeToLog(lm=self.lm, op=LogRecord.COMMIT, txnum=self.txnum)
        self.lm.flushPage(lsn)

    # Since each Transaction has its own recovery manager, it should know what transaction we are in
    def rollback(self):
        """Makes a single backward pass in the log and undo all changed made by this tx until the START commit was found. Finally add ROLLBACK log record."""
        for l in self.lm.iterator():
            log_data = LogRecord.createLogRecord(l) # from byte array extract log record information
            op, txnum = log_data[0], log_data[1]
            if txnum == self.txnum:
                if op == LogRecord.START:
                    break
                LogRecord.undo(self.tx, *log_data)

        self.bm.flushAll(self.txnum) # TODO: Flushing buffers should not be mandatory here.
        lsn = LogRecord.writeToLog(lm=self.lm, op=LogRecord.ROLLBACK, txnum=self.txnum)
        self.lm.flushPage(lsn)

    def recover(self):
        """Makes a single backward pass in the log file and undoes any changes made by incomplete tx"""
        completed_tx = set()
        for l in self.lm.iterator():
            log_data = LogRecord.createLogRecord(l)
            op, txnum = log_data[0], log_data[1]
            if op == LogRecord.CHECKPOINT:
                break

            if op == LogRecord.COMMIT or op == LogRecord.ROLLBACK:
                completed_tx.add(txnum)
                continue
            elif txnum not in completed_tx and (op == LogRecord.SETINT or op == LogRecord.SETSTRING):
                LogRecord.undo(self.tx, *log_data)
            else:
                pass # such as <START, txnum> log record

        # Upon recovery completion; add checkpoint log
        self.bm.flushAll(self.txnum) # TODO: Flushing buffers should not be mandatory here.
        lsn = LogRecord.writeToLog(lm = self.lm, op = LogRecord.CHECKPOINT)
        self.lm.flushPage(lsn)

    # we want to save the old value in the log; so undo recovery can replace current value with this old value
    # Choosing to use static method instead of Book's SetIntRecord.writeToLog
    def setInt(self, target_buffer, block_offset):
        old_val = target_buffer.page.getInt(block_offset)
        return LogRecord.writeToLog(
            lm=self.lm,
            op=LogRecord.SETINT,
            txnum=self.txnum,
            blk_file=target_buffer.block.file_name,
            blk_num=target_buffer.block.block_number,
            blk_offset=block_offset,
            old_val=old_val
        )

    def setString(self, target_buffer, block_offset):
        old_val = target_buffer.page.getStr(block_offset)
        return LogRecord.writeToLog(
            lm=self.lm,
            op=LogRecord.SETSTRING,
            txnum=self.txnum,
            blk_file=target_buffer.block.file_name,
            blk_num=target_buffer.block.block_number,
            blk_offset=block_offset,
            old_val=old_val
        )

class LockTable:
    """
    Database maintains a single instance of LockTable.
    LockTable grants locks on block; and maintains list of all blocks with locks and their lock counts/status.
    Each tx has an instance of ConcurrencyMgr, and tx requests locks on block from LockTable through the ConcurrencyMgr instance.
    """
    import collections
    _all_locks = collections.defaultdict(int) # TODO: check if using defaultdict is introducing any bug

    def __init__(self):
        self._condition = threading.Condition()

    # similar to BufferMgr.pin
    def sLock(self, target_block):
        with self._condition:
            start = time.time()
            # we will wait if there is a xlock
            while LockTable._all_locks[target_block] < 0 and (time.time() - start) < 10: # <= is not necessary because -1 is used to identify xlock
                self._condition.wait(2.0) # within this 10 seconds, every 2 sec check if xlock was released

            # Since multiple threads are woken up at the same time; another thread might race first to xlock before this thread
            if LockTable._all_locks[target_block] < 0:
                raise Exception('Tx aborted because it waited to long to acquire slock or another Tx raced first to acquire the slock. Try again.')
            LockTable._all_locks[target_block] += 1

    # We use Approximate Deadlock Detection to prevent Tx from waiting to obtain for a lock for too long
    # Here, we prevent deadlock by aborting Tx that is waiting too long(10 sec) for a lock.
    # Long wait time doesn't mean deadlock, it could also mean another transaction has a lot to do
    # Meaning, our approach react to situation that could potentially lead to deadlock, which may or may not be an actual deadlock
    def xLock(self, target_block):
        """
        Behaves similar to BufferMgr.pin
        """
        with self._condition:
            start = time.time()
            # > 1 is because slock is obtained before attempting to xlock
            # meaning, if a tx has xlock on a block, it is implied that the tx also have slock on it
            while LockTable._all_locks[target_block] > 1 and (time.time() - start) < 10:
                self._condition.wait(2.0)

            # see sLock function doc
            if LockTable._all_locks[target_block] > 1:
                raise Exception('Tx aborted because it waited to long to acquire xlock or another Tx raced first to acquire the xlock. Try again.')
            LockTable._all_locks[target_block] = -1

    # release lock on a block
    def unlock(self, target_block):
        with self._condition:
            if LockTable._all_locks[target_block] > 0:
                LockTable._all_locks[target_block] -= 1
            else:
                # TODO: Maybe another alternative is set this entry to zero
                del LockTable._all_locks[target_block]
                self._condition.notify_all()

# Concurrency Manager responsible for correctly executing concurrent transaction.
# Each Transaction is a group of operation the behaves as a single operation.
# For example, T1 transaction is a group of two operations, and they are W(b1) and W(b2)
# We know the multiple transaction running in serial have their sub operations correctly scheduled - proof by contradiction.
# In this example, we have to run following two transactions needs to run concurrently.
# In serial schedule we will run all sub operation T1 to completion and before running any sub operation of T2.
# But we will have sub operations of T1 and T2 interleaved - run them in non-serial schedule.
# We need to find out a schedule that serializable.
# Meaning, running those sub operation interleaved or in non-serial schedule will be equivalent to running the transactions in series.

# Example, we want to find a schedule running the sub operations of T1 and T2.
# Correct non-serial schedule will be serializable.
#       T1: W(b1) W(b2)
#       T2: W(b1) W(b2)
# Example of non-serial schedule 1:
#   One example of running those sub operations is W1(b1) W2(b1) W1(b2) W2(b2)
#   This non-serial schedule IS serializable because this schedule is equivalent to running T1 first following by T2
# Example of non-serial schedule 2:
#   Another example of running those constituent operations in non-serial schedule is W1(b1) W2(b1) W2(b2) W1(b2)
#   This non-serial schedule IS NOT serializable, because at the end B1 blocks contains update from T1 and B2 block contains update from T2
#   In fact, in this case CM will grant T1 a xlock on B1. If CM also grant T2 a xblock on B2, then we have a deadlock.
#   Both transaction will be waiting for the other transaction to release their lock

# A non-serial schedule is called serializable if it produces some serial schedule.
# Concurrency Manager uses locking to ensure a non-serial schedule is serializable
# Any conflicting transactions are forced to run in series by the concurrency manager.
# When running multiple transaction CM is responsible for finding a non-serial schedule that is serializable
class ConcurrencyMgr:
    """
    Maintain the dict of all locks held by a transaction.
    sLock and xLock method create the following dict entries, and release method removed them all.
    tx_locks = { blk1: 'S', blk2: 'X'}
    """
    _global_locktable = LockTable() # ConcurrencyMgr.db_locktable

    def __init__(self):
        self.tx_locks = {}

    def sLock(self, target_block):
        if target_block not in self.tx_locks:
            ConcurrencyMgr._global_locktable.sLock(target_block)
            self.tx_locks[target_block] = 'S'

    def xLock(self, target_block):
        if not (target_block in self.tx_locks and self.tx_locks[target_block] == 'X'):
            # this block is already in the tx_locks list and we have xLock on it
            self.sLock(target_block) # TODO Why always acquire slock before obtaining xlock?
            ConcurrencyMgr._global_locktable.xLock(target_block)
            self.tx_locks[target_block] = 'X'

    def release(self):
        for block in self.tx_locks.keys():
            ConcurrencyMgr._global_locktable.unlock(block)
        self.tx_locks.clear()

class BufferList:
    """
    Captures block/buffer usage.
     - mapping to get buffer ref from block ref {blk1_ref : buf1_ref, ... }
     - block pin history [blk1, blk2, blk1, blk1, blk2, blk3, ...]
    """
    def __init__(self, bm):
        self.bm: BufferMgr = bm

        self.block_buffer_map = {}
        self.block_pin_history = []

    def pin(self, target_block):
        buf_ref = self.bm.pin(target_block)
        self.block_buffer_map[target_block] = buf_ref
        self.block_pin_history.append(target_block)

    def unpin(self, target_block):
        self.bm.unpin(self.block_buffer_map[target_block])
        self.block_pin_history.remove(target_block) # remove the first entry, one instance, of matching block
        if target_block not in self.block_pin_history:
            del self.block_buffer_map[target_block]

    def unpinAll(self):
        for blk in self.block_pin_history:
            self.bm.unpin(self.block_buffer_map[blk])
        self.block_buffer_map.clear()
        self.block_pin_history.clear()

    def getBuffer(self, target_block):
        return self.block_buffer_map[target_block]

# Transaction uses
#   Recovery Manager(read/write logs recording changes to buffer) and (Multiple transactions will write in the log at ths ame time)
#       Any uncommited transaction(either explicit rollback/system crash) must be undone properly by Recovery Manager
#   Concurrency manager to provide controlled access to buffers

# Normal db shutdown involves completing all transactions and flushing buffers into disk
class Transaction:
    """
    Transaction is a GROUP of operations that behaves as a SINGLE operation.
    Each client communicates to the database in a series of transaction.
    Opening a new transaction implies closing previous transaction.
    Transaction object starts by pinning a block and call setX/getX method on it.
    At the end we either commit or rollback the transaction.
    Terminating the database involves completing all pending transaction.

    tx = Transaction()
    tx.pin(blk = Block()) >>> BufferList.pin >>> BufferPool.pin
    tx.setInt(blk, offset, 10) >>> rm.setInt and buffer.setInt(to be implemented)
    tx.commit() >>> rm.commit(), cm.release() and bufferList.unpin(which is bufferPool.unpin)

    Transaction object uses,
    - RecoveryMgr object to write to log file
    - ConcurrencyMgr object to acquire/release locks on blocks before reading/writing to buffer
    - BufferList object to maintain a list of pinned blocks

    We interact with block references.
    These block references are used internally to work with buffer pool.
    For example, we use the block reference to get buffer reference. i.e. self.bufferList.getBuffer(target_block)

    LockTable maintains,
    - block lock status {blk1: 1, blk2:3, blk3:-1}
    ConcurrencyMgr maintains,
    - per transaction block lock status { blk1: 'S', blk2: 'X'}
    - Uses LockTable status dict before granting lock request to a transaction
    BufferList maintains,
    - We can use the buffers once locks are obtained through ConcurrencyMgr
    - {blk1_ref : buf1_ref, ... } so we can get buffer reference using block reference
    - [blk1, blk2, blk1, blk1, blk2, blk3, ...] list of blk references to pin/unpin the correct amount
    """
    # Used together to synchronously increase txnum
    _lock = threading.Lock()
    _next_txnum = 0

    def __init__(self, fm, lm, bm):
        self.fm: FileMgr = fm
        self.lm: LogMgr = lm
        self.bm: BufferMgr = bm

        self.txnum = Transaction.get_next_txnum()
        self.rm: RecoveryMgr = RecoveryMgr(self, self.txnum, self.lm, self.bm)
        self.cm: ConcurrencyMgr = ConcurrencyMgr()
        self.bufferList: BufferList = BufferList(self.bm)

    # Transaction lifespan
    def commit(self):
        self.rm.commit()
        db_logger.info("Commited " + str(self.txnum))
        self.cm.release()
        self.bufferList.unpinAll()

    def rollback(self):
        self.rm.rollback()
        db_logger.info("Rolled back " + str(self.txnum))
        self.cm.release()
        self.bufferList.unpinAll()

    # Unlike commit/rollback, there is no locking during startup recovery because the database server itself is running the recovery
    # database server is not ready to accept new transaction from client yet
    # Since no clients interacting with the database, there is no need for locking
    def recover(self):
        """
        We use a dummy transaction to run recovery.
        This dummy transaction appends the following three log records at the end of log file.

        ...new transaction log goes here
        <COMMIT X>
        <CHECKPOINT>
        <START X>
        ...previous transaction log goes here
        """
        self.bm.flushAll(self.txnum) # This line is not necessary if recovery is done during startup. Buffer manager is empty at this point.
        self.rm.recover()

    # Transaction buffer access
    def pin(self, target_block):
        """
        Similar to bufferPool.pin(blk)
        """
        # buffer manager holds mapping between page/block mapping for all transaction
        # self.bufferList holds mapping of blocks used by this transaction
        self.bufferList.pin(target_block)

    def unpin(self, target_block):
        self.bufferList.unpin(target_block)

    # Read and returns value (uses CM for locking)
    def getInt(self, target_block, block_offset):
        self.cm.sLock(target_block)
        buf_ref = self.bufferList.getBuffer(target_block) # TODO: currently throws KeyError, it should returns None if tx has not pinned this block yet
        return buf_ref.page.getInt(block_offset)

    def getString(self, target_block, block_offset):
        self.cm.sLock(target_block)
        buf_ref = self.bufferList.getBuffer(target_block)
        return buf_ref.page.getStr(block_offset)

    # Write value (Uses CM for locking and RM for logging)
    def setInt(self, target_block, block_offset, new_val, okToLog):
        self.cm.xLock(target_block)
        buf_ref: Buffer = self.bufferList.getBuffer(target_block) # TODO This line will throw KeyError if the block has not been pinned yet
        lsn = -1
        if okToLog:
            lsn = self.rm.setInt(buf_ref, block_offset)
        db_logger.info('Writing int ' + str(new_val) + ' to ' + str(buf_ref.block) + ' at ' + str(block_offset))
        buf_ref.page.setData(block_offset, new_val)
        buf_ref.setModified(self.txnum, lsn)

    def setString(self, target_block, block_offset, new_val, okToLog):
        self.cm.xLock(target_block)
        buf_ref: Buffer = self.bufferList.getBuffer(target_block)
        lsn = -1
        if okToLog:
            lsn = self.rm.setString(buf_ref, block_offset)
        db_logger.info('Writing str ' + str(new_val) + ' to ' + str(buf_ref.block) + ' at ' + str(block_offset))
        buf_ref.page.setData(block_offset, new_val)
        buf_ref.setModified(self.txnum, lsn)

    def availableBuffers(self):
        return self.bm.pool_availability

    # size and append read and modifies the end of file marker
    # TODO: Size/append obtains a lock on Block(-1); but when it is being added to the list of buffers associated with a transactions?
    def size(self, filename):
        """call fm.length() that returns block count of a file. Acquires lock on dummy block"""
        self.cm.sLock(Block(filename, -1))
        return self.fm.length(filename)

    # returns the new block references
    # 2nd paragraph of 5.4.5
    def append(self, filename):
        """TODO: Once we have table level lock - we should not need to maintain block lock. This also implies, once we add new block we don't need to acquire lock on the new block."""
        self.cm.xLock(Block(filename, -1))
        return self.fm.appendEmptyBlock(filename)

    def blockSize(self):
        return self.fm.block_size

    @staticmethod
    def get_next_txnum():
        with Transaction._lock:
            Transaction._next_txnum += 1
        return Transaction._next_txnum


if __name__ == "__main__":
    fig = [5.3, 5.19, 501][1]

    if fig == 501:
        # RecoveryTest - Not mentioned the book
        fm: FileMgr = FileMgr('recoverytest', 400)
        lm: LogMgr = LogMgr(fm, 'simpledb.log')
        bm: BufferMgr = BufferMgr(fm, lm, 2)

        if fm.length('testfile'):
            # recover
            tx = Transaction(fm, lm, bm)
            tx.recover()
            print('recovery - complete')
        else:
            # init
            tx1 = Transaction(fm, lm, bm)
            tx2 = Transaction(fm, lm, bm)
            blk0 = Block('testfile', 0)
            blk1 = Block('testfile', 1)
            tx1.pin(blk0)
            tx2.pin(blk1)
            pos = 0
            for i in range(6):
                tx1.setInt(blk0, pos, pos, False)
                tx2.setInt(blk1, pos, pos, False)
                pos += 4
            tx1.setString(blk0, 30, "abc", False)
            tx2.setString(blk1, 30, "def", False)
            tx1.commit()
            tx2.commit()

            # modify
            tx3 = Transaction(fm, lm, bm)
            tx4 = Transaction(fm, lm, bm)
            tx3.pin(blk0)
            tx4.pin(blk1)
            pos = 0
            for i in range(6):
                tx3.setInt(blk0, pos, pos + 100, True)
                tx4.setInt(blk1, pos, pos + 100, True)
                pos += 4
            tx3.setString(blk0, 30, 'uvw', True)
            tx4.setString(blk1, 30, 'xyz', True)
            # tx3.commit()
            # tx4.commit()
            bm.flushAll(3)
            bm.flushAll(4)

            tx3.rollback()

            print('init and modify - complete')
            print(lm)

    elif fig == 5.19:
        # Fig 5.19 ConcurrencyTest; Testing Concurrency class
        # Output files are not binary compatible because thread could start in any order
        fm = FileMgr('concurrencytest', 400)
        lm = LogMgr(fm, 'simpledb.log')
        bm = BufferMgr(fm, lm, 8)

        def A():
            try:
                txA = Transaction(fm, lm, bm)
                blk1 = Block('testfile', 1)
                blk2 = Block('testfile', 2)
                txA.pin(blk1)
                txA.pin(blk2)
                print('txA requesting slock1')
                txA.getInt(blk1, 0)
                print('txA received slock1')
                time.sleep(1)
                print('txA requesting slock2')
                txA.getInt(blk2, 0)
                print('txA received slock2')
                txA.commit()
            except Exception as e:
                txA.rollback()
                print("Exception: " + str(e))

        def B():
            try:
                txB = Transaction(fm, lm, bm)
                blk1 = Block('testfile', 1)
                blk2 = Block('testfile', 2)
                txB.pin(blk1)
                txB.pin(blk2)
                print('txB requesting xlock2')
                txB.setInt(blk2, 0, 0, False)
                print('txB received xlock2')
                time.sleep(1)
                print('txB requesting slock1')
                txB.getInt(blk1, 0)
                print('txB received slock1')
                txB.commit()
            except Exception as e:
                txB.rollback()
                print("Exception: " + str(e))

        def C():
            try:
                txC = Transaction(fm, lm, bm)
                blk1 = Block('testfile', 1)
                blk2 = Block('testfile', 2)
                txC.pin(blk1)
                txC.pin(blk2)
                print('txC requesting xlock1')
                txC.setInt(blk1, 0, 0, False)
                print('txC received xlock1')
                time.sleep(1)
                print('txC requesting slock2')
                txC.getInt(blk2, 0)
                print('txC received slock2')
                txC.commit()
            except Exception as e:
                txC.rollback()
                print("Exception: " + str(e))


        t1 = threading.Thread(target=A)
        t1.start()
        t2 = threading.Thread(target=B)
        t2.start()
        t3 = threading.Thread(target=C)
        t3.start()

        t1.join()
        t2.join()
        t3.join()

        print(lm)

    elif fig == 5.3:
        # Fig 5.3 TxTest; Testing Transactions
        fm = FileMgr('txtest', 400)
        lm = LogMgr(fm, 'simpledb.log')
        bm = BufferMgr(fm, lm, 8)

        tx1 = Transaction(fm, lm, bm)
        blk = Block('testfile', 1)
        tx1.pin(blk)
        tx1.setInt(blk, 80, 1, False)
        tx1.setString(blk, 40, "one", False)
        tx1.commit()

        tx2 = Transaction(fm, lm, bm)
        tx2.pin(blk)
        ival = tx2.getInt(blk, 80)
        sval = tx2.getString(blk, 40)
        print("Initial value at loc 80 =", str(ival))
        print("Initial value at loc 40 =", str(sval))
        newival = ival + 1
        newsval = sval + '!'
        tx2.setInt(blk, 80, newival, True)
        tx2.setString(blk, 40, newsval, True)
        tx2.commit()
        print(lm)

        tx3 = Transaction(fm, lm, bm)
        tx3.pin(blk)
        print('new value at loc 80 = ', str(tx3.getInt(blk, 80)))
        print('new value at loc 40 = ', str(tx3.getString(blk, 40)))
        tx3.setInt(blk, 80, 9999, True)
        print('pre-rollback value at loc 80 = ', str(tx3.getInt(blk, 80)))
        tx3.rollback()
        print(lm)

        tx4 = Transaction(fm, lm, bm)
        tx4.pin(blk)
        print('post-rollback value at loc 80 = ', str(tx4.getInt(blk, 80)))
        tx4.commit()
        print(lm)