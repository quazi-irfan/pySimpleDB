### pySimpleDB

This is a python implementation of [SimpleDB](https://cs.bc.edu/~sciore/simpledb/), originally authored by [Edward Sciore](https://www.bc.edu/bc-web/schools/morrissey/departments/computer-science/people/faculty-directory/edward-sciore.html)[1, 2].

### Features
- ACID Compliance
  - Atomicity and Durability is implemented using Logging and Recovery manager
  - Consistency is implemented using runtime-check in the query planner
  - Serializable isolation level is implemented using two phase locking.
- Storage and Buffer Pool
  - Files are treated as raw disk
  - Files are read in blocks and paged into memory one block at a time
  - Fixed number of pages are maintained in a Buffer pool
  - Buffers from buffer pool are swap out using FIFO
- Concurrent Transactions Support
  - Locks are acquired on file blocks 
  - Supports multiple user simultaneously using concurrent transactions
  - In this variant of two phase locking, locks(shared and exclusive) are acquired on immediately before read/write operation 
  - All locks are released at transaction completion(commit/rollback)
  - Uses time-limit algorithm to approximate deadlock detection when acquiring lock or pinning buffer
  - Exception is thrown if a transaction can't acquire requested lock on a block or can't page in a block into buffer pool in 10 seconds
- Logging and Recovery Manager
  - All transactions with commit or rollback log record are considered completed successfully
  - Each modification to a field generates a log entry capturing the old value of the field
  - During recovery, recovery manager reads the log file in reverse and undo all operations made by incomplete transactions
  - (Write ahead) log is flushed before flushing buffer to ensure all changes made to a buffer has corresponding update log 
  - Log files gets very large, but recovery manager only reads until a quiescent checkpoint
- SQL Support
  - 4 bit integer and fixed length string
  - Supported Relational operators: Project, Product, Select
  - Select statement with Where clause with multiple predicate(equality operator only)
  - Simplified form of Create, Update and Delete statement

### References
- [1] Book [Database Design and Implementation by Edward Sciore](https://link.springer.com/book/10.1007/978-3-030-33836-7)
- [2] Article [SimpleDB: a simple java-based multiuser syst for teaching database internals](https://dl.acm.org/doi/abs/10.1145/1227504.1227498)

