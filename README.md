### pySimpleDB

This is a python implementation of SimpleDB, originally authored by Edward Sciore[1, 2].

### Features
- Uses Heap file and buffer pool to minimize disk seek
- Concurrent Transactions
    - Supports concurrent user using concurrent transactions
    - Transactions adhear to serializable isolation level
    - Uses a variant of two phase locking
    - Locks(Both soft and hard) are acquired on demand, and all released at transcation commit
- Recovery
  - Write ahead log for recovery
  - Recovery manager peforms undo operation on all uncommited transactions during database startup
  - Log files gets very large, but recovery manager only reads until a quiescent checkpoint
- Supported data types
  - 4 bit integer and fixed length string
- SQL Support
  - Supported Relational operators: Project, Product, Select
  - Select statement with Where clause with multiple predicate(equality operator only)
  - Simplified form of Create, Update and Delete statement

### References
- [1] Book [Database Design and Implementation by Edward Sciore](https://link.springer.com/book/10.1007/978-3-030-33836-7)
- [2] Article [SimpleDB: a simple java-based multiuser syst for teaching database internals](https://dl.acm.org/doi/abs/10.1145/1227504.1227498)

