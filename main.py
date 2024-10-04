from Planner import *

import logging
db_logger = logging.getLogger('SimpleDB')
db_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(filename)-10s:%(lineno)-4d - %(message)s', datefmt="%H:%M:%S")
console_handler.setFormatter(formatter)
db_logger.addHandler(console_handler)

class SimpleDB:
    def __init__(self, db_name, block_size, buffer_pool_size):
        self.fm: FileMgr = FileMgr(db_name, block_size)
        self.lm: LogMgr = LogMgr(self.fm, db_name + '.log')
        self.bm: BufferMgr = BufferMgr(self.fm, self.lm, buffer_pool_size)

        tx: Transaction = Transaction(self.fm, self.lm, self.bm)
        if self.fm.db_exists:
            print('Recovering...')
            tx.recover()
        else:
            print('Created new db...')
            self.mm = MetadataMgr(tx, True) # if db does not existes, then initialize everything
        tx.commit()



db = SimpleDB('Plannertest', 400, 8)
tx = Transaction(db.fm, db.lm, db.bm)

sA = Schema(['Aa', 'int', 4], ['Ab', 'str', 9])
db.mm.createTable(tx, 'A', sA)
p1 = TablePlan(tx, 'A', db.mm)
s1 = p1.open()
s1.beforeFirst()
for i in [0, 1, 2]:
    s1.nextEmptyRecord()
    s1.setInt('Aa', i % 3)
    s1.setString('Ab', 'rec'+str(i))
s1.closeRecordPage()

qp = BasicQueryPlanner(db.mm)
up = BasicUpdatePlanner(db.mm)
p = Planner(qp, up)
query = 'select Aa, Ab from A where Aa = 0'
pln = p.createQueryPlan(tx, query)
scn = pln.open()
scn_schema = pln.plan_schema()
while scn.nextRecord():
    for field_name in scn_schema.field_info.keys():
        print(scn.getVal(field_name), end=" ")
    print()
scn.closeRecordPage()