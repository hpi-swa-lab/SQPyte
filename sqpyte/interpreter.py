from rpython.rtyper.lltypesystem import rffi, lltype
from capi import CConfig
from rpython.rlib.rarithmetic import intmask
import sys
import os
import capi

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

class Sqlite3(object):
    db = None
    p = None

    def __init__(self, db_name, query):
        self.opendb(db_name)
        self.prepare(query)

    def opendb(self, db_name):
        with rffi.scoped_str2charp(db_name) as db_name, lltype.scoped_alloc(capi.SQLITE3PP.TO, 1) as result:
            errorcode = capi.sqlite3_open(db_name, result)
            assert errorcode == 0
            self.db = rffi.cast(capi.SQLITE3P, result[0])

    def prepare(self, query):
        length = len(query)
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare(self.db, query, length, result, unused_buffer)
            assert errorcode == 0
            self.p = rffi.cast(capi.VDBEP, result[0])

    def python_OP_Init(self, pc, pOp):
        if pOp.p2:
            pc = pOp.p2 - 1
        return pc

    def python_OP_Rewind(self, pc, pOp):
        with lltype.scoped_alloc(rffi.INTP.TO, 1) as internalPc:
            internalPc[0] = rffi.cast(rffi.INT, pc)
            rc = capi.impl_OP_Rewind(self.p, self.db, internalPc, pOp)
            retPc = internalPc[0]
        return retPc, rc

    def python_OP_Transaction(self, pc, pOp):
        return capi.impl_OP_Transaction(self.p, self.db, pc, pOp)

    def python_OP_TableLock(self, pc, pOp):
        return capi.impl_OP_TableLock(self.p, self.db, pc, pOp)

    def python_OP_Goto(self, pc, pOp):
        pc = pOp.p2 - 1
        return pc

    def python_OP_OpenRead(self, pc, pOp):
        capi.impl_OP_OpenRead(self.p, self.db, pc, pOp)

    def python_OP_Column(self, pc, pOp):
        return capi.impl_OP_Column(self.p, self.db, pc, pOp)

    def python_OP_ResultRow(self, pc, pOp):
        return capi.impl_OP_ResultRow(self.p, self.db, pc, pOp)

    def python_OP_Next(self, pc, pOp):
        with lltype.scoped_alloc(rffi.INTP.TO, 1) as internalPc:
            internalPc[0] = rffi.cast(rffi.INT, pc)
            rc = capi.impl_OP_Next(self.p, self.db, internalPc, pOp)
            retPc = internalPc[0]
        return retPc, rc

    def python_OP_Close(self, pc, pOp):
        capi.impl_OP_Close(self.p, self.db, pc, pOp)

    def python_OP_Halt(self, pc, pOp):
        with lltype.scoped_alloc(rffi.INTP.TO, 1) as internalPc:
            internalPc[0] = rffi.cast(rffi.INT, pc)
            rc = capi.impl_OP_Halt(self.p, self.db, internalPc, pOp)
            retPc = internalPc[0]
        return retPc, rc

    def python_OP_Compare(self, pc, pOp):
        return capi.impl_OP_Compare(self.p, self.db, pc, pOp)

    def python_OP_Integer(self, pc, pOp):
        capi.impl_OP_Integer(self.p, self.db, pc, pOp)
        # pOut.u.i = pOp.p1
        # self.p.aMem[pOp.p2] = pOp.p1
#     case OP_Integer: {         /* out2-prerelease */
#   pOut->u.i = pOp->p1;
#   break;
# }


    def python_sqlite3_column_text(self, iCol):
        return capi.sqlite3_column_text(self.p, iCol)
    def python_sqlite3_column_bytes(self, iCol):
        return capi.sqlite3_column_bytes(self.p, iCol)


    def mainloop(self):
        ops = self.p.aOp
        rc = CConfig.SQLITE_OK
        pc = self.p.pc
        if pc < 0:
            pc = 0 # XXX maybe more to too, see vdbeapi.c:418

        while rc == CConfig.SQLITE_OK:
            pOp = ops[pc]

            if pOp.opcode == CConfig.OP_Init:
                print '>>> OP_Init <<<'
                pc = self.python_OP_Init(pc, pOp)
            elif pOp.opcode == CConfig.OP_OpenRead or pOp.opcode == CConfig.OP_OpenWrite:
                print '>>> OP_OpenRead <<<'
                self.python_OP_OpenRead(pc, pOp)
            elif pOp.opcode == CConfig.OP_Rewind:
                print '>>> OP_Rewind <<<'
                pc, rc = self.python_OP_Rewind(pc, pOp)
            elif pOp.opcode == CConfig.OP_Transaction:
                print '>>> OP_Transaction <<<'
                rc = self.python_OP_Transaction(pc, pOp)
                if rc == CConfig.SQLITE_BUSY:
                    print 'ERROR: in OP_Transaction SQLITE_BUSY'
                    print 'rc = %s\npc = %s' % (rc, pc)
                    return rc
            elif pOp.opcode == CConfig.OP_TableLock:
                print '>>> OP_TableLock <<<'
                rc = self.python_OP_TableLock(pc, pOp)
            elif pOp.opcode == CConfig.OP_Goto:
                print '>>> OP_Goto <<<'
                pc = self.python_OP_Goto(pc, pOp)
            elif pOp.opcode == CConfig.OP_Column:
                print '>>> OP_Column <<<'
                rc = self.python_OP_Column(pc, pOp)
            elif pOp.opcode == CConfig.OP_ResultRow:
                print '>>> OP_ResultRow <<<'
                rc = self.python_OP_ResultRow(pc, pOp)
                if rc == CConfig.SQLITE_ROW:
                    print 'rc = %s\npc = %s' % (rc, pc)
                    return rc
            elif pOp.opcode == CConfig.OP_Next:
                print '>>> OP_Next <<<'
                pc, rc = self.python_OP_Next(pc, pOp)
            elif pOp.opcode == CConfig.OP_Close:
                print '>>> OP_Close <<<'
                self.python_OP_Close(pc, pOp)
            elif pOp.opcode == CConfig.OP_Halt:
                print '>>> OP_Halt <<<'
                pc, rc = self.python_OP_Halt(pc, pOp)
                return rc
            elif (pOp.opcode == CConfig.OP_Eq or 
                  pOp.opcode == CConfig.OP_Ne or 
                  pOp.opcode == CConfig.OP_Lt or 
                  pOp.opcode == CConfig.OP_Le or 
                  pOp.opcode == CConfig.OP_Gt or 
                  pOp.opcode == CConfig.OP_Ge):
                print '>>> OP_Compare: %s <<<' % pOp.opcode
                pc = self.python_OP_Compare(pc, pOp) - 1
                print pc
            elif pOp.opcode == CConfig.OP_Integer:
                print '>>> OP_Integer <<<'
                self.python_OP_Integer(pc, pOp)
                # print self.p.aMem[pOp.p2]
            else:
                print 'Opcode %s is not there yet!' % pOp.opcode
                # raise Exception("Unimplemented bytecode %s." % pOp.opcode)
                pass
            # print 'rc = %s\npc = %s' % (rc, pc)
            pc += 1
        return rc


def run():
    sqlite3 = Sqlite3(testdb, 'select name from contacts;')
    rc = sqlite3.mainloop()

def entry_point(argv):
    run()
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point()
