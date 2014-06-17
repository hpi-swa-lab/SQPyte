from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from capi import CConfig
from rpython.rlib.rarithmetic import intmask
import sys
import os
import capi

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")
jitdriver = jit.JitDriver(
    greens=['pc', 'rc', 'ops', 'self_'], 
    reds=[],
    should_unroll_one_iteration=lambda *args: True,
    )
    # get_printable_location=get_printable_location)

class Sqlite3(object):

    _immutable_fields_ = ['internalPc', 'db', 'p']

    def __init__(self, db_name, query):
        self.opendb(db_name)
        self.prepare(query)
        self.internalPc = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')

    def opendb(self, db_name):
        with rffi.scoped_str2charp(db_name) as db_name, lltype.scoped_alloc(capi.SQLITE3PP.TO, 1) as result:
            errorcode = capi.sqlite3_open(db_name, result)
            assert(errorcode == 0)
            self.db = rffi.cast(capi.SQLITE3P, result[0])

    def prepare(self, query):
        length = len(query)
        print 'length = %s' % length
        print 'query = %s' % query
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare(self.db, query, length, result, unused_buffer)
            assert errorcode == 0
            self.p = rffi.cast(capi.VDBEP, result[0])

    def reset_query(self):
        capi.sqlite3_reset(self.p)

    def python_OP_Init(self, pc, pOp):
        cond = rffi.cast(lltype.Bool, pOp.p2)
        p2 = rffi.cast(lltype.Signed, pOp.p2)
        if cond:
            pc = p2 - 1
        return pc

    def python_OP_Rewind(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.INT, pc)
        rc = capi.impl_OP_Rewind(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Transaction(self, pc, pOp):
        return capi.impl_OP_Transaction(self.p, self.db, pc, pOp)

    def python_OP_TableLock(self, pc, pOp):
        return capi.impl_OP_TableLock(self.p, self.db, pc, pOp)

    def python_OP_Goto(self, pc, pOp):
        p2 = rffi.cast(lltype.Signed, pOp.p2)
        pc = p2 - 1
        return pc

    def python_OP_OpenRead(self, pc, pOp):
        capi.impl_OP_OpenRead(self.p, self.db, pc, pOp)

    def python_OP_Column(self, pc, pOp):
        return capi.impl_OP_Column(self.p, self.db, pc, pOp)

    def python_OP_ResultRow(self, pc, pOp):
        return capi.impl_OP_ResultRow(self.p, self.db, pc, pOp)

    def python_OP_Next(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.INT, pc)
        rc = capi.impl_OP_Next(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Close(self, pc, pOp):
        capi.impl_OP_Close(self.p, self.db, pc, pOp)

    def python_OP_Halt(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.INT, pc)
        rc = capi.impl_OP_Halt(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Compare(self, pc, pOp):
        return capi.impl_OP_Compare(self.p, self.db, pc, pOp)

    def python_OP_Integer(self, pc, pOp):
        capi.impl_OP_Integer(self.p, self.db, pc, pOp)


    def python_sqlite3_column_text(self, iCol):
        return capi.sqlite3_column_text(self.p, iCol)
    def python_sqlite3_column_bytes(self, iCol):
        return capi.sqlite3_column_bytes(self.p, iCol)

    def debug_print(self, s):
        return
        if not jit.we_are_jitted():
            print s

    @jit.elidable
    def get_opcode(self, pOp):
        return rffi.cast(lltype.Unsigned, pOp.opcode)

    @jit.elidable
    def get_aOp(self):
        return self.p.aOp

    def mainloop(self):
        ops = self.get_aOp()
        rc = CConfig.SQLITE_OK
        # pc = self.p.pc
        pc = jit.promote(rffi.cast(lltype.Signed, self.p.pc))
        if pc < 0:
            pc = 0 # XXX maybe more to too, see vdbeapi.c:418

        while True:
            jitdriver.jit_merge_point(pc=pc, self_=self, ops=ops, rc=rc)
            if rc != CConfig.SQLITE_OK:
                break
            pOp = ops[pc]
            opcode = self.get_opcode(pOp)
            oldpc = pc

            if opcode == CConfig.OP_Init:
                self.debug_print('>>> OP_Init <<<')
                pc = self.python_OP_Init(pc, pOp)
            elif opcode == CConfig.OP_OpenRead or opcode == CConfig.OP_OpenWrite:
                self.debug_print('>>> OP_OpenRead <<<')
                self.python_OP_OpenRead(pc, pOp)
            elif opcode == CConfig.OP_Rewind:
                self.debug_print('>>> OP_Rewind <<<')
                pc, rc = self.python_OP_Rewind(pc, pOp)
            elif opcode == CConfig.OP_Transaction:
                self.debug_print('>>> OP_Transaction <<<')
                rc = self.python_OP_Transaction(pc, pOp)
                if rc == CConfig.SQLITE_BUSY:
                    print 'ERROR: in OP_Transaction SQLITE_BUSY'
                    # print 'rc = %s\npc = %s' % (rc, pc)
                    return rc
            elif opcode == CConfig.OP_TableLock:
                self.debug_print('>>> OP_TableLock <<<')
                rc = self.python_OP_TableLock(pc, pOp)
            elif opcode == CConfig.OP_Goto:
                self.debug_print('>>> OP_Goto <<<')
                pc = self.python_OP_Goto(pc, pOp)
            elif opcode == CConfig.OP_Column:
                self.debug_print('>>> OP_Column <<<')
                rc = self.python_OP_Column(pc, pOp)
            elif opcode == CConfig.OP_ResultRow:
                self.debug_print('>>> OP_ResultRow <<<')
                rc = self.python_OP_ResultRow(pc, pOp)
                if rc == CConfig.SQLITE_ROW:
                    # print 'rc = %s\npc = %s' % (rc, pc)
                    return rc
            elif opcode == CConfig.OP_Next:
                self.debug_print('>>> OP_Next <<<')
                pc, rc = self.python_OP_Next(pc, pOp)
            elif opcode == CConfig.OP_Close:
                self.debug_print('>>> OP_Close <<<')
                self.python_OP_Close(pc, pOp)
            elif opcode == CConfig.OP_Halt:
                self.debug_print('>>> OP_Halt <<<')
                pc, rc = self.python_OP_Halt(pc, pOp)
                return rc
            elif (opcode == CConfig.OP_Eq or 
                  opcode == CConfig.OP_Ne or 
                  opcode == CConfig.OP_Lt or 
                  opcode == CConfig.OP_Le or 
                  opcode == CConfig.OP_Gt or 
                  opcode == CConfig.OP_Ge):
                self.debug_print('>>> OP_Compare: %s <<<' % opcode)
                pc = self.python_OP_Compare(pc, pOp)
            elif opcode == CConfig.OP_Integer:
                self.debug_print('>>> OP_Integer <<<')
                self.python_OP_Integer(pc, pOp)
            else:
                print 'Opcode %s is not there yet!' % opcode
                # raise Exception("Unimplemented bytecode %s." % opcode)
                pass
            # print 'rc = %s\npc = %s' % (rc, pc)
            pc = jit.promote(rffi.cast(lltype.Signed, pc))
            pc += 1
            if pc <= oldpc:
                jitdriver.can_enter_jit(pc=pc, self_=self, ops=ops, rc=rc)
        return rc

