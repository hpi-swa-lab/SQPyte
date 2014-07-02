from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from capi import CConfig
from rpython.rlib.rarithmetic import intmask
import sys
import os
import capi
import translated
import math

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test/test.db")
jitdriver = jit.JitDriver(
    greens=['pc', 'rc', 'ops', 'self_'], 
    reds=[],
    should_unroll_one_iteration=lambda *args: True,
    )
    # get_printable_location=get_printable_location)

class SQPyteException(Exception):
    def __init__(self, msg):
        self.msg = msg

class Sqlite3DB(object):
    _immutable_fields_ = ['db']

    def __init__(self, db_name):
        self.opendb(db_name)

    def opendb(self, db_name):
        with rffi.scoped_str2charp(db_name) as db_name, lltype.scoped_alloc(capi.SQLITE3PP.TO, 1) as result:
            errorcode = capi.sqlite3_open(db_name, result)
            assert(errorcode == 0)
            self.db = rffi.cast(capi.SQLITE3P, result[0])


class Sqlite3Query(object):

    _immutable_fields_ = ['internalPc', 'db', 'p']

    def __init__(self, db, query):
        self.db = db
        self.prepare(query)
        self.internalPc = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')

    def prepare(self, query):
        length = len(query)
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare(self.db, query, length, result, unused_buffer)
            assert errorcode == 0
            self.p = rffi.cast(capi.VDBEP, result[0])
            # self.p.bIsReader = rffi.cast(rffi.UINT, 1)

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
        # translated.python_OP_OpenRead_translated(self.p, self.db, pc, pOp)

    def python_OP_Column(self, pc, pOp):
        return capi.impl_OP_Column(self.p, self.db, pc, pOp)
        # return translated.python_OP_Column_translated(self.p, self.db, pc, pOp)

    def python_OP_ResultRow(self, pc, pOp):
        return capi.impl_OP_ResultRow(self.p, self.db, pc, pOp)

    def python_OP_Next(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.INT, pc)
        rc = capi.impl_OP_Next(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

        # retPc, rc = translated.python_OP_Next_translated(self.p, self.db, pc, pOp) #self.internalPc, pOp)
        # return retPc, rc

    def python_OP_Close(self, pc, pOp):
        capi.impl_OP_Close(self.p, self.db, pc, pOp)

    def python_OP_Halt(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.INT, pc)
        rc = capi.impl_OP_Halt(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Ne_Eq_Gt_Le_Lt_Ge(self, pc, pOp):
        return capi.impl_OP_Ne_Eq_Gt_Le_Lt_Ge(self.p, self.db, pc, pOp)

    def python_OP_Integer(self, pc, pOp):
        capi.impl_OP_Integer(self.p, self.db, pc, pOp)

    def python_OP_Null(self, pc, pOp):
        capi.impl_OP_Null(self.p, self.db, pc, pOp)

    def python_OP_AggStep(self, rc, pOp):
        return capi.impl_OP_AggStep(self.p, self.db, rc, pOp)

    def python_OP_AggFinal(self, rc, pOp):
        capi.impl_OP_AggFinal(self.p, self.db, rc, pOp)

    def python_OP_Copy(self, pc, pOp):
        capi.impl_OP_Copy(self.p, self.db, pc, pOp)

    def python_OP_MustBeInt(self, pc, pOp):
        return capi.impl_OP_MustBeInt(self.p, self.db, pc, pOp)

    def python_OP_NotExists(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.INT, pc)
        rc = capi.impl_OP_NotExists(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_String(self, pc, pOp):
        capi.impl_OP_String(self.p, self.db, pc, pOp)

    def python_OP_String8(self, rc, pOp):
        return capi.impl_OP_String8(self.p, self.db, rc, pOp)

    def python_OP_Function(self, pc, rc, pOp):
        return capi.impl_OP_Function(self.p, self.db, pc, rc, pOp)

    def python_OP_Real(self, pc, pOp):
        # aMem = self.p.aMem
        # pOut = aMem[pOp.p2]
        # pOut.flags = rffi.cast(rffi.USHORT, CConfig.MEM_Real)
        # assert not math.isnan(pOp.p4.pReal)
        # pOut.r = pOp.p4.pReal

        capi.impl_OP_Real(self.p, self.db, pc, pOp)

    def python_OP_RealAffinity(self, pc, pOp):
        capi.impl_OP_RealAffinity(self.p, self.db, pc, pOp)

    def python_OP_Add_Subtract_Multiply_Divide_Remainder(self, pc, pOp):
        capi.impl_OP_Add_Subtract_Multiply_Divide_Remainder(self.p, self.db, pc, pOp)

    def python_OP_If_IfNot(self, pc, pOp):
        return capi.impl_OP_If_IfNot(self.p, self.db, pc, pOp)

    def python_OP_Rowid(self, rc, pOp):
        return capi.impl_OP_Rowid(self.p, self.db, rc, pOp)


    def python_sqlite3_column_text(self, iCol):
        return capi.sqlite3_column_text(self.p, iCol)
    def python_sqlite3_column_bytes(self, iCol):
        return capi.sqlite3_column_bytes(self.p, iCol)


    def debug_print(self, s):
        return
        if not jit.we_are_jitted():
            print s

    def get_opcode_str(self, opcode):
        if opcode == CConfig.OP_Eq:
            return 'OP_Eq'
        elif opcode == CConfig.OP_Ne:
            return 'OP_Ne'
        elif opcode == CConfig.OP_Lt:
            return 'OP_Lt'
        elif opcode == CConfig.OP_Le:
            return 'OP_Le'
        elif opcode == CConfig.OP_Gt:
            return 'OP_Gt'
        elif opcode == CConfig.OP_Ge:
            return 'OP_Ge'
        elif opcode == CConfig.OP_Add:
            return 'OP_Add'
        elif opcode == CConfig.OP_Subtract:
            return 'OP_Subtract'
        elif opcode == CConfig.OP_Multiply:
            return 'OP_Multiply'
        elif opcode == CConfig.OP_Divide:
            return 'OP_Divide'
        elif opcode == CConfig.OP_Remainder:
            return 'OP_Remainder'
        elif opcode == CConfig.OP_If:
            return 'OP_If'
        elif opcode == CConfig.OP_IfNot:
            return 'OP_IfNot'
        else:
            return ''

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

        i = 0
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
                self.debug_print('>>> %s <<<' % self.get_opcode_str(opcode))
                pc = self.python_OP_Ne_Eq_Gt_Le_Lt_Ge(pc, pOp)
            elif opcode == CConfig.OP_Integer:
                self.debug_print('>>> OP_Integer <<<')
                self.python_OP_Integer(pc, pOp)
            elif opcode == CConfig.OP_Null:
                self.debug_print('>>> OP_Null <<<')
                self.python_OP_Null(pc, pOp)
            elif opcode == CConfig.OP_AggStep:
                self.debug_print('>>> OP_AggStep <<<')
                rc = self.python_OP_AggStep(rc, pOp)
            elif opcode == CConfig.OP_AggFinal:
                self.debug_print('>>> OP_AggFinal <<<')
                self.python_OP_AggFinal(rc, pOp)
            elif opcode == CConfig.OP_Copy:
                self.debug_print('>>> OP_Copy <<<')
                self.python_OP_Copy(pc, pOp)
            elif opcode == CConfig.OP_MustBeInt:
                self.debug_print('>>> OP_MustBeInt <<<')
                pc = self.python_OP_MustBeInt(pc, pOp)
            elif opcode == CConfig.OP_NotExists:
                self.debug_print('>>> OP_NotExists <<<')
                pc, rc = self.python_OP_NotExists(pc, pOp)
            elif opcode == CConfig.OP_String:
                self.debug_print('>>> OP_String <<<')
                self.python_OP_String(pc, pOp)
            elif opcode == CConfig.OP_String8:
                self.debug_print('>>> OP_String8 <<<')
                rc = self.python_OP_String8(rc, pOp)
            elif opcode == CConfig.OP_Function:
                self.debug_print('>>> OP_Function <<<')
                rc = self.python_OP_Function(pc, rc, pOp)
            elif opcode == CConfig.OP_Real:
                self.debug_print('>>> OP_Real <<<')
                self.python_OP_Real(pc, pOp)
            elif opcode == CConfig.OP_RealAffinity:
                self.debug_print('>>> OP_RealAffinity <<<')
                self.python_OP_RealAffinity(pc, pOp)
            elif (opcode == CConfig.OP_Add or 
                  opcode == CConfig.OP_Subtract or 
                  opcode == CConfig.OP_Multiply or 
                  opcode == CConfig.OP_Divide or 
                  opcode == CConfig.OP_Remainder):
                self.debug_print('>>> %s <<<' % self.get_opcode_str(opcode))
                self.python_OP_Add_Subtract_Multiply_Divide_Remainder(pc, pOp)
            elif (opcode == CConfig.OP_If or
                  opcode == CConfig.OP_IfNot):
                self.debug_print('>>> %s <<<' % self.get_opcode_str(opcode))
                pc = self.python_OP_If_IfNot(pc, pOp)
            elif opcode == CConfig.OP_Rowid:
                self.debug_print('>>> OP_Rowid <<<')
                rc = self.python_OP_Rowid(rc, pOp)
            else:
                raise Exception("Unimplemented bytecode %s." % opcode)
                # raise SQPyteException("Unimplemented bytecode %s." % opcode)
            pc = jit.promote(rffi.cast(lltype.Signed, pc))
            pc += 1
            if pc <= oldpc:
                jitdriver.can_enter_jit(pc=pc, self_=self, ops=ops, rc=rc)
        return rc


def main_work(query):
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, query)
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    print count

def entry_point(argv):
    try:
        query = argv[1]
    except IndexError:
        print "You must supply a query to be run: e.g., 'select first_name from people where age > 1;'."
        return 1
    
    main_work(query)
    return 0

def target(*args):
    return entry_point
    
if __name__ == "__main__":
    entry_point(sys.argv)
