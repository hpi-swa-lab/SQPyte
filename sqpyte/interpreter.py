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
# testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test/big-test.db")

def get_printable_location(pc, rc, ops, self):
    opcode = self.get_opcode(ops[pc])
    name = self.get_opcode_str(opcode)
    return "%s %s %s" % (pc, rc, name)


jitdriver = jit.JitDriver(
    greens=['pc', 'rc', 'ops', 'self_'], 
    reds=[],
    should_unroll_one_iteration=lambda *args: True,
    get_printable_location=get_printable_location)

class SQPyteException(Exception):
    def __init__(self, msg):
        print msg

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
        self.internalPc = lltype.malloc(rffi.LONGP.TO, 1, flavor='raw')
        self.intp = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')

    def prepare(self, query):
        length = len(query)
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare(self.db, query, length, result, unused_buffer)
            assert errorcode == 0
            self.p = rffi.cast(capi.VDBEP, result[0])

    def reset_query(self):
        capi.sqlite3_reset(self.p)

    def python_OP_Init(self, pc, pOp):
        return translated.python_OP_Init_translated(self, pc, pOp)

    def python_OP_Rewind(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_Rewind(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Transaction(self, pc, pOp):
        return capi.impl_OP_Transaction(self.p, self.db, pc, pOp)

    def python_OP_TableLock(self, rc, pOp):
        return capi.impl_OP_TableLock(self.p, self.db, rc, pOp)

    def python_OP_Goto(self, pc, rc, pOp):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # retRc = capi.impl_OP_Goto(self.p, self.db, self.internalPc, rc, pOp)
        # retPc = self.internalPc[0]
        # return retPc, retRc

        return translated.python_OP_Goto_translated(self, pc, rc, pOp)

    def python_OP_OpenRead_OpenWrite(self, pc, pOp):
        return capi.impl_OP_OpenRead_OpenWrite(self.p, self.db, pc, pOp)
        # translated.python_OP_OpenRead_OpenWrite_translated(self, self.db, pc, pOp)

    def python_OP_Column(self, pc, pOp):
        return capi.impl_OP_Column(self.p, self.db, pc, pOp)
        # return translated.python_OP_Column_translated(self, self.db, pc, pOp)

    def python_OP_ResultRow(self, pc, pOp):
        return capi.impl_OP_ResultRow(self.p, self.db, pc, pOp)

    def python_OP_Next(self, pc, pOp):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_Next(self.p, self.db, self.internalPc, pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc

        return translated.python_OP_Next_translated(self, pc, pOp)

    def python_OP_Close(self, pOp):
        capi.impl_OP_Close(self.p, pOp)

    def python_OP_Halt(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_Halt(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Ne_Eq_Gt_Le_Lt_Ge(self, pc, rc, pOp):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_Ne_Eq_Gt_Le_Lt_Ge(self.p, self.db, self.internalPc, rc, pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc

        return translated.python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(self, pc, rc, pOp)

    def python_OP_Integer(self, pOp):
        capi.impl_OP_Integer(self.p, pOp)

    def python_OP_Null(self, pOp):
        capi.impl_OP_Null(self.p, pOp)

    def python_OP_AggStep(self, rc, pOp):
        return capi.impl_OP_AggStep(self.p, self.db, rc, pOp)

    def python_OP_AggFinal(self, pc, rc, pOp):
        return capi.impl_OP_AggFinal(self.p, self.db, pc, rc, pOp)

    def python_OP_Copy(self, pc, rc, pOp):
        return capi.impl_OP_Copy(self.p, self.db, pc, rc, pOp)

    def python_OP_MustBeInt(self, pc, rc, pOp):
        return translated.python_OP_MustBeInt(self, pc, rc, pOp)

    def python_OP_NotExists(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_NotExists(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_String(self, pOp):
        capi.impl_OP_String(self.p, self.db, pOp)

    def python_OP_String8(self, pc, rc, pOp):
        return capi.impl_OP_String8(self.p, self.db, pc, rc, pOp)

    def python_OP_Function(self, pc, rc, pOp):
        return capi.impl_OP_Function(self.p, self.db, pc, rc, pOp)

    def python_OP_Real(self, pOp):
        # aMem = self.p.aMem
        # pOut = aMem[pOp.p2]
        # pOut.flags = rffi.cast(rffi.USHORT, CConfig.MEM_Real)
        # assert not math.isnan(pOp.p4.pReal)
        # pOut.r = pOp.p4.pReal

        capi.impl_OP_Real(self.p, pOp)

    def python_OP_RealAffinity(self, pOp):
        translated.python_OP_RealAffinity(self, pOp)

    def python_OP_Add_Subtract_Multiply_Divide_Remainder(self, pOp):
        capi.impl_OP_Add_Subtract_Multiply_Divide_Remainder(self.p, pOp)

    def python_OP_If_IfNot(self, pc, pOp):
        return capi.impl_OP_If_IfNot(self.p, pc, pOp)

    def python_OP_Rowid(self, pc, rc, pOp):
        return capi.impl_OP_Rowid(self.p, self.db, pc, rc, pOp)

    def python_OP_IsNull(self, pc, pOp):
        return translated.python_OP_IsNull(self, pc, pOp)

    def python_OP_SeekLT_SeekLE_SeekGE_SeekGT(self, pc, rc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_SeekLT_SeekLE_SeekGE_SeekGT(self.p, self.db, self.internalPc, rc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Move(self, pOp):
        capi.impl_OP_Move(self.p, pOp)

    def python_OP_IfZero(self, pc, pOp):
        return capi.impl_OP_IfZero(self.p, pc, pOp)

    def python_OP_IdxRowid(self, pc, rc, pOp):
        return capi.impl_OP_IdxRowid(self.p, self.db, pc, rc, pOp)

    def python_OP_IdxLE_IdxGT_IdxLT_IdxGE(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_IdxLE_IdxGT_IdxLT_IdxGE(self.p, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Seek(self, pOp):
        capi.impl_OP_Seek(self.p, pOp)

    def python_OP_Once(self, pc, pOp):
        return translated.python_OP_Once(self, pc, pOp)

    def python_OP_SCopy(self, pOp):
        capi.impl_OP_SCopy(self.p, pOp)

    def python_OP_Affinity(self, pOp):
        translated.python_OP_Affinity(self, pOp)

    def python_OP_OpenAutoindex_OpenEphemeral(self, pc, pOp):
        return capi.impl_OP_OpenAutoindex_OpenEphemeral(self.p, self.db, pc, pOp)

    def python_OP_MakeRecord(self, pc, rc, pOp):
        return capi.impl_OP_MakeRecord(self.p, self.db, pc, rc, pOp)

    def python_OP_SorterInsert_IdxInsert(self, pOp):
        return capi.impl_OP_SorterInsert_IdxInsert(self.p, self.db, pOp)

    def python_OP_NoConflict_NotFound_Found(self, pc, rc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_NoConflict_NotFound_Found(self.p, self.db, self.internalPc, rc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_RowSetTest(self, pc, rc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_RowSetTest(self.p, self.db, self.internalPc, rc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_Gosub(self, pc, pOp):
        return capi.impl_OP_Gosub(self.p, pc, pOp)

    def python_OP_Return(self, pc, pOp):
        return capi.impl_OP_Return(self.p, pc, pOp)

    def python_OP_SorterOpen(self, pc, pOp):
        return capi.impl_OP_SorterOpen(self.p, self.db, pc, pOp)

    def python_OP_NextIfOpen(self, pc, rc, pOp):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_NextIfOpen(self.p, self.db, self.internalPc, rc, pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc        

        return translated.python_OP_NextIfOpen_translated(self, pc, rc, pOp)

    def python_OP_Sequence(self, pOp):
        capi.impl_OP_Sequence(self.p, pOp)

    def python_OP_OpenPseudo(self, pc, rc, pOp):
        return capi.impl_OP_OpenPseudo(self.p, self.db, pc, rc, pOp)

    def python_OP_SorterSort_Sort(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_SorterSort_Sort(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_SorterData(self, pOp):
        return capi.impl_OP_SorterData(self.p, pOp)

    def python_OP_SorterNext(self, pc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_SorterNext(self.p, self.db, self.internalPc, pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_Noop_Explain(self, pOp):
        translated.python_OP_Noop_Explain_translated(pOp)

    def python_OP_Compare(self, pOp):
        capi.impl_OP_Compare(self.p, pOp)

    def python_OP_Jump(self, pOp):
        return capi.impl_OP_Jump(pOp)

    def python_OP_IfPos(self, pc, pOp):
        return capi.impl_OP_IfPos(self.p, pc, pOp)

    def python_OP_CollSeq(self, pOp):
        capi.impl_OP_CollSeq(self.p, pOp)

    def python_OP_NotNull(self, pc, pOp):
        return translated.python_OP_NotNull(self, pc, pOp)

    def python_OP_InitCoroutine(self, pc, pOp):
        return capi.impl_OP_InitCoroutine(self.p, pc, pOp)

    def python_OP_Yield(self, pc, pOp):
        return capi.impl_OP_Yield(self.p, pc, pOp)

    def python_OP_NullRow(self, pOp):
        capi.impl_OP_NullRow(self.p, pOp)

    def python_OP_EndCoroutine(self, pOp):
        return capi.impl_OP_EndCoroutine(self.p, pOp)

    def python_OP_ReadCookie(self, pOp):
        capi.impl_OP_ReadCookie(self.p, self.db, pOp)

    def python_OP_NewRowid(self, pc, rc, pOp):
        return capi.impl_OP_NewRowid(self.p, self.db, pc, rc, pOp)

    def python_OP_Insert_InsertInt(self, pOp):
        return capi.impl_OP_Insert_InsertInt(self.p, self.db, pOp)

    def python_OP_SetCookie(self, pOp):
        return capi.impl_OP_SetCookie(self.p, self.db, pOp)

    def python_OP_ParseSchema(self, pc, rc, pOp):
        return capi.impl_OP_ParseSchema(self.p, self.db, pc, rc, pOp)

    def python_OP_RowSetAdd(self, pc, rc, pOp):
        return capi.impl_OP_RowSetAdd(self.p, self.db, pc, rc, pOp)

    def python_OP_RowSetRead(self, pc, rc, pOp):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        retRc = capi.impl_OP_RowSetRead(self.p, self.db, self.internalPc, rc, pOp)
        retPc = self.internalPc[0]
        return retPc, retRc   

    def python_OP_Delete(self, pc, pOp):
        return capi.impl_OP_Delete(self.p, self.db, pc, pOp)

    def python_OP_DropTable(self, pOp):
        return capi.impl_OP_DropTable(self.db, pOp)


    def python_sqlite3_column_text(self, iCol):
        return capi.sqlite3_column_text(self.p, iCol)
    def python_sqlite3_column_bytes(self, iCol):
        return capi.sqlite3_column_bytes(self.p, iCol)


    def debug_print(self, s):
        return
        if not jit.we_are_jitted():
            print s

    @jit.elidable
    def get_opcode_str(self, opcode):
        return capi.opnames_dict.get(opcode, '')

    @jit.elidable
    def get_opcode(self, pOp):
        return rffi.cast(lltype.Unsigned, pOp.opcode)

    @jit.elidable
    def get_aOp(self):
        return self.p.aOp

    @jit.elidable
    def p_Signed(self, pOp, i):
        if i == 1:
            return rffi.cast(lltype.Signed, pOp.p1)
        if i == 2:
            return rffi.cast(lltype.Signed, pOp.p2)
        if i == 3:
            return rffi.cast(lltype.Signed, pOp.p3)
        if i == 5:
            return rffi.cast(lltype.Signed, pOp.p5)
        assert 0

    @jit.elidable
    def p_Unsigned(self, pOp, i):
        if i == 1:
            return rffi.cast(lltype.Unsigned, pOp.p1)
        if i == 2:
            return rffi.cast(lltype.Unsigned, pOp.p2)
        if i == 3:
            return rffi.cast(lltype.Unsigned, pOp.p3)
        if i == 5:
            return rffi.cast(lltype.Unsigned, pOp.p5)
        assert 0

    @jit.elidable
    def p4type(self, pOp):
        return pOp.p4type

    @jit.elidable
    def p4_z(self, pOp):
        return rffi.charp2str(pOp.p4.z)

    def p2as_pc(self, pOp):
        return self.p_Signed(pOp, 2) - 1

    def mem_of_p(self, pOp, i):
        return self.p.aMem[self.p_Signed(pOp, i)]

    def mem_and_flags_of_p(self, pOp, i):
        mem = self.mem_and_flags_of_p(pOp, i)
        flags = rffi.cast(lltype.Unsigned, mem.flags)
        return mem, flags

    def mainloop(self):
        ops = self.get_aOp()
        rc = CConfig.SQLITE_OK
        pc = jit.promote(rffi.cast(lltype.Signed, self.p.pc))
        if pc < 0:
            pc = 0 # XXX maybe more to do, see vdbeapi.c:418

        i = 0
        while True:
            jitdriver.jit_merge_point(pc=pc, self_=self, ops=ops, rc=rc)
            if rc != CConfig.SQLITE_OK:
                break
            pOp = ops[pc]
            opcode = self.get_opcode(pOp)
            oldpc = pc

            self.debug_print('>>> %s <<<' % self.get_opcode_str(opcode))
            if opcode == CConfig.OP_Init:
                pc = self.python_OP_Init(pc, pOp)
            elif (opcode == CConfig.OP_OpenRead or
                  opcode == CConfig.OP_OpenWrite):
                rc = self.python_OP_OpenRead_OpenWrite(pc, pOp)
            elif opcode == CConfig.OP_Rewind:
                pc, rc = self.python_OP_Rewind(pc, pOp)
            elif opcode == CConfig.OP_Transaction:
                rc = self.python_OP_Transaction(pc, pOp)
                if rc == CConfig.SQLITE_BUSY:
                    print 'ERROR: in OP_Transaction SQLITE_BUSY'
                    return rc
            elif opcode == CConfig.OP_TableLock:
                rc = self.python_OP_TableLock(rc, pOp)
            elif opcode == CConfig.OP_Goto:
                pc, rc = self.python_OP_Goto(pc, rc, pOp)
            elif opcode == CConfig.OP_Column:
                rc = self.python_OP_Column(pc, pOp)
            elif opcode == CConfig.OP_ResultRow:
                rc = self.python_OP_ResultRow(pc, pOp)
                if rc == CConfig.SQLITE_ROW:
                    return rc
            elif opcode == CConfig.OP_Next:
                pc, rc = self.python_OP_Next(pc, pOp)
            elif opcode == CConfig.OP_Close:
                self.python_OP_Close(pOp)
            elif opcode == CConfig.OP_Halt:
                pc, rc = self.python_OP_Halt(pc, pOp)
                return rc
            elif (opcode == CConfig.OP_Eq or 
                  opcode == CConfig.OP_Ne or 
                  opcode == CConfig.OP_Lt or 
                  opcode == CConfig.OP_Le or 
                  opcode == CConfig.OP_Gt or 
                  opcode == CConfig.OP_Ge):
                pc, rc = self.python_OP_Ne_Eq_Gt_Le_Lt_Ge(pc, rc, pOp)
            elif opcode == CConfig.OP_Integer:
                self.python_OP_Integer(pOp)
            elif opcode == CConfig.OP_Null:
                self.python_OP_Null(pOp)
            elif opcode == CConfig.OP_AggStep:
                rc = self.python_OP_AggStep(rc, pOp)
            elif opcode == CConfig.OP_AggFinal:
                rc = self.python_OP_AggFinal(pc, rc, pOp)
            elif opcode == CConfig.OP_Copy:
                rc = self.python_OP_Copy(pc, rc, pOp)
            elif opcode == CConfig.OP_MustBeInt:
                pc, rc = self.python_OP_MustBeInt(pc, rc, pOp)
            elif opcode == CConfig.OP_NotExists:
                pc, rc = self.python_OP_NotExists(pc, pOp)
            elif opcode == CConfig.OP_String:
                self.python_OP_String(pOp)
            elif opcode == CConfig.OP_String8:
                rc = self.python_OP_String8(pc, rc, pOp)
            elif opcode == CConfig.OP_Function:
                rc = self.python_OP_Function(pc, rc, pOp)
            elif opcode == CConfig.OP_Real:
                self.python_OP_Real(pOp)
            elif opcode == CConfig.OP_RealAffinity:
                self.python_OP_RealAffinity(pOp)
            elif (opcode == CConfig.OP_Add or 
                  opcode == CConfig.OP_Subtract or 
                  opcode == CConfig.OP_Multiply or 
                  opcode == CConfig.OP_Divide or 
                  opcode == CConfig.OP_Remainder):
                self.python_OP_Add_Subtract_Multiply_Divide_Remainder(pOp)
            elif (opcode == CConfig.OP_If or
                  opcode == CConfig.OP_IfNot):
                pc = self.python_OP_If_IfNot(pc, pOp)
            elif opcode == CConfig.OP_Rowid:
                rc = self.python_OP_Rowid(pc, rc, pOp)
            elif opcode == CConfig.OP_IsNull:
                pc = self.python_OP_IsNull(pc, pOp)
            elif (opcode == CConfig.OP_SeekLT or 
                  opcode == CConfig.OP_SeekLE or 
                  opcode == CConfig.OP_SeekGE or 
                  opcode == CConfig.OP_SeekGT):
                pc, rc = self.python_OP_SeekLT_SeekLE_SeekGE_SeekGT(pc, rc, pOp)
            elif opcode == CConfig.OP_Move:
                self.python_OP_Move(pOp)
            elif opcode == CConfig.OP_IfZero:
                pc = self.python_OP_IfZero(pc, pOp)
            elif opcode == CConfig.OP_IdxRowid:
                rc = self.python_OP_IdxRowid(pc, rc, pOp)
            elif (opcode == CConfig.OP_IdxLE or 
                  opcode == CConfig.OP_IdxGT or 
                  opcode == CConfig.OP_IdxLT or 
                  opcode == CConfig.OP_IdxGE):
                pc, rc = self.python_OP_IdxLE_IdxGT_IdxLT_IdxGE(pc, pOp)
            elif opcode == CConfig.OP_Seek:
                self.python_OP_Seek(pOp)
            elif opcode == CConfig.OP_Once:
                pc = self.python_OP_Once(pc, pOp)
            elif opcode == CConfig.OP_SCopy:
                self.python_OP_SCopy(pOp)
            elif opcode == CConfig.OP_Affinity:
                self.python_OP_Affinity(pOp)
            elif (opcode == CConfig.OP_OpenAutoindex or 
                  opcode == CConfig.OP_OpenEphemeral):
                rc = self.python_OP_OpenAutoindex_OpenEphemeral(pc, pOp)
            elif opcode == CConfig.OP_MakeRecord:
                rc = self.python_OP_MakeRecord(pc, rc, pOp)
            elif (opcode == CConfig.OP_SorterInsert or 
                  opcode == CConfig.OP_IdxInsert):
                rc = self.python_OP_SorterInsert_IdxInsert(pOp)
            elif (opcode == CConfig.OP_NoConflict or 
                  opcode == CConfig.OP_NotFound or 
                  opcode == CConfig.OP_Found):
                pc, rc = self.python_OP_NoConflict_NotFound_Found(pc, rc, pOp)
            elif opcode == CConfig.OP_RowSetTest:
                pc, rc = self.python_OP_RowSetTest(pc, rc, pOp)
            elif opcode == CConfig.OP_Gosub:
                pc = self.python_OP_Gosub(pc, pOp)
            elif opcode == CConfig.OP_Return:
                pc = self.python_OP_Return(pc, pOp)
            elif opcode == CConfig.OP_SorterOpen:
                rc = self.python_OP_SorterOpen(pc, pOp)
            elif opcode == CConfig.OP_NextIfOpen:
                pc, rc = self.python_OP_NextIfOpen(pc, rc, pOp)
            elif opcode == CConfig.OP_Sequence:
                self.python_OP_Sequence(pOp)
            elif opcode == CConfig.OP_OpenPseudo:
                rc = self.python_OP_OpenPseudo(pc, rc, pOp)
            elif (opcode == CConfig.OP_SorterSort or 
                  opcode == CConfig.OP_Sort):
                pc, rc = self.python_OP_SorterSort_Sort(pc, pOp)
            elif opcode == CConfig.OP_SorterData:
                rc = self.python_OP_SorterData(pOp)
            elif opcode == CConfig.OP_SorterNext:
                pc, rc = self.python_OP_SorterNext(pc, pOp)
            elif (opcode == CConfig.OP_Noop or 
                  opcode == CConfig.OP_Explain):
                self.python_OP_Noop_Explain(pOp)
            elif opcode == CConfig.OP_Compare:
                self.python_OP_Compare(pOp)
            elif opcode == CConfig.OP_Jump:
                pc = self.python_OP_Jump(pOp)
            elif opcode == CConfig.OP_IfPos:
                pc = self.python_OP_IfPos(pc, pOp)
            elif opcode == CConfig.OP_CollSeq:
                self.python_OP_CollSeq(pOp)
            elif opcode == CConfig.OP_NotNull:
                pc = self.python_OP_NotNull(pc, pOp)
            elif opcode == CConfig.OP_InitCoroutine:
                pc = self.python_OP_InitCoroutine(pc, pOp)
            elif opcode == CConfig.OP_Yield:
                pc = self.python_OP_Yield(pc, pOp)
            elif opcode == CConfig.OP_NullRow:
                self.python_OP_NullRow(pOp)
            elif opcode == CConfig.OP_EndCoroutine:
                pc = self.python_OP_EndCoroutine(pOp)
            elif opcode == CConfig.OP_ReadCookie:
                self.python_OP_ReadCookie(pOp)
            elif opcode == CConfig.OP_NewRowid:
                rc = self.python_OP_NewRowid(pc, rc, pOp)
            elif (opcode == CConfig.OP_Insert or 
                  opcode == CConfig.OP_InsertInt):
                rc = self.python_OP_Insert_InsertInt(pOp)
            elif opcode == CConfig.OP_SetCookie:
                rc = self.python_OP_SetCookie(pOp)
            elif opcode == CConfig.OP_ParseSchema:
                rc = self.python_OP_ParseSchema(pc, rc, pOp)
            elif opcode == CConfig.OP_RowSetAdd:
                rc = self.python_OP_RowSetAdd(pc, rc, pOp)
            elif opcode == CConfig.OP_RowSetRead:
                pc, rc = self.python_OP_RowSetRead(pc, rc, pOp)
            elif opcode == CConfig.OP_Delete:
                rc = self.python_OP_Delete(pc, pOp)
            elif opcode == CConfig.OP_DropTable:
                self.python_OP_DropTable(pOp)
            else:
                raise SQPyteException("SQPyteException: Unimplemented bytecode %s." % opcode)
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
