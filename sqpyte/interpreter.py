from rpython.rlib import jit, objectmodel
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

def get_printable_location(pc, rc, self, cache_state):
    op = self._hlops[pc]
    opcode = op.get_opcode()
    name = self.get_opcode_str(opcode)
    unsafe = ''
    if not _cache_safe_opcodes[opcode]:
        unsafe = ' UNSAFE'
    return "%s %s %s %s %s" % (pc, rc, name, cache_state.repr(), unsafe)


jitdriver = jit.JitDriver(
    greens=['pc', 'rc', 'self_', 'cache_state'],
    reds=[],#['_mem_caches'],
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

_cache_safe_opcodes = [False] * 256

def cache_safe(opcodes=None, hide=False, mutates=None):
    assert opcodes is None or isinstance(opcodes, list)
    def decorate(func):
        ops = opcodes
        if ops is None:
            name = func.func_name
            assert name.startswith("python_")
            opcodename = name[len("python_"):]
            ops = [getattr(CConfig, opcodename)]
        for opcode in ops:
            _cache_safe_opcodes[opcode] = True
        if hide:
            func = hide_cache(func)
        if mutates is not None:
            if not isinstance(mutates, list):
                all_mutates = [mutates]
            else:
                all_mutates = mutates

            for mut in all_mutates:
                func = mutate_func(func, mut)
        return func
    return decorate

def mutate_func(func, mutates):
    if mutates == "p1":
        def p1_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            i = op.p_Signed(1)
            hlquery.mem_cache.invalidate(i)
            return result
        return p1_mutation
    if mutates == "p2":
        def p2_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            i = op.p_Signed(2)
            hlquery.mem_cache.invalidate(i)
            return result
        return p2_mutation
    if mutates == "p3":
        def p3_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            i = op.p_Signed(3)
            hlquery.mem_cache.invalidate(i)
            return result
        return p3_mutation
    if mutates == "p1@p2":
        @jit.unroll_safe
        def p1_p2_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            for i in range(op.p_Signed(1), op.p_Signed(1) + op.p_Signed(2)):
                hlquery.mem_cache.invalidate(i)
            return result
        return p1_p2_mutation
    if mutates == "p3@p4":
        @jit.unroll_safe
        def p3_p4_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            for i in range(op.p_Signed(3), op.p_Signed(3) + op.p4_i()):
                hlquery.mem_cache.invalidate(i)
            return result
        return p3_p4_mutation
    if mutates == "p3@p4 or p3":
        @jit.unroll_safe
        def p3_p4_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            length = op.p4_i()
            if not length:
                length = 1
            for i in range(op.p_Signed(3), op.p_Signed(3) + length):
                hlquery.mem_cache.invalidate(i)
            return result
        return p3_p4_mutation
    if mutates == "p2@p5":
        @jit.unroll_safe
        def p2_p5_mutation(hlquery, *args):
            result = func(hlquery, *args)
            op = args[-1]
            for i in range(op.p_Signed(2), op.p_Signed(2) + op.p_Signed(5)):
                hlquery.mem_cache.invalidate(i)
            return result
        return p2_p5_mutation
    else:
        raise ValueError("unknown mutation %s" % mutates)

def hide_cache(func):
    def newfunc(hlquery, *args):
        data = hlquery.mem_cache.hide()
        result = func(hlquery, *args)
        hlquery.mem_cache.reveal(data)
        return result
    return newfunc

class Sqlite3Query(object):

    _immutable_fields_ = ['internalPc', 'db', 'p', '_mem_as_python_list[*]', '_llmem_as_python_list[*]', 'intp', 'longp', 'unpackedrecordp',
                          '_hlops[*]', 'mem_cache']

    def __init__(self, db, query):
        self.db = db
        self.internalPc = lltype.malloc(rffi.LONGP.TO, 1, flavor='raw')
        self.unpackedrecordp = lltype.malloc(capi.UNPACKEDRECORD, flavor='raw')
        self.intp = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')
        self.longp = lltype.malloc(rffi.LONGP.TO, 1, flavor='raw')
        self.prepare(query)
        self.iCompare = 0

    def __del__(self):
        lltype.free(self.internalPc, flavor='raw')
        lltype.free(self.intp, flavor='raw')
        lltype.free(self.longp, flavor='raw')
        lltype.free(self.unpackedrecordp, flavor='raw')

    def prepare(self, query):
        length = len(query)
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare(self.db, query, length, result, unused_buffer)
            assert errorcode == 0
            self.p = rffi.cast(capi.VDBEP, result[0])
        self._init_python_data()

    def _init_python_data(self):
        from sqpyte.mem import Mem
        self._llmem_as_python_list = [self.p.aMem[i] for i in range(self.p.nMem)]
        self._mem_as_python_list = [Mem(self, self.p.aMem[i], i)
                for i in range(self.p.nMem)]
        self._hlops = [Op(self, self.p.aOp[i]) for i in range(self.p.nOp)]
        self.init_mem_cache()

    def init_mem_cache(self):
        from sqpyte.mem import CacheHolder
        self.mem_cache = CacheHolder(len(self._mem_as_python_list))

    def check_cache_consistency(self):
        if objectmodel.we_are_translated():
            return
        for mem in self._mem_as_python_list:
            mem.check_cache_consistency()


    @jit.unroll_safe
    def invalidate_caches(self):
        for mem in self._mem_as_python_list:
            mem.invalidate_cache()

    def is_op_cache_safe(self, opcode):
        return _cache_safe_opcodes[opcode]

    def reset_query(self):
        capi.sqlite3_reset(self.p)

    def python_OP_Init(self, pc, op):
        return translated.python_OP_Init_translated(self, pc, op)

    @cache_safe()
    def python_OP_Rewind(self, pc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_Rewind(self.p, self.db, self.internalPc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Transaction(self, pc, op):
        return capi.impl_OP_Transaction(self.p, self.db, pc, op.pOp)

    def python_OP_TableLock(self, rc, op):
        return capi.impl_OP_TableLock(self.p, self.db, rc, op.pOp)

    @cache_safe()
    def python_OP_Goto(self, pc, rc, op):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # retRc = capi.impl_OP_Goto(self.p, self.db, self.internalPc, rc, op.pOp)
        # retPc = self.internalPc[0]
        # return retPc, retRc

        return translated.python_OP_Goto_translated(self, pc, rc, op)

    @cache_safe(opcodes=[CConfig.OP_OpenRead, CConfig.OP_OpenWrite])
    def python_OP_OpenRead_OpenWrite(self, pc, op):
        if op.p_Signed(5):
            self.mem_cache.invalidate(op.p_Signed(2))
        return capi.impl_OP_OpenRead_OpenWrite(self.p, self.db, pc, op.pOp)
        # translated.python_OP_OpenRead_OpenWrite_translated(self, self.db, pc, op)

    @cache_safe(hide=True, mutates="p3")
    def python_OP_Column(self, pc, op):
        return capi.impl_OP_Column(self.p, self.db, pc, op.pOp)

    @cache_safe(hide=True, mutates="p1@p2")
    def python_OP_ResultRow(self, pc, op):
        return capi.impl_OP_ResultRow(self.p, self.db, pc, op.pOp)

    @cache_safe()
    def python_OP_Next(self, pc, op):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_Next(self.p, self.db, self.internalPc, op.pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc

        return translated.python_OP_Next_translated(self, pc, op)

    def python_OP_Close(self, op):
        capi.impl_OP_Close(self.p, op.pOp)

    def python_OP_Halt(self, pc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_Halt(self.p, self.db, self.internalPc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    @cache_safe(
        opcodes=[CConfig.OP_Eq, CConfig.OP_Ne, CConfig.OP_Lt, CConfig.OP_Le,
                 CConfig.OP_Gt, CConfig.OP_Ge])
    def python_OP_Ne_Eq_Gt_Le_Lt_Ge(self, pc, rc, op):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_Ne_Eq_Gt_Le_Lt_Ge(self.p, self.db, self.internalPc, rc, op.pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc

        return translated.python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(self, pc, rc, op)

    @cache_safe()
    def python_OP_Integer(self, op):
        translated.python_OP_Integer(self, op)
        #capi.impl_OP_Integer(self.p, op.pOp)

    def python_OP_Null(self, op):
        capi.impl_OP_Null(self.p, op.pOp)

    def python_OP_AggStep(self, rc, pc, op):
        #return capi.impl_OP_AggStep(self.p, self.db, rc, op.pOp)
        return translated.python_OP_AggStep(self, rc, pc, op)

    def python_OP_AggFinal(self, pc, rc, op):
        return capi.impl_OP_AggFinal(self.p, self.db, pc, rc, op.pOp)

    def python_OP_Copy(self, pc, rc, op):
        return capi.impl_OP_Copy(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_MustBeInt(self, pc, rc, op):
        return translated.python_OP_MustBeInt(self, pc, rc, op)

    @cache_safe()
    def python_OP_NotExists(self, pc, op):
        return translated.python_OP_NotExists(self, pc, op)
        #self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        #rc = capi.impl_OP_NotExists(self.p, self.db, self.internalPc, op.pOp)
        #retPc = self.internalPc[0]
        #return retPc, rc

    def python_OP_String(self, op):
        capi.impl_OP_String(self.p, self.db, op.pOp)

    def python_OP_String8(self, pc, rc, op):
        return capi.impl_OP_String8(self.p, self.db, pc, rc, op.pOp)

    @cache_safe(mutates=["p3", "p2@p5"])
    def python_OP_Function(self, pc, rc, op):
        return capi.impl_OP_Function(self.p, self.db, pc, rc, op.pOp)

    def python_OP_Real(self, op):
        # aMem = self.p.aMem
        # pOut = aMem[pOp.p2]
        # pOut.flags = rffi.cast(rffi.USHORT, CConfig.MEM_Real)
        # assert not math.isnan(pOp.p4.pReal)
        # pOut.r = pOp.p4.pReal

        capi.impl_OP_Real(self.p, op.pOp)

    @cache_safe()
    def python_OP_RealAffinity(self, op):
        # capi.impl_OP_RealAffinity(self.p, op.pOp)
        translated.python_OP_RealAffinity(self, op)

    @cache_safe(
        opcodes=[CConfig.OP_Add, CConfig.OP_Subtract, CConfig.OP_Multiply,
                 CConfig.OP_Divide, CConfig.OP_Remainder])
    def python_OP_Add_Subtract_Multiply_Divide_Remainder(self, op):
        # capi.impl_OP_Add_Subtract_Multiply_Divide_Remainder(self.p, op.pOp)
        translated.python_OP_Add_Subtract_Multiply_Divide_Remainder(self, op)

    @cache_safe(
        opcodes=[CConfig.OP_If, CConfig.OP_IfNot])
    def python_OP_If_IfNot(self, pc, op):
        # return capi.impl_OP_If_IfNot(self.p, pc, op.pOp)
        return translated.python_OP_If_IfNot(self, pc, op)

    def python_OP_Rowid(self, pc, rc, op):
        return capi.impl_OP_Rowid(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_IsNull(self, pc, op):
        # return capi.impl_OP_IsNull(self.p, pc, op.pOp)
        return translated.python_OP_IsNull(self, pc, op)

    def python_OP_SeekLT_SeekLE_SeekGE_SeekGT(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_SeekLT_SeekLE_SeekGE_SeekGT(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_Move(self, op):
        capi.impl_OP_Move(self.p, op.pOp)

    @cache_safe()
    def python_OP_IfZero(self, pc, op):
        # XXX port me?
        return capi.impl_OP_IfZero(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_IdxRowid(self, pc, rc, op):
        return translated.python_OP_IdxRowid(self, pc, rc, op)
        #return capi.impl_OP_IdxRowid(self.p, self.db, pc, rc, op.pOp)

    @cache_safe(
        opcodes=[CConfig.OP_IdxLE, CConfig.OP_IdxGT, CConfig.OP_IdxLT, CConfig.OP_IdxGE],
        mutates="p3@p4")
    def python_OP_IdxLE_IdxGT_IdxLT_IdxGE(self, pc, op):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_IdxLE_IdxGT_IdxLT_IdxGE(self.p, self.internalPc, op.pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc

        return translated.python_OP_IdxLE_IdxGT_IdxLT_IdxGE(self, pc, op)

    @cache_safe()
    def python_OP_Seek(self, op):
        #capi.impl_OP_Seek(self.p, op.pOp)
        translated.python_OP_Seek(self, op)

    def python_OP_Once(self, pc, op):
        # return capi.impl_OP_Once(self.p, pc, op.pOp)
        return translated.python_OP_Once(self, pc, op)

    @cache_safe(mutates=["p1", "p2"])
    def python_OP_SCopy(self, op):
        # capi.impl_OP_SCopy(self.p, op.pOp)
        translated.python_OP_SCopy(self, op)

    @cache_safe()
    def python_OP_Affinity(self, op):
        # capi.impl_OP_Affinity(self.p, self.db, op.pOp)
        translated.python_OP_Affinity(self, op)

    def python_OP_OpenAutoindex_OpenEphemeral(self, pc, op):
        return capi.impl_OP_OpenAutoindex_OpenEphemeral(self.p, self.db, pc, op.pOp)

    @cache_safe(mutates=["p3", "p1@p2"])
    def python_OP_MakeRecord(self, pc, rc, op):
        return capi.impl_OP_MakeRecord(self.p, self.db, pc, rc, op.pOp)

    @cache_safe(opcodes=[CConfig.OP_SorterInsert, CConfig.OP_IdxInsert],
                mutates=["p2"])
    def python_OP_SorterInsert_IdxInsert(self, op):
        return capi.impl_OP_SorterInsert_IdxInsert(self.p, self.db, op.pOp)

    @cache_safe(
        opcodes=[CConfig.OP_NoConflict, CConfig.OP_NotFound, CConfig.OP_Found],
        mutates="p3@p4 or p3")
    def python_OP_NoConflict_NotFound_Found(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_NoConflict_NotFound_Found(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_RowSetTest(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_RowSetTest(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_Gosub(self, pc, op):
        return capi.impl_OP_Gosub(self.p, pc, op.pOp)

    def python_OP_Return(self, pc, op):
        return capi.impl_OP_Return(self.p, pc, op.pOp)

    def python_OP_SorterOpen(self, pc, op):
        return capi.impl_OP_SorterOpen(self.p, self.db, pc, op.pOp)

    def python_OP_NextIfOpen(self, pc, rc, op):
        # self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        # rc = capi.impl_OP_NextIfOpen(self.p, self.db, self.internalPc, rc, op.pOp)
        # retPc = self.internalPc[0]
        # return retPc, rc        

        return translated.python_OP_NextIfOpen_translated(self, pc, rc, op)

    @cache_safe(mutates="p2")
    def python_OP_Sequence(self, op):
        capi.impl_OP_Sequence(self.p, op.pOp)

    def python_OP_OpenPseudo(self, pc, rc, op):
        return capi.impl_OP_OpenPseudo(self.p, self.db, pc, rc, op.pOp)

    def python_OP_SorterSort_Sort(self, pc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_SorterSort_Sort(self.p, self.db, self.internalPc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    @cache_safe(mutates="p2")
    def python_OP_SorterData(self, op):
        return capi.impl_OP_SorterData(self.p, op.pOp)

    @cache_safe()
    def python_OP_SorterNext(self, pc, op):
        # XXX would be very simple to port, it looks like half of OP_Next
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_SorterNext(self.p, self.db, self.internalPc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc        

    def python_OP_Noop_Explain(self, op):
        translated.python_OP_Noop_Explain_translated(op)

    @cache_safe()
    def python_OP_Compare(self, op):
        # capi.impl_OP_Compare(self.p, op.pOp)
        translated.python_OP_Compare(self, op)

    @cache_safe()
    def python_OP_Jump(self, op):
        # return capi.impl_OP_Jump(op.pOp)
        return translated.python_OP_Jump(self, op)

    def python_OP_IfPos(self, pc, op):
        return translated.python_OP_IfPos(self, pc, op)

    def python_OP_CollSeq(self, op):
        capi.impl_OP_CollSeq(self.p, op.pOp)

    @cache_safe()
    def python_OP_NotNull(self, pc, op):
        # return capi.impl_OP_NotNull(self.p, pc, op.pOp)
        return translated.python_OP_NotNull(self, pc, op)

    def python_OP_InitCoroutine(self, pc, op):
        return capi.impl_OP_InitCoroutine(self.p, pc, op.pOp)

    def python_OP_Yield(self, pc, op):
        return capi.impl_OP_Yield(self.p, pc, op.pOp)

    def python_OP_NullRow(self, op):
        capi.impl_OP_NullRow(self.p, op.pOp)

    def python_OP_EndCoroutine(self, op):
        return capi.impl_OP_EndCoroutine(self.p, op.pOp)

    def python_OP_ReadCookie(self, op):
        capi.impl_OP_ReadCookie(self.p, self.db, op.pOp)

    def python_OP_NewRowid(self, pc, rc, op):
        return capi.impl_OP_NewRowid(self.p, self.db, pc, rc, op.pOp)

    def python_OP_Insert_InsertInt(self, op):
        return capi.impl_OP_Insert_InsertInt(self.p, self.db, op.pOp)

    def python_OP_SetCookie(self, op):
        return capi.impl_OP_SetCookie(self.p, self.db, op.pOp)

    def python_OP_ParseSchema(self, pc, rc, op):
        return capi.impl_OP_ParseSchema(self.p, self.db, pc, rc, op.pOp)

    def python_OP_RowSetAdd(self, pc, rc, op):
        return capi.impl_OP_RowSetAdd(self.p, self.db, pc, rc, op.pOp)

    def python_OP_RowSetRead(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        retRc = capi.impl_OP_RowSetRead(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, retRc   

    def python_OP_Delete(self, pc, op):
        return capi.impl_OP_Delete(self.p, self.db, pc, op.pOp)

    def python_OP_DropTable(self, op):
        return capi.impl_OP_DropTable(self.db, op.pOp)


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
    def get_aOp(self):
        return self.p.aOp

    @jit.elidable
    def enc(self):
        return self.db.aDb[0].pSchema.enc

    def mem_with_index(self, i):
        return self._mem_as_python_list[i]

    def mainloop(self):
        rc = CConfig.SQLITE_OK
        pc = jit.promote(rffi.cast(lltype.Signed, self.p.pc))
        if pc < 0:
            pc = 0 # XXX maybe more to do, see vdbeapi.c:418

        while True:
            jitdriver.jit_merge_point(pc=pc, self_=self, rc=rc, cache_state=self.mem_cache._cache_state)
            if rc != CConfig.SQLITE_OK:
                break
            op = self._hlops[pc]
            opcode = op.get_opcode()
            oldpc = pc
            self.debug_print('>>> %s <<<' % self.get_opcode_str(opcode))

            opflags = op.opflags()
            if opflags & CConfig.OPFLG_OUT2_PRERELEASE:
                pOut = op.mem_of_p(2)
                pOut.VdbeMemRelease()
                pOut.set_flags(CConfig.MEM_Int)

            self.check_cache_consistency()
            if not self.is_op_cache_safe(opcode):
                self.invalidate_caches()
            self.check_cache_consistency()

            if opcode == CConfig.OP_Init:
                pc = self.python_OP_Init(pc, op)
            elif (opcode == CConfig.OP_OpenRead or
                  opcode == CConfig.OP_OpenWrite):
                rc = self.python_OP_OpenRead_OpenWrite(pc, op)
            elif opcode == CConfig.OP_Rewind:
                pc, rc = self.python_OP_Rewind(pc, op)
            elif opcode == CConfig.OP_Transaction:
                rc = self.python_OP_Transaction(pc, op)
                if rc == CConfig.SQLITE_BUSY:
                    print 'ERROR: in OP_Transaction SQLITE_BUSY'
                    return rc
            elif opcode == CConfig.OP_TableLock:
                rc = self.python_OP_TableLock(rc, op)
            elif opcode == CConfig.OP_Goto:
                pc, rc = self.python_OP_Goto(pc, rc, op)
            elif opcode == CConfig.OP_Column:
                rc = self.python_OP_Column(pc, op)
            elif opcode == CConfig.OP_ResultRow:
                rc = self.python_OP_ResultRow(pc, op)
                if rc == CConfig.SQLITE_ROW:
                    return rc
            elif opcode == CConfig.OP_Next:
                pc, rc = self.python_OP_Next(pc, op)
            elif opcode == CConfig.OP_Close:
                self.python_OP_Close(op)
            elif opcode == CConfig.OP_Halt:
                pc, rc = self.python_OP_Halt(pc, op)
                return rc
            elif (opcode == CConfig.OP_Eq or 
                  opcode == CConfig.OP_Ne or 
                  opcode == CConfig.OP_Lt or 
                  opcode == CConfig.OP_Le or 
                  opcode == CConfig.OP_Gt or 
                  opcode == CConfig.OP_Ge):
                pc, rc = self.python_OP_Ne_Eq_Gt_Le_Lt_Ge(pc, rc, op)
            elif opcode == CConfig.OP_Integer:
                self.python_OP_Integer(op)
            elif opcode == CConfig.OP_Null:
                self.python_OP_Null(op)
            elif opcode == CConfig.OP_AggStep:
                rc = self.python_OP_AggStep(rc, pc, op)
            elif opcode == CConfig.OP_AggFinal:
                rc = self.python_OP_AggFinal(pc, rc, op)
            elif opcode == CConfig.OP_Copy:
                rc = self.python_OP_Copy(pc, rc, op)
            elif opcode == CConfig.OP_MustBeInt:
                pc, rc = self.python_OP_MustBeInt(pc, rc, op)
            elif opcode == CConfig.OP_NotExists:
                pc, rc = self.python_OP_NotExists(pc, op)
            elif opcode == CConfig.OP_String:
                self.python_OP_String(op)
            elif opcode == CConfig.OP_String8:
                rc = self.python_OP_String8(pc, rc, op)
            elif opcode == CConfig.OP_Function:
                rc = self.python_OP_Function(pc, rc, op)
            elif opcode == CConfig.OP_Real:
                self.python_OP_Real(op)
            elif opcode == CConfig.OP_RealAffinity:
                self.python_OP_RealAffinity(op)
            elif (opcode == CConfig.OP_Add or 
                  opcode == CConfig.OP_Subtract or 
                  opcode == CConfig.OP_Multiply or 
                  opcode == CConfig.OP_Divide or 
                  opcode == CConfig.OP_Remainder):
                self.python_OP_Add_Subtract_Multiply_Divide_Remainder(op)
            elif (opcode == CConfig.OP_If or
                  opcode == CConfig.OP_IfNot):
                pc = self.python_OP_If_IfNot(pc, op)
            elif opcode == CConfig.OP_Rowid:
                rc = self.python_OP_Rowid(pc, rc, op)
            elif opcode == CConfig.OP_IsNull:
                pc = self.python_OP_IsNull(pc, op)
            elif (opcode == CConfig.OP_SeekLT or 
                  opcode == CConfig.OP_SeekLE or 
                  opcode == CConfig.OP_SeekGE or 
                  opcode == CConfig.OP_SeekGT):
                pc, rc = self.python_OP_SeekLT_SeekLE_SeekGE_SeekGT(pc, rc, op)
            elif opcode == CConfig.OP_Move:
                self.python_OP_Move(op)
            elif opcode == CConfig.OP_IfZero:
                pc = self.python_OP_IfZero(pc, op)
            elif opcode == CConfig.OP_IdxRowid:
                rc = self.python_OP_IdxRowid(pc, rc, op)
            elif (opcode == CConfig.OP_IdxLE or 
                  opcode == CConfig.OP_IdxGT or 
                  opcode == CConfig.OP_IdxLT or 
                  opcode == CConfig.OP_IdxGE):
                pc, rc = self.python_OP_IdxLE_IdxGT_IdxLT_IdxGE(pc, op)
            elif opcode == CConfig.OP_Seek:
                self.python_OP_Seek(op)
            elif opcode == CConfig.OP_Once:
                pc = self.python_OP_Once(pc, op)
            elif opcode == CConfig.OP_SCopy:
                self.python_OP_SCopy(op)
            elif opcode == CConfig.OP_Affinity:
                self.python_OP_Affinity(op)
            elif (opcode == CConfig.OP_OpenAutoindex or 
                  opcode == CConfig.OP_OpenEphemeral):
                rc = self.python_OP_OpenAutoindex_OpenEphemeral(pc, op)
            elif opcode == CConfig.OP_MakeRecord:
                rc = self.python_OP_MakeRecord(pc, rc, op)
            elif (opcode == CConfig.OP_SorterInsert or 
                  opcode == CConfig.OP_IdxInsert):
                rc = self.python_OP_SorterInsert_IdxInsert(op)
            elif (opcode == CConfig.OP_NoConflict or 
                  opcode == CConfig.OP_NotFound or 
                  opcode == CConfig.OP_Found):
                pc, rc = self.python_OP_NoConflict_NotFound_Found(pc, rc, op)
            elif opcode == CConfig.OP_RowSetTest:
                pc, rc = self.python_OP_RowSetTest(pc, rc, op)
            elif opcode == CConfig.OP_Gosub:
                pc = self.python_OP_Gosub(pc, op)
            elif opcode == CConfig.OP_Return:
                pc = self.python_OP_Return(pc, op)
            elif opcode == CConfig.OP_SorterOpen:
                rc = self.python_OP_SorterOpen(pc, op)
            elif opcode == CConfig.OP_NextIfOpen:
                pc, rc = self.python_OP_NextIfOpen(pc, rc, op)
            elif opcode == CConfig.OP_Sequence:
                self.python_OP_Sequence(op)
            elif opcode == CConfig.OP_OpenPseudo:
                rc = self.python_OP_OpenPseudo(pc, rc, op)
            elif (opcode == CConfig.OP_SorterSort or 
                  opcode == CConfig.OP_Sort):
                pc, rc = self.python_OP_SorterSort_Sort(pc, op)
            elif opcode == CConfig.OP_SorterData:
                rc = self.python_OP_SorterData(op)
            elif opcode == CConfig.OP_SorterNext:
                pc, rc = self.python_OP_SorterNext(pc, op)
            elif (opcode == CConfig.OP_Noop or 
                  opcode == CConfig.OP_Explain):
                self.python_OP_Noop_Explain(op)
            elif opcode == CConfig.OP_Compare:
                self.python_OP_Compare(op)
            elif opcode == CConfig.OP_Jump:
                pc = self.python_OP_Jump(op)
            elif opcode == CConfig.OP_IfPos:
                pc = self.python_OP_IfPos(pc, op)
            elif opcode == CConfig.OP_CollSeq:
                self.python_OP_CollSeq(op)
            elif opcode == CConfig.OP_NotNull:
                pc = self.python_OP_NotNull(pc, op)
            elif opcode == CConfig.OP_InitCoroutine:
                pc = self.python_OP_InitCoroutine(pc, op)
            elif opcode == CConfig.OP_Yield:
                pc = self.python_OP_Yield(pc, op)
            elif opcode == CConfig.OP_NullRow:
                self.python_OP_NullRow(op)
            elif opcode == CConfig.OP_EndCoroutine:
                pc = self.python_OP_EndCoroutine(op)
            elif opcode == CConfig.OP_ReadCookie:
                self.python_OP_ReadCookie(op)
            elif opcode == CConfig.OP_NewRowid:
                rc = self.python_OP_NewRowid(pc, rc, op)
            elif (opcode == CConfig.OP_Insert or 
                  opcode == CConfig.OP_InsertInt):
                rc = self.python_OP_Insert_InsertInt(op)
            elif opcode == CConfig.OP_SetCookie:
                rc = self.python_OP_SetCookie(op)
            elif opcode == CConfig.OP_ParseSchema:
                rc = self.python_OP_ParseSchema(pc, rc, op)
            elif opcode == CConfig.OP_RowSetAdd:
                rc = self.python_OP_RowSetAdd(pc, rc, op)
            elif opcode == CConfig.OP_RowSetRead:
                pc, rc = self.python_OP_RowSetRead(pc, rc, op)
            elif opcode == CConfig.OP_Delete:
                rc = self.python_OP_Delete(pc, op)
            elif opcode == CConfig.OP_DropTable:
                self.python_OP_DropTable(op)
            else:
                raise SQPyteException("SQPyteException: Unimplemented bytecode %s." % opcode)
            pc = jit.promote(rffi.cast(lltype.Signed, pc))
            pc += 1
            if not self.is_op_cache_safe(opcode):
                self.invalidate_caches()
            if pc <= oldpc:
                jitdriver.can_enter_jit(pc=pc, self_=self, rc=rc, cache_state=self.mem_cache._cache_state)
            self.check_cache_consistency()
        return rc

class Op(object):
    _immutable_fields_ = ['hlquery', 'pOp']

    def __init__(self, hlquery, pOp):
        self.hlquery = hlquery
        self.pOp = pOp

    @jit.elidable
    def get_opcode(self):
        return rffi.cast(lltype.Unsigned, self.pOp.opcode)

    @jit.elidable
    def p_Signed(self, i):
        if i == 1:
            return rffi.cast(lltype.Signed, self.pOp.p1)
        if i == 2:
            return rffi.cast(lltype.Signed, self.pOp.p2)
        if i == 3:
            return rffi.cast(lltype.Signed, self.pOp.p3)
        if i == 5:
            return rffi.cast(lltype.Signed, self.pOp.p5)
        assert 0

    @jit.elidable
    def p_Unsigned(self, i):
        if i == 1:
            return rffi.cast(lltype.Unsigned, self.pOp.p1)
        if i == 2:
            return rffi.cast(lltype.Unsigned, self.pOp.p2)
        if i == 3:
            return rffi.cast(lltype.Unsigned, self.pOp.p3)
        if i == 5:
            return rffi.cast(lltype.Unsigned, self.pOp.p5)
        assert 0

    @jit.elidable
    def p4type(self):
        return self.pOp.p4type

    @jit.elidable
    def p4_z(self):
        return rffi.charp2str(self.pOp.p4.z)

    @jit.elidable
    def p4_pFunc(self):
        return self.pOp.p4.pFunc

    @jit.elidable
    def p4_pColl(self):
        return self.pOp.p4.pColl

    @jit.elidable
    def p4_i(self):
        return rffi.cast(lltype.Signed, self.pOp.p4.i)

    @jit.elidable
    def p4_pKeyInfo(self):
        return self.pOp.p4.pKeyInfo

    @jit.elidable
    def p4_pKeyInfo_aColl(self, i):
        # XXX I'm rather sure that the KeyInfo is immutable, but we should
        # check to make sure
        pKeyInfo = self.p4_pKeyInfo()
        assert i < rffi.getintfield(pKeyInfo, 'nField')
        return pKeyInfo.aColl[i]

    @jit.elidable
    def p4_pKeyInfo_aSortOrder(self, i):
        # XXX I'm rather sure that the KeyInfo is immutable, but we should
        # check to make sure
        pKeyInfo = self.p4_pKeyInfo()
        assert i < rffi.getintfield(pKeyInfo, 'nField')
        return rffi.cast(lltype.Unsigned, pKeyInfo.aSortOrder[i])

    def p2as_pc(self):
        return self.p_Signed(2) - 1

    def mem_of_p(self, i):
        return self.hlquery.mem_with_index(self.p_Signed(i))

    def mem_and_flags_of_p(self, i, promote=False):
        mem = self.mem_of_p(i)
        flags = mem.get_flags(promote=promote)
        return mem, flags

    @jit.elidable
    def opflags(self):
        return rffi.cast(lltype.Unsigned, self.pOp.opflags)


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
