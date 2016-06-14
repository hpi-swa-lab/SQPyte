from rpython.rlib import jit, objectmodel
from rpython.rtyper.lltypesystem import rffi, lltype
from capi import CConfig
import sys
import os
import capi
import translated
import math

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test/test.db")
# testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test/big-test.db")

from sqpyte import opcode


def get_printable_location(pc, rc, self, cache_state):
    op = self._hlops[pc]
    name = op.get_opcode_str()
    unsafe = ''
    opcode = op.get_opcode()
    if not _cache_safe_opcodes[opcode]:
        unsafe = ' UNSAFE'
    return "%s %s %s %s %s" % (pc, rc, name, cache_state.repr(), unsafe)


_cache_safe_opcodes = [False] * 256


def cache_safe(opcodes=None, mutates=None):
    assert opcodes is None or isinstance(opcodes, list)
    def decorate(func):
        ops = opcodes
        if ops is None:
            name = func.func_name
            assert name.startswith("python_")
            opcodename = name[len("python_"):]
            ops = [getattr(CConfig, opcodename)]
        for op in ops:
            _cache_safe_opcodes[op] = True
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
    methname = mutates.replace("@", "_").replace(":", "_").replace(" ", "_") + "_mutation"
    def mutation(hlquery, *args):
        result = func(hlquery, *args)
        op = args[-1]
        getattr(hlquery, methname)(op)
        return result
    return mutation


jitdriver = jit.JitDriver(
    greens=['pc', 'rc', 'self_', 'cache_state'],
    reds=[],#['_mem_caches'],
    should_unroll_one_iteration=lambda *args: True,
    get_printable_location=get_printable_location)


class SQPyteException(Exception):
    def __init__(self, msg):
        print msg


class SqliteException(SQPyteException):
    def __init__(self, errorcode, msg):
        self.errorcode = errorcode
        self.msg = msg


class SQLite3DB(object):
    _immutable_fields_ = ['db']

    def __init__(self, db_name):
        from sqpyte.function import FuncRegistry
        self.opendb(db_name)
        self.funcregistry = FuncRegistry()

    def opendb(self, db_name):
        with rffi.scoped_str2charp(db_name) as db_name, lltype.scoped_alloc(capi.SQLITE3PP.TO, 1) as result:
            errorcode = capi.sqlite3_open(db_name, result)
            assert errorcode == 0
            self.db = rffi.cast(capi.SQLITE3P, result[0])

    def execute(self, sql, use_flag_cache=True):
        return SQLite3Query(self, sql, use_flag_cache)

    def close(self):
        if self.db:
            capi.sqlite3_close(self.db)
            self.db = lltype.nullptr(lltype.typeOf(self.db).TO)

    @jit.dont_look_inside
    def create_aggregate(self, name, nargs, contextcls):
        index, func = self.funcregistry.create_aggregate(name, nargs, contextcls)
        with rffi.scoped_str2charp(name) as name:
            # use 1 as the function pointer and pass in the index as the user
            # data
            errorcode = capi.sqlite3_create_function(
                    self.db, name, nargs, CConfig.SQLITE_UTF8,
                    rffi.cast(rffi.VOIDP, index),
                    lltype.nullptr(rffi.VOIDP.TO),
                    rffi.cast(rffi.VOIDP, 1),
                    rffi.cast(rffi.VOIDP, 1),
            )
            assert errorcode == 0
        return func

    @jit.dont_look_inside
    def create_function(self, name, nargs, callable):
        # callable has signature (RPyFunc, [args])
        index, func = self.funcregistry.create_function(name, nargs, callable)
        with rffi.scoped_str2charp(name) as name:
            # use 1 as the function pointer and pass in the index as the user
            # data
            errorcode = capi.sqlite3_create_function(
                    self.db, name, nargs, CConfig.SQLITE_UTF8,
                    rffi.cast(rffi.VOIDP, index),
                    rffi.cast(rffi.VOIDP, 1),
                    lltype.nullptr(rffi.VOIDP.TO),
                    lltype.nullptr(rffi.VOIDP.TO),
            )
            assert errorcode == 0
        return func


class SQLite3Query(object):

    _immutable_fields_ = ['db', 'p']

    def __init__(self, hldb, query, use_flag_cache=True):
        self.hldb = hldb
        self.db = hldb.db
        self.prepare(query, use_flag_cache)

    def __del__(self):
        self.close()

    def prepare(self, query, use_flag_cache):
        length = len(query)
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare_v2(rffi.cast(rffi.VOIDP, self.db), query, length, result, unused_buffer)
            if not errorcode == 0:
                raise SqliteException(errorcode, rffi.charp2str(capi.sqlite3_errmsg(self.db)))
            self.p = rffi.cast(capi.VDBEP, result[0])

    def mainloop(self):
        return capi.sqlite3_step(self.p)

    def data_count(self):
        return capi.sqlite3_column_count(self.p)

    def column_type(self, i):
        return capi.sqlite3_column_type(self.p, i)

    def column_text(self, i):
        return capi.sqlite3_column_text(self.p, i)

    def column_bytes(self, i):
        return capi.sqlite3_column_bytes(self.p, i)

    def column_int64(self, i):
        return capi.sqlite3_column_int64(self.p, i)

    def column_double(self, i):
        return capi.sqlite3_column_double(self.p, i)

    def bind_parameter_count(self):
        return capi.sqlite3_bind_parameter_count(self.p)

    def bind_int64(self, i, val):
        return capi.sqlite3_bind_int64(self.p, i, val)

    def bind_double(self, i, val):
        return capi.sqlite3_bind_double(self.p, i, val)

    def bind_null(self, i):
        return capi.sqlite3_bind_null(self.p, i)

    @jit.dont_look_inside
    def bind_str(self, i, s):
        with rffi.scoped_str2charp(s) as charp:
            return rffi.cast(
                lltype.Signed,
                capi.sqlite3_bind_text(
                    self.p, i, charp, len(s),
                    rffi.cast(rffi.VOIDP, CConfig.SQLITE_TRANSIENT)))

    def reset_query(self):
        capi.sqlite3_reset(self.p)

    def close(self):
        pass


class SQPyteDB(SQLite3DB):
    def execute(self, sql, use_flag_cache=True):
        return SQPyteQuery(self, sql, use_flag_cache)


class SQPyteQuery(SQLite3Query):

    _immutable_fields_ = ['internalPc', 'db', 'p', '_mem_as_python_list[*]',
                          '_var_as_python_list[*]',
                          '_llmem_as_python_list[*]', 'intp', 'longp',
                          'unpackedrecordp', '_hlops[*]', 'mem_cache']

    def __init__(self, hldb, query, use_flag_cache=True):
        self.hldb = hldb
        self.db = hldb.db
        self.internalPc = lltype.malloc(rffi.LONGP.TO, 1, flavor='raw')
        self.unpackedrecordp = lltype.malloc(capi.UNPACKEDRECORD, flavor='raw')
        self.intp = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')
        self.longp = lltype.malloc(rffi.LONGP.TO, 1, flavor='raw')
        self.prepare(query, use_flag_cache)
        self.iCompare = 0
        self.result_set_index = -1

    def close(self):
        if self.internalPc:
            lltype.free(self.internalPc, flavor='raw')
            lltype.free(self.intp, flavor='raw')
            lltype.free(self.longp, flavor='raw')
            lltype.free(self.unpackedrecordp, flavor='raw')
            self.internalPc = lltype.nullptr(rffi.LONGP.TO)

    def __del__(self):
        self.close()

    def prepare(self, query, use_flag_cache):
        length = len(query)
        with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
            errorcode = capi.sqlite3_prepare_v2(rffi.cast(rffi.VOIDP, self.db), query, length, result, unused_buffer)
            if not errorcode == 0:
                raise SqliteException(errorcode, rffi.charp2str(capi.sqlite3_errmsg(self.db)))
            self.p = rffi.cast(capi.VDBEP, result[0])
        self._init_python_data(use_flag_cache)

    def _init_python_data(self, use_flag_cache):
        from sqpyte.mem import Mem
        nMem = rffi.getintfield(self.p, 'nMem')
        self._llmem_as_python_list = [self.p.aMem[i] for i in range(nMem + 1)]
        self._mem_as_python_list = [Mem(self, self.p.aMem[i], i)
                for i in range(nMem + 1)]
        nVar = rffi.getintfield(self.p, 'nVar')
        self._var_as_python_list = [Mem(self, self.p.aVar[i], i + nMem + 1)
                for i in range(nVar)]
        self._hlops = [Op(self, self.p.aOp[i], i) for i in range(self.p.nOp)]
        self.init_mem_cache(use_flag_cache)
        self.use_translated = opcode.OpcodeStatus(use_flag_cache)

    def init_mem_cache(self, use_flag_cache):
        from sqpyte.mem import CacheHolder
        self.mem_cache = CacheHolder(len(self._mem_as_python_list) +
                len(self._var_as_python_list), use_flag_cache)

    def check_cache_consistency(self):
        if objectmodel.we_are_translated():
            return
        for mem in self._mem_as_python_list:
            mem.check_cache_consistency()

    def columnMem(self, i):
        """
        Check to see if column iCol of the given statement is valid.  If
        it is, return a pointer to the Mem for the value of that column.
        If iCol is not valid, return a pointer to a Mem which has a value
        of NULL.
        """
        # mutexes are commented out, we assume no other thread is allowed to
        # use sqpyte (which rpython doesn't support atm anyway)
        # sqlite3_mutex_enter(pVm->db->mutex);
        return self.mem_with_index(jit.promote(self.result_set_index) + i)

    def get_isPrepareV2(self):
        return True # because prepare always uses v2

    # _______________________________________________________________

    def vdbeUnbind(self, i):
        """
        ** Unbind the value bound to variable i in virtual machine p. This is the
        ** the same as binding a NULL value to the column. If the "i" parameter is
        ** out of range, then SQLITE_RANGE is returned. Othewise SQLITE_OK.
        **
        ** A successful evaluation of this routine acquires the mutex on p.
        ** the mutex is released if any kind of error occurs.
        **
        ** The error code stored in database p->db is overwritten with the return
        ** value in any case.
        """
        p = self.p
        if rffi.getintfield(p, 'magic') != CConfig.VDBE_MAGIC_RUN or rffi.getintfield(p, 'pc') >= 0:
            raise SQPyteException("bind on a busy prepared statement") #: [%s]", p.zSql
        i -= 1
        pVar = self.var_with_index(i)
        pVar.sqlite3VdbeMemRelease()
        pVar.set_flags(CConfig.MEM_Null)
        # sqlite3Error(p->db, SQLITE_OK); # XXX

        # If the bit corresponding to this variable in Vdbe.expmask is set, then
        # binding a new value to this variable invalidates the current query plan.

        # IMPLEMENTATION-OF: R-48440-37595 If the specific value bound to host
        # parameter in the WHERE clause might influence the choice of query plan
        # for a statement, then the statement will be automatically recompiled,
        # as if there had been a schema change, on the first sqlite3_step() call
        # following any change to the bindings of that parameter.

        if self.get_isPrepareV2() and i < 32:
            expmask = rffi.getintfield(p, 'expmask')
            if expmask & (1 << i) or expmask == 0xffffffff:
                raise SQPyteException("XXX expiry not supported :-(")
                #p.expired = 1

    # _______________________________________________________________
    # externally useful API

    def data_count(self):
        if rffi.getintfield(self.p, 'pResultSet') == 0:
            return 0
        return self._nres_column()

    @jit.elidable
    def _nres_column(self):
        return rffi.getintfield(self.p, 'nResColumn')

    def column_type(self, iCol):
        mem = self.columnMem(iCol)
        return mem.sqlite3_value_type()

    def column_text(self, iCol):
        mem = self.columnMem(iCol)
        return mem.sqlite3_value_text()

    def column_bytes(self, iCol):
        mem = self.columnMem(iCol)
        return mem.sqlite3_value_bytes()

    def column_int64(self, iCol):
        mem = self.columnMem(iCol)
        return mem.sqlite3_value_int64()

    def column_double(self, iCol):
        mem = self.columnMem(iCol)
        return mem.sqlite3_value_double()

    @jit.elidable
    def bind_parameter_count(self):
        return rffi.getintfield(self.p, 'nVar')

    def bind_int64(self, i, val):
        self.vdbeUnbind(i)
        self.var_with_index(i - 1).sqlite3VdbeMemSetInt64(val)
        return CConfig.SQLITE_OK

    def bind_double(self, i, val):
        self.vdbeUnbind(i)
        self.var_with_index(i - 1).sqlite3VdbeMemSetDouble(val)
        return CConfig.SQLITE_OK

    def bind_null(self, i):
        self.vdbeUnbind(i)
        return CConfig.SQLITE_OK

    @jit.dont_look_inside
    def bind_str(self, i, s):
        self.invalidate_caches_outside()
        with rffi.scoped_str2charp(s) as charp:
            return rffi.cast(
                lltype.Signed,
                capi.sqlite3_bind_text(
                    self.p, i, charp, len(s),
                    rffi.cast(rffi.VOIDP, CConfig.SQLITE_TRANSIENT)))

    # _______________________________________________________________
    # cache invalidation
    def invalidate_caches(self):
        self.mem_cache.invalidate_all()

    def invalidate_caches_outside(self):
        self.mem_cache.invalidate_all_outside()

    def p1_mutation(self, op):
        i = op.p_Signed(1)
        self.mem_cache.invalidate(i)

    def p2_mutation(self, op):
        i = op.p_Signed(2)
        self.mem_cache.invalidate(i)

    def p3_mutation(self, op):
        i = op.p_Signed(3)
        self.mem_cache.invalidate(i)

    @jit.unroll_safe
    def p1_p2_mutation(self, op):
        for i in range(op.p_Signed(1), op.p_Signed(1) + op.p_Signed(2)):
            self.mem_cache.invalidate(i)

    @jit.unroll_safe
    def p2_p3_mutation(self, op):
        self.mem_cache.invalidate(op.p_Signed(2))
        if op.p_Signed(3) > op.p_Signed(2):
            for i in range(op.p_Signed(2) + 1, op.p_Signed(3) + 1):
                self.mem_cache.invalidate(i)

    @jit.unroll_safe
    def p3_p4_mutation(self, op):
        for i in range(op.p_Signed(3), op.p_Signed(3) + op.p4_i()):
            self.mem_cache.invalidate(i)

    @jit.unroll_safe
    def p3_p4_or_p3_mutation(self, op):
        length = op.p4_i()
        if not length:
            length = 1
        for i in range(op.p_Signed(3), op.p_Signed(3) + length):
            self.mem_cache.invalidate(i)

    @jit.unroll_safe
    def p2_p5_mutation(self, op):
        for i in range(op.p_Signed(2), op.p_Signed(2) + op.p_Signed(5)):
            self.mem_cache.invalidate(i)

    # _______________________________________________________________

    def is_op_cache_safe(self, opcode):
        return _cache_safe_opcodes[opcode]

    def reset_query(self):
        self.invalidate_caches_outside()
        capi.sqlite3_reset(self.p)

    # _______________________________________________________________
    # C-based opcodes

    def python_OP_CreateTable_CreateIndex(self, rc, op):
        return capi.impl_OP_CreateIndex_CreateTable(self.p, self.db, rc, op.pOp)

    def python_OP_Clear(self, rc, op):
        return capi.impl_OP_Clear(self.p, self.db, rc, op.pOp)

    @cache_safe(mutates=["p1", "p2", "p3"])
    def python_OP_Concat(self, pc, rc, op):
        return capi.impl_OP_Concat(self.p, self.db, pc, rc, op.pOp)

    @cache_safe(opcodes=[CConfig.OP_Insert, CConfig.OP_InsertInt])
    def python_OP_Insert_InsertInt(self, op):
        return capi.impl_OP_Insert_InsertInt(self.p, self.db, op.pOp)

    @cache_safe()
    def python_OP_NewRowid(self, pc, rc, op):
        return capi.impl_OP_NewRowid(self.p, self.db, pc, rc, op.pOp)

    def python_OP_ParseSchema(self, pc, rc, op):
        return capi.impl_OP_ParseSchema(self.p, self.db, pc, rc, op.pOp)

    def python_OP_ReadCookie(self, op):
        capi.impl_OP_ReadCookie(self.p, self.db, op.pOp)

    def python_OP_RowSetAdd(self, pc, rc, op):
        return capi.impl_OP_RowSetAdd(self.p, self.db, pc, rc, op.pOp)

    def python_OP_RowSetRead(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        retRc = capi.impl_OP_RowSetRead(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, retRc

    @cache_safe()
    def python_OP_Delete(self, pc, op):
        return capi.impl_OP_Delete(self.p, self.db, pc, op.pOp)

    def python_OP_DropTable(self, op):
        return capi.impl_OP_DropTable(self.db, op.pOp)

    @cache_safe(opcodes=[CConfig.OP_RowKey, CConfig.OP_RowData],
                mutates="p2")
    def python_OP_RowKey_RowData(self, pc, rc, op):
        rc = capi.impl_OP_RowKey_RowData(self.p, self.db, pc, rc, op.pOp)
        # this always returns a blob
        return rc

    def python_OP_Blob(self, op):
        capi.impl_OP_Blob(self.p, self.db, op.pOp)

    @cache_safe()
    def python_OP_Close(self, op):
        capi.impl_OP_Close(self.p, op.pOp)

    @cache_safe() # XXX not 100% sure
    def python_OP_Halt(self, pc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_Halt(self.p, self.db, self.internalPc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    @cache_safe(
        opcodes=[CConfig.OP_NoConflict, CConfig.OP_NotFound, CConfig.OP_Found],
        mutates="p3@p4 or p3")
    def python_OP_NoConflict_NotFound_Found(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_NoConflict_NotFound_Found(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    @cache_safe()
    def python_OP_NullRow(self, op):
        capi.impl_OP_NullRow(self.p, op.pOp)

    def python_OP_OpenAutoindex_OpenEphemeral(self, pc, op):
        return capi.impl_OP_OpenAutoindex_OpenEphemeral(self.p, self.db, pc, op.pOp)

    def python_OP_OpenPseudo(self, pc, rc, op):
        return capi.impl_OP_OpenPseudo(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_Rewind(self, pc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_Rewind(self.p, self.db, self.internalPc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    @cache_safe(mutates="p2")
    def python_OP_Rowid(self, pc, rc, op):
        return capi.impl_OP_Rowid(self.p, self.db, pc, rc, op.pOp)

    def python_OP_RowSetTest(self, pc, rc, op):
        self.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_RowSetTest(self.p, self.db, self.internalPc, rc, op.pOp)
        retPc = self.internalPc[0]
        return retPc, rc

    def python_OP_SetCookie(self, op):
        return capi.impl_OP_SetCookie(self.p, self.db, op.pOp)

    @cache_safe(mutates="p2")
    def python_OP_String(self, op):
        capi.impl_OP_String(self.p, self.db, op.pOp)

    @cache_safe(mutates="p2")
    def python_OP_String8(self, pc, rc, op):
        return capi.impl_OP_String8(self.p, self.db, pc, rc, op.pOp)

    @cache_safe(opcodes=[CConfig.OP_SorterInsert, CConfig.OP_IdxInsert],
                mutates=["p2"])
    def python_OP_SorterInsert_IdxInsert(self, op):
        return capi.impl_OP_SorterInsert_IdxInsert(self.p, self.db, op.pOp)

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

    def python_OP_SorterOpen(self, pc, op):
        return capi.impl_OP_SorterOpen(self.p, self.db, pc, op.pOp)

    @cache_safe() # XXX not 100% sure
    def python_OP_TableLock(self, rc, op):
        return capi.impl_OP_TableLock(self.p, self.db, rc, op.pOp)

    @cache_safe() # XXX not 100% sure
    def python_OP_Transaction(self, pc, op):
        return capi.impl_OP_Transaction(self.p, self.db, pc, op.pOp)

    @cache_safe()
    def python_OP_Column(self, pc, op):
        # not really translated
        return translated.OP_Column(self, pc, op)

    # _______________________________________________________________
    # both implementations exist

    @cache_safe() # invalidation done in the slow path
    def python_OP_AggFinal(self, pc, rc, op):
        if self.use_translated.AggFinal:
            return translated.OP_AggFinal(self, rc, pc, op)
        else:
            return capi.impl_OP_AggFinal(self.p, self.db, pc, rc, op.pOp)

    @cache_safe() # invalidation done in the slow path
    def python_OP_AggStep(self, rc, pc, op):
        if self.use_translated.AggStep:
            return translated.OP_AggStep(self, rc, pc, op)
        else:
            return capi.impl_OP_AggStep(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_Function(self, pc, rc, op):
        if self.use_translated.Function:
            return translated.OP_Function(self, pc, rc, op)
        else:
            result = capi.impl_OP_Function(self.p, self.db, pc, rc, op.pOp)
            return op._decode_combined_flags_rc_for_p3(result)

    @cache_safe()
    def python_OP_Goto(self, pc, rc, op):
        if self.use_translated.Goto:
            return translated.OP_Goto(self, pc, rc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            retRc = capi.impl_OP_Goto(self.p, self.db, self.internalPc, rc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, retRc

    @cache_safe(opcodes=[CConfig.OP_OpenRead, CConfig.OP_OpenWrite])
    def python_OP_OpenRead_OpenWrite(self, pc, op):
        if self.use_translated.OpenRead_OpenWrite:
            return translated.OP_OpenRead_OpenWrite(self, self.db, pc, op)
        else:
            if op.p_Signed(5):
                self.mem_cache.invalidate(op.p_Signed(2))
            return capi.impl_OP_OpenRead_OpenWrite(self.p, self.db, pc, op.pOp)

    @cache_safe()
    def python_OP_ResultRow(self, pc, op):
        if self.use_translated.ResultRow:
            return translated.OP_ResultRow(self, pc, op)
        else:
            return capi.impl_OP_ResultRow(self.p, self.db, pc, op.pOp)

    @cache_safe()
    def python_OP_Next(self, pc, op):
        if self.use_translated.Next:
            return translated.OP_Next(self, pc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_Next(self.p, self.db, self.internalPc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc


    @cache_safe(
    opcodes=[CConfig.OP_Eq, CConfig.OP_Ne, CConfig.OP_Lt, CConfig.OP_Le,
             CConfig.OP_Gt, CConfig.OP_Ge])
    def python_OP_Ne_Eq_Gt_Le_Lt_Ge(self, pc, rc, op):
        if self.use_translated.Ne_Eq_Gt_Le_Lt_Ge:
            return translated.OP_Ne_Eq_Gt_Le_Lt_Ge(self, pc, rc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_Ne_Eq_Gt_Le_Lt_Ge(self.p, self.db, self.internalPc, rc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc


    @cache_safe()
    def python_OP_Integer(self, op):
        if self.use_translated.Integer:
            translated.OP_Integer(self, op)
            #capi.impl_OP_Integer(self.p, op.pOp)

    @cache_safe()
    def python_OP_Null(self, op):
        if self.use_translated.Null:
            #capi.impl_OP_Null(self.p, op.pOp)
            translated.OP_Null(self, op)

    @cache_safe()
    def python_OP_MustBeInt(self, pc, rc, op):
        if self.use_translated.MustBeInt:
            return translated.OP_MustBeInt(self, pc, rc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_MustBeInt(self.p, self.db, self.internalPc, rc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc

    @cache_safe()
    def python_OP_Copy(self, pc, rc, op):
        if self.use_translated.Copy:
            return translated.OP_Copy(self, pc, rc, op)
        else:
            return capi.impl_OP_Copy(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_NotExists(self, pc, op):
        if self.use_translated.NotExists:
            return translated.OP_NotExists(self, pc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_NotExists(self.p, self.db, self.internalPc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc

    @cache_safe()
    def python_OP_Real(self, op):
        if self.use_translated.Real:
            # aMem = self.p.aMem
            # pOut = aMem[pOp.p2]
            # pOut.flags = rffi.cast(rffi.USHORT, CConfig.MEM_Real)
            # assert not math.isnan(pOp.p4.pReal)
            # pOut.r = pOp.p4.pReal
            return translated.OP_Real(self, op)
        else:
            return capi.impl_OP_Real(self.p, op.pOp)

    @cache_safe()
    def python_OP_RealAffinity(self, op):
        if self.use_translated.RealAffinity:
            translated.OP_RealAffinity(self, op)
        else:
            capi.impl_OP_RealAffinity(self.p, op.pOp)

    @cache_safe(
    opcodes=[CConfig.OP_Add, CConfig.OP_Subtract, CConfig.OP_Multiply,
             CConfig.OP_Divide, CConfig.OP_Remainder])
    def python_OP_Add_Subtract_Multiply_Divide_Remainder(self, op):
        if self.use_translated.Add_Subtract_Multiply_Divide_Remainder:
            translated.OP_Add_Subtract_Multiply_Divide_Remainder(self, op)
        else:
            capi.impl_OP_Add_Subtract_Multiply_Divide_Remainder(self.p, op.pOp)

    @cache_safe(
        opcodes=[CConfig.OP_If, CConfig.OP_IfNot])
    def python_OP_If_IfNot(self, pc, op):
        if self.use_translated.If_IfNot:
            return translated.OP_If_IfNot(self, pc, op)
        else:
            return capi.impl_OP_If_IfNot(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_IsNull(self, pc, op):
        if self.use_translated.IsNull:
            return translated.OP_IsNull(self, pc, op)
        else:
            return capi.impl_OP_IsNull(self.p, pc, op.pOp)

    @cache_safe(
        opcodes=[CConfig.OP_SeekLT,
                 CConfig.OP_SeekLE,
                 CConfig.OP_SeekGE,
             CConfig.OP_SeekGT],
        mutates="p3@p4")
    def python_OP_SeekLT_SeekLE_SeekGE_SeekGT(self, pc, rc, op):
        if self.use_translated.SeekLT_SeekLE_SeekGE_SeekGT:
            return translated.OP_SeekLT_SeekLE_SeekGE_SeekGT(self, pc, rc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_SeekLT_SeekLE_SeekGE_SeekGT(self.p, self.db, self.internalPc, rc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc

    @cache_safe()
    def python_OP_Move(self, op):
        if self.use_translated.Move:
            translated.OP_Move(self, op)
        else:
            capi.impl_OP_Move(self.p, op.pOp)

    @cache_safe()
    def python_OP_IfZero(self, pc, op):
        if self.use_translated.IfZero:
            return translated.OP_IfZero(self, pc, op)
        else:
            return capi.impl_OP_IfZero(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_IdxRowid(self, pc, rc, op):
        if self.use_translated.IdxRowid:
            return translated.OP_IdxRowid(self, pc, rc, op)
        else:
            return capi.impl_OP_IdxRowid(self.p, self.db, pc, rc, op.pOp)

    @cache_safe(
        opcodes=[CConfig.OP_IdxLE, CConfig.OP_IdxGT, CConfig.OP_IdxLT, CConfig.OP_IdxGE],
        mutates="p3@p4")
    def python_OP_IdxLE_IdxGT_IdxLT_IdxGE(self, pc, op):
        if self.use_translated.IdxLE_IdxGT_IdxLT_IdxGE:
            return translated.OP_IdxLE_IdxGT_IdxLT_IdxGE(self, pc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_IdxLE_IdxGT_IdxLT_IdxGE(self.p, self.db, self.internalPc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc

    @cache_safe()
    def python_OP_Seek(self, op):
        if self.use_translated.Seek:
            translated.OP_Seek(self, op)
        else:
            capi.impl_OP_Seek(self.p, op.pOp)

    @cache_safe()
    def python_OP_Once(self, pc, op):
        if self.use_translated.Once:
            return translated.OP_Once(self, pc, op)
        else:
            return capi.impl_OP_Once(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_SCopy(self, op):
        if self.use_translated.SCopy:
            translated.OP_SCopy(self, op)
        else:
            capi.impl_OP_SCopy(self.p, op.pOp)

    @cache_safe()
    def python_OP_Affinity(self, op):
        if self.use_translated.Affinity:
            translated.OP_Affinity(self, op)
        else:
            capi.impl_OP_Affinity(self.p, self.db, op.pOp)

    @cache_safe()
    def python_OP_MakeRecord(self, pc, rc, op):
        if self.use_translated.MakeRecord:
            return translated.OP_MakeRecord(self, pc, rc, op)
        else:
            return capi.impl_OP_MakeRecord(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_Gosub(self, pc, op):
        if self.use_translated.Gosub:
            return translated.OP_Gosub(self, pc, op)
        else:
            return capi.impl_OP_Gosub(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_Return(self, pc, op):
        if self.use_translated.Return:
            return translated.OP_Return(self, op)
        else:
            return capi.impl_OP_Return(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_NextIfOpen(self, pc, rc, op):
        if self.use_translated.NextIfOpen:
            return translated.OP_NextIfOpen(self, pc, rc, op)
        else:
            self.internalPc[0] = rffi.cast(rffi.LONG, pc)
            rc = capi.impl_OP_NextIfOpen(self.p, self.db, self.internalPc, rc, op.pOp)
            retPc = self.internalPc[0]
            return retPc, rc


    @cache_safe(mutates="p2")
    def python_OP_Sequence(self, op):
        if self.use_translated.Sequence:
            translated.OP_Sequence(self, op)
        else:
            capi.impl_OP_Sequence(self.p, op.pOp)

    @cache_safe()
    def python_OP_Compare(self, op):
        # Compare and Jump must be implemented on the same side
        if self.use_translated.Compare:
            assert self.use_translated.Jump
            translated.OP_Compare(self, op)
        else:
            assert not self.use_translated.Jump
            capi.impl_OP_Compare(self.p, op.pOp)

    @cache_safe()
    def python_OP_Jump(self, op):
        # Compare and Jump must be implemented on the same side
        if self.use_translated.Jump:
            assert self.use_translated.Compare
            return translated.OP_Jump(self, op)
        else:
            assert not self.use_translated.Compare
            return capi.impl_OP_Jump(op.pOp)

    @cache_safe()
    def python_OP_Variable(self, pc, rc, op):
        if self.use_translated.Variable:
            return translated.OP_Variable(self, pc, rc, op)
        else:
            self.invalidate_caches() # XXX annoying
            return capi.impl_OP_Variable(self.p, self.db, pc, rc, op.pOp)

    @cache_safe()
    def python_OP_CollSeq(self, op):
        if self.use_translated.CollSeq:
            translated.OP_CollSeq(self, op)
        else:
            capi.impl_OP_CollSeq(self.p, op.pOp)

    @cache_safe()
    def python_OP_NotNull(self, pc, op):
        if self.use_translated.NotNull:
            return translated.OP_NotNull(self, pc, op)
        else:
            return capi.impl_OP_NotNull(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_InitCoroutine(self, pc, op):
        if self.use_translated.InitCoroutine:
            return translated.OP_InitCoroutine(self, pc, op)
        else:
            return capi.impl_OP_InitCoroutine(self.p, pc, op.pOp)

    @cache_safe(mutates="p1")
    def python_OP_Yield(self, pc, op):
        if self.use_translated.Yield:
            return translated.OP_Yield(self, pc, op)
        else:
            return capi.impl_OP_Yield(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_EndCoroutine(self, op):
        if self.use_translated.EndCoroutine:
            return translated.OP_EndCoroutine(self, op)
        else:
            return capi.impl_OP_EndCoroutine(self.p, op.pOp)

    @cache_safe()
    def python_OP_IfPos(self, pc, op):
        if self.use_translated.IfPos:
            return translated.OP_IfPos(self, pc, op)
        else:
            return capi.impl_OP_IfPos(self.p, pc, op.pOp)

    @cache_safe()
    def python_OP_Cast(self, rc, op):
        if self.use_translated.IfPos:
            return translated.OP_Cast(self, rc, op)
        else:
            return capi.impl_OP_Cast(self.p, self.db, rc, op.pOp)


    # _______________________________________________________________
    # only translated

    @cache_safe()
    def python_OP_Init(self, pc, op):
        # XXX
        return translated.OP_Init(self, pc, op)

    def python_OP_Noop_Explain(self, op):
        # XXX
        translated.OP_Noop_Explain(op)

    # _______________________________________________________________

    def debug_print(self, pc, op):
        if objectmodel.we_are_translated():
            return
        if '-debug-print' not in sys.argv:
            return
        print '%s %s %s %s %s %s' % (
                pc, op.get_opcode_str(), op.p_Signed(1),
                op.p_Signed(2), op.p_Signed(3), op.p_Signed(5))

    @jit.elidable
    def get_opcode_str(self, opcode):
        return capi.opnames_dict.get(opcode, '')


    @jit.elidable
    def get_aOp(self):
        return self.p.aOp

    @jit.elidable
    def enc(self):
        return self.db.enc

    def mem_with_index(self, i):
        try:
            return self._mem_as_python_list[i]
        except IndexError:
            raise SQPyteException("unknown mem location")

    def var_with_index(self, i):
        try:
            return self._var_as_python_list[i]
        except IndexError:
            raise SQPyteException("unknown var location")

    def gotoNoMem(self, pc):
        return rffi.cast(lltype.Signed, capi.gotoNoMem(self.p, self.db, rffi.cast(rffi.INT, pc)))

    def gotoTooBig(self, pc):
        return rffi.cast(lltype.Signed, capi.gotoTooBig(self.p, self.db, rffi.cast(rffi.INT, pc)))

    def mainloop(self):
        rc = CConfig.SQLITE_OK
        pc = jit.promote(rffi.cast(lltype.Signed, self.p.pc))
        if pc < 0:
            pc = 0 # XXX maybe more to do, see vdbeapi.c:418
        cache_state = self.mem_cache.reenter()

        self.p.pResultSet = lltype.nullptr(lltype.typeOf(self.p.pResultSet).TO)
        self.result_set_index = -1
        self.use_translated.freeze()

        while True:
            jitdriver.jit_merge_point(pc=pc, self_=self, rc=rc, cache_state=cache_state)
            self.mem_cache.reveal(cache_state)
            if rc != CConfig.SQLITE_OK:
                break
            op = self._hlops[pc]
            opcode = op.get_opcode()
            oldpc = pc
            self.debug_print(pc, op)

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
                jit.promote(rc)
                if rc == CConfig.SQLITE_BUSY:
                    print 'ERROR: in OP_Transaction SQLITE_BUSY'
                    break
            elif opcode == CConfig.OP_TableLock:
                rc = self.python_OP_TableLock(rc, op)
            elif opcode == CConfig.OP_Goto:
                pc, rc = self.python_OP_Goto(pc, rc, op)
            elif opcode == CConfig.OP_Column:
                rc = self.python_OP_Column(pc, op)
            elif opcode == CConfig.OP_ResultRow:
                rc = self.python_OP_ResultRow(pc, op)
                if rc == CConfig.SQLITE_ROW:
                    break
            elif opcode == CConfig.OP_Next:
                pc, rc = self.python_OP_Next(pc, op)
            elif opcode == CConfig.OP_Close:
                self.python_OP_Close(op)
            elif opcode == CConfig.OP_Halt:
                pc, rc = self.python_OP_Halt(pc, op)
                break
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
            elif opcode == CConfig.OP_RowKey or opcode == CConfig.OP_RowData:
                rc = self.python_OP_RowKey_RowData(pc, rc, op)
            elif opcode == CConfig.OP_Blob:
                self.python_OP_Blob(op)
            elif opcode == CConfig.OP_Cast:
                rc = self.python_OP_Cast(rc, op)
            elif opcode == CConfig.OP_Concat:
                rc = self.python_OP_Concat(pc, rc, op)
            elif opcode == CConfig.OP_Variable:
                rc = self.python_OP_Variable(pc, rc, op)
            elif opcode == CConfig.OP_CreateTable or opcode == CConfig.OP_CreateIndex:
                rc = self.python_OP_CreateTable_CreateIndex(rc, op)
            elif opcode == CConfig.OP_Clear:
                rc = self.python_OP_Clear(rc, op)
            else:
                raise SQPyteException("SQPyteException: Unimplemented bytecode %s." % opcode)
            pc = jit.promote(rffi.cast(lltype.Signed, pc))
            pc += 1
            if not self.is_op_cache_safe(opcode):
                self.invalidate_caches()
            self.check_cache_consistency()
            cache_state = self.mem_cache.cache_state()
            if pc <= oldpc:
                self.mem_cache.hide()
                jitdriver.can_enter_jit(pc=pc, self_=self, rc=rc, cache_state=cache_state)
        jit.promote(rc)
        self.mem_cache.prepare_return()
        return rc


class Op(object):
    _immutable_fields_ = ['hlquery', 'pOp', 'pc']

    def __init__(self, hlquery, pOp, pc):
        self.hlquery = hlquery
        self.pOp = pOp
        self.pc = pc

    @jit.elidable
    def get_opcode(self):
        return rffi.cast(lltype.Unsigned, self.pOp.opcode)

    def get_opcode_str(self):
        return capi.opnames_dict.get(self.get_opcode(), '')

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
        if self.pOp.p4.z:
            return rffi.charp2str(self.pOp.p4.z)
        return None

    @jit.elidable
    def p4_Real(self):
        # XXX I'm only 99% certain that this is safe
        val = self.pOp.p4.pReal[0]
        assert not math.isnan(val)
        return val

    @jit.elidable
    def p4_pFunc(self):
        return self.hlquery.hldb.funcregistry.get_func(self.pOp.p4.pFunc)

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

    def mem_and_flags_of_p(self, i):
        mem = self.mem_of_p(i)
        flags = mem.get_flags()
        return mem, flags

    @jit.elidable
    def opflags(self):
        return rffi.cast(lltype.Unsigned, self.pOp.opflags)

    def _decode_combined_flags_rc_for_p3(self, result):
        from rpython.rlib import rarithmetic
        # this is just a trick to promote two values at once, rc and the new flags
        # of p3
        # it also invalidates p3 before doing that
        jit.promote(result)
        rc = result & 0xffff
        if rc:
            return rc
        flags = rarithmetic.r_uint(result >> 16)
        pOut = self.mem_of_p(3)
        pOut.invalidate_cache()
        pOut.assure_flags(flags)
        return rc


def main_work(query):
    db = SQPyteDB(testdb)
    query = db.execute(query)
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
