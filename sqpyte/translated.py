from rpython.rlib import jit, rarithmetic
from rpython.rlib.objectmodel import specialize
from rpython.rtyper.lltypesystem import rffi
from sqpyte.capi import CConfig
from rpython.rtyper.lltypesystem import lltype
from sqpyte import capi
import sys
import math

assert sys.maxint == 2 ** 63 - 1 # sqpyte only works on 64 bit machines

LARGEST_INT64 = 0xffffffff | (0x7fffffff << 32)
SMALLEST_INT64 = -1 - LARGEST_INT64
TWOPOWER32 = 1 << 32
TWOPOWER31 = 1 << 31


def allocateCursor(vdbe_struct, p1, nField, iDb, isBtreeCursor):
    return capi.sqlite3_allocateCursor(vdbe_struct, p1, nField, iDb, isBtreeCursor) 

def sqlite3BtreeCursor(p, iTable, wrFlag, pKeyInfo, pCur):
    return capi.sqlite3_sqlite3BtreeCursor(p, iTable, wrFlag, pKeyInfo, pCur)

def sqlite3BtreeCursorHints(btCursor, mask):
    return capi.sqlite3_sqlite3BtreeCursorHints(btCursor, mask)

def sqlite3VdbeSorterRewind(db, pC, res):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as res:
        errorcode = capi.sqlite3_sqlite3VdbeSorterRewind(db, pC, res)
        return rffi.cast(rffi.INTP, res[0])



# Opcode: Init * P2 * P4 *
# Synopsis:  Start at P2
#
# Programs contain a single instance of this opcode as the very first
# opcode.
#
# If tracing is enabled (by the sqlite3_trace()) interface, then
# the UTF-8 string contained in P4 is emitted on the trace callback.
# Or if P4 is blank, use the string returned by sqlite3_sql().
#
# If P2 is not zero, jump to instruction P2.

def python_OP_Init_translated(hlquery, pc, op):
    p2 = op.p_Signed(2)
    if p2:
        pc = p2 - 1

    # #ifndef SQLITE_OMIT_TRACE
    #   if( db->xTrace
    #    && !p->doingRerun
    #    && (zTrace = (pOp->p4.z ? pOp->p4.z : p->zSql))!=0
    #   ){
    #     z = sqlite3VdbeExpandSql(p, zTrace);
    #     db->xTrace(db->pTraceArg, z);
    #     sqlite3DbFree(db, z);
    #   }
    # #ifdef SQLITE_USE_FCNTL_TRACE
    #   zTrace = (pOp->p4.z ? pOp->p4.z : p->zSql);
    #   if( zTrace ){
    #     int i;
    #     for(i=0; i<db->nDb; i++){
    #       if( MASKBIT(i) & p->btreeMask)==0 ) continue;
    #       sqlite3_file_control(db, db->aDb[i].zName, SQLITE_FCNTL_TRACE, zTrace);
    #     }
    #   }
    # #endif /* SQLITE_USE_FCNTL_TRACE */
    # #ifdef SQLITE_DEBUG
    #   if( (db->flags & SQLITE_SqlTrace)!=0
    #    && (zTrace = (pOp->p4.z ? pOp->p4.z : p->zSql))!=0
    #   ){
    #     sqlite3DebugPrintf("SQL-trace: %s\n", zTrace);
    #   }
    # #endif /* SQLITE_DEBUG */
    # #endif /* SQLITE_OMIT_TRACE */

    return pc

def python_OP_Goto_translated(hlquery, pc, rc, op):
    db = hlquery.db
    pc = op.p2as_pc()

    # Translated goto check_for_interrupt;
    if rffi.cast(lltype.Signed, db.u1.isInterrupted) != 0:
        # goto abort_due_to_interrupt;
        print 'In python_OP_Goto_translated(): abort_due_to_interrupt.'
        rc = capi.sqlite3_gotoAbortDueToInterrupt(hlquery.p, db, pc, rc)
        return pc, rc

    # #ifndef SQLITE_OMIT_PROGRESS_CALLBACK
    #   /* Call the progress callback if it is configured and the required number
    #   ** of VDBE ops have been executed (either since this invocation of
    #   ** sqlite3VdbeExec() or since last time the progress callback was called).
    #   ** If the progress callback returns non-zero, exit the virtual machine with
    #   ** a return code SQLITE_ABORT.
    #   */
    #   if( db->xProgress!=0 && nVmStep>=nProgressLimit ){
    #     assert( db->nProgressOps!=0 );
    #     nProgressLimit = nVmStep + db->nProgressOps - (nVmStep%db->nProgressOps);
    #     if( db->xProgress(db->pProgressArg) ){
    #       rc = SQLITE_INTERRUPT;
    #       // goto vdbe_error_halt;
    #       printf("In python_OP_Goto(): vdbe_error_halt.\n");
    #       rc = gotoVdbeErrorHalt(p, db, *pc, rc);
    #       return rc;
    #     }
    #   }
    # #endif

    return pc, rc


def python_OP_OpenRead_OpenWrite_translated(hlquery, db, pc, op):
    pOp = op.pOp
    p = hlquery.p
    assert pOp.p5 & (CConfig.OPFLAG_P2ISREG | CConfig.OPFLAG_BULKCSR) == pOp.p5
    assert pOp.opcode == CConfig.OP_OpenWrite or pOp.p5 == 0
    # assert(p.bIsReader)
    assert pOp.opcode == CConfig.OP_OpenRead or p.readOnly == 0

    if p.expired:
        rc = CConfig.SQLITE_ABORT
        return

    nField = 0
    pKeyInfo = lltype.nullptr(capi.KEYINFO)
    p2 = pOp.p2;
    iDb = pOp.p3
    wrFlag = 0

    assert iDb >= 0 and iDb < db.nDb
    #   assert( (p->btreeMask & (((yDbMask)1)<<iDb))!=0 );

    pDb = db.aDb[iDb]
    pX = pDb.pBt

    assert pX

    if pOp.opcode == CConfig.OP_OpenWrite:
        wrFlag = 1
        #     assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
        if pDb.pSchema.file_format < p.minWriteFileFormat:
            p.minWriteFileFormat = pDb.pSchema.file_format
        else:
            wrFlag = 0

    if pOp.p5 & CConfig.OPFLAG_P2ISREG:
        assert p2 > 0
        assert p2 <= p.nMem - p.nCursor
        pIn2 = aMem[p2]
        #     assert( memIsValid(pIn2) );
        #     assert( (pIn2->flags & MEM_Int)!=0 );
        pIn2.sqlite3VdbeMemIntegerify()
        p2 = rffi.cast(rffi.INT, pIn2.get_u_i())
        #     /* The p2 value always comes from a prior OP_CreateTable opcode and
        #     ** that opcode will always set the p2 value to 2 or more or else fail.
        #     ** If there were a failure, the prepared statement would have halted
        #     ** before reaching this instruction. */
        #     if( NEVER(p2<2) ) {
        #       rc = SQLITE_CORRUPT_BKPT;
        #       goto abort_due_to_error;
        #     }
        if pOp.p4type == CConfig.P4_KEYINFO:
            pKeyInfo = pOp.p4.pKeyInfo
            #     assert( pKeyInfo->enc==ENC(db) );
            assert(pKeyInfo.db == db)
            nField = intmask(pKeyInfo.nField) + intmask(pKeyInfo.nXField)
        elif pOp.p4type == CConfig.P4_INT32:
            nField = pOp.p4.i

        assert pOp.p1 >= 0
        assert nField >= 0
        #   testcase( nField==0 );  /* Table with INTEGER PRIMARY KEY and nothing else */
        pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
        #   if( pCur==0 ) goto no_mem;
        pCur.nullRow = 1
        pCur.isOrdered = bool(1)
        rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
        pCur.pKeyInfo = pKeyInfo
        assert CConfig.OPFLAG_BULKCSR == CConfig.BTREE_BULKLOAD
        sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))

        #   /* Since it performs no memory allocation or IO, the only value that
        #   ** sqlite3BtreeCursor() may return is SQLITE_OK. */
        assert rc == CConfig.SQLITE_OK
        #   /* Set the VdbeCursor.isTable variable. Previous versions of
        #   ** SQLite used to check if the root-page flags were sane at this point
        #   ** and report database corruption if they were not, but this check has
        #   ** since moved into the btree layer.  */
        pCur.isTable = pOp.p4type != CConfig.P4_KEYINFO


def sqlite3BtreeNext(hlquery, pCur, pRes):
    internalRes = hlquery.intp
    internalRes[0] = rffi.cast(rffi.INT, pRes)
    rc = capi.sqlite3_sqlite3BtreeNext(pCur, internalRes)
    retRes = internalRes[0]
    return rc, retRes

def sqlite3BtreePrevious(hlquery, pCur, pRes):
    internalRes = hlquery.intp
    internalRes[0] = rffi.cast(rffi.INT, pRes)
    rc = capi.sqlite3_sqlite3BtreePrevious(pCur, internalRes)
    retRes = internalRes[0]
    return rc, retRes

@jit.dont_look_inside
def _increase_counter_hidden_from_jit(p, p5):
    # the JIT can't deal with FixedSizeArrays
    aCounterValue = rffi.cast(lltype.Unsigned, p.aCounter[p5])
    aCounterValue += 1
    p.aCounter[p5] = rffi.cast(rffi.UINT, aCounterValue)

def python_OP_Next_translated(hlquery, pc, op):
    p = hlquery.p
    db = hlquery.db
    pcRet = pc
    p1 = op.p_Signed(1)
    p5 = op.p_Unsigned(5)
    assert p1 >= 0 and p1 < rffi.cast(lltype.Signed, p.nCursor)
    assert p5 < len(p.aCounter)
    pC = p.apCsr[p1]
    res = op.p_Signed(3)
    assert pC
    assert rffi.cast(lltype.Unsigned, pC.deferredMoveto) == 0
    assert pC.pCursor
    #assert res == 0 or (res == 1 and rffi.cast(lltype.Signed, pC.isTable) == 0)
    
    # testcase() is used for SQLite3 coverage testing and logically
    # should not be used in production.
    # See sqliteInt.h lines 269-288
    # testcase( res==1 );

    # assert( pOp->opcode!=OP_Next || pOp->p4.xAdvance==sqlite3BtreeNext );
    # assert( pOp->opcode!=OP_Prev || pOp->p4.xAdvance==sqlite3BtreePrevious );
    # assert( pOp->opcode!=OP_NextIfOpen || pOp->p4.xAdvance==sqlite3BtreeNext );
    # assert( pOp->opcode!=OP_PrevIfOpen || pOp->p4.xAdvance==sqlite3BtreePrevious);
    
    # Specifically for OP_Next, xAdvance() is always sqlite3BtreeNext()
    # as can be deduced from assertions above.
    # rc = pOp->p4.xAdvance(pC->pCursor, &res);
    rc, resRet = sqlite3BtreeNext(hlquery, pC.pCursor, res)
    jit.promote(rc)

    # next_tail:
    pC.cacheStatus = rffi.cast(rffi.UINT, CConfig.CACHE_STALE)

    # VdbeBranchTaken() is used for test suite validation only and 
    # does not appear an production builds.
    # See vdbe.c lines 110-136.
    # VdbeBranchTaken(res==0, 2)

    if rffi.cast(lltype.Signed, resRet) == 0:
        pC.nullRow = rffi.cast(rffi.UCHAR, 0)
        pcRet = op.p2as_pc()

        _increase_counter_hidden_from_jit(p, p5)

        # Should not be used in production.
        #ifdef SQLITE_TEST
            # sqlite3_search_count++;
        #endif
    else:
        pC.nullRow = rffi.cast(rffi.UCHAR, 1)

    pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)

    # Translated goto check_for_interrupt;
    if rffi.cast(lltype.Signed, db.u1.isInterrupted) != 0:
        # goto abort_due_to_interrupt;
        print 'In python_OP_Next_translated(): abort_due_to_interrupt.'
        rc = capi.sqlite3_gotoAbortDueToInterrupt(p, db, pcRet, rc)
        return pcRet, rc

    # #ifndef SQLITE_OMIT_PROGRESS_CALLBACK
    #   /* Call the progress callback if it is configured and the required number
    #   ** of VDBE ops have been executed (either since this invocation of
    #   ** sqlite3VdbeExec() or since last time the progress callback was called).
    #   ** If the progress callback returns non-zero, exit the virtual machine with
    #   ** a return code SQLITE_ABORT.
    #   */
    #   if( db->xProgress!=0 && nVmStep>=nProgressLimit ){
    #     assert( db->nProgressOps!=0 );
    #     nProgressLimit = nVmStep + db->nProgressOps - (nVmStep%db->nProgressOps);
    #     if( db->xProgress(db->pProgressArg) ){
    #       rc = SQLITE_INTERRUPT;
    #       // goto vdbe_error_halt;
    #       printf("In impl_OP_Next(): vdbe_error_halt.\n");
    #       rc = gotoVdbeErrorHalt(p, db, *pc, rc);
    #       return rc;
    #     }
    #   }
    # #endif

    return pcRet, rc

def python_OP_NextIfOpen_translated(hlquery, pc, rc, op):
    p = hlquery.p
    db = hlquery.db
    p1 = op.p_Signed(1)
    if not p.apCsr[p1]:
        return pc, rc
    else:
        return python_OP_Next_translated(hlquery, pc, op)

@specialize.argtype(1)
def _cmp_depending_on_opcode(opcode, a, b):
    if opcode == CConfig.OP_Eq:
        return a == b
    elif opcode == CConfig.OP_Ne:
        return a != b
    elif opcode == CConfig.OP_Lt:
        return a < b
    elif opcode == CConfig.OP_Le:
        return a <= b
    elif opcode == CConfig.OP_Gt:
        return a > b
    else:
        return a >= b


# Opcode: Lt P1 P2 P3 P4 P5
# Synopsis: if r[P1]<r[P3] goto P2
#
# Compare the values in register P1 and P3.  If reg(P3)<reg(P1) then
# jump to address P2.
#
# If the SQLITE_JUMPIFNULL bit of P5 is set and either reg(P1) or
# reg(P3) is NULL then take the jump.  If the SQLITE_JUMPIFNULL
# bit is clear then fall through if either operand is NULL.
#
# The SQLITE_AFF_MASK portion of P5 must be an affinity character -
# SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made
# to coerce both inputs according to this affinity before the
# comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric
# affinity is used. Note that the affinity conversions are stored
# back into the input registers P1 and P3.  So this opcode can cause
# persistent changes to registers P1 and P3.
#
# Once any conversions have taken place, and neither value is NULL,
# the values are compared. If both values are blobs then memcmp() is
# used to determine the results of the comparison.  If both values
# are text, then the appropriate collating function specified in
# P4 is  used to do the comparison.  If P4 is not specified then
# memcmp() is used to compare text string.  If both values are
# numeric, then a numeric comparison is used. If the two values
# are of different types, then numbers are considered less than
# strings and strings are considered less than blobs.
#
# If the SQLITE_STOREP2 bit of P5 is set, then do not jump.  Instead,
# store a boolean result (either 0, or 1, or NULL) in register P2.
#
# If the SQLITE_NULLEQ bit is set in P5, then NULL values are considered
# equal to one another, provided that they do not have their MEM_Cleared
# bit set.
#
# Opcode: Ne P1 P2 P3 P4 P5
# Synopsis: if r[P1]!=r[P3] goto P2
#
# This works just like the Lt opcode except that the jump is taken if
# the operands in registers P1 and P3 are not equal.  See the Lt opcode for
# additional information.
#
# If SQLITE_NULLEQ is set in P5 then the result of comparison is always either
# true or false and is never NULL.  If both operands are NULL then the result
# of comparison is false.  If either operand is NULL then the result is true.
# If neither operand is NULL the result is the same as it would be if
# the SQLITE_NULLEQ flag were omitted from P5.
#
# Opcode: Eq P1 P2 P3 P4 P5
# Synopsis: if r[P1]==r[P3] goto P2
#
# This works just like the Lt opcode except that the jump is taken if
# the operands in registers P1 and P3 are equal.
# See the Lt opcode for additional information.
#
# If SQLITE_NULLEQ is set in P5 then the result of comparison is always either
# true or false and is never NULL.  If both operands are NULL then the result
# of comparison is true.  If either operand is NULL then the result is false.
# If neither operand is NULL the result is the same as it would be if
# the SQLITE_NULLEQ flag were omitted from P5.
#
# Opcode: Le P1 P2 P3 P4 P5
# Synopsis: if r[P1]<=r[P3] goto P2
#
# This works just like the Lt opcode except that the jump is taken if
# the content of register P3 is less than or equal to the content of
# register P1.  See the Lt opcode for additional information.
#
# Opcode: Gt P1 P2 P3 P4 P5
# Synopsis: if r[P1]>r[P3] goto P2
#
# This works just like the Lt opcode except that the jump is taken if
# the content of register P3 is greater than the content of
# register P1.  See the Lt opcode for additional information.
#
# Opcode: Ge P1 P2 P3 P4 P5
# Synopsis: if r[P1]>=r[P3] goto P2
#
# This works just like the Lt opcode except that the jump is taken if
# the content of register P3 is greater than or equal to the content of
# register P1.  See the Lt opcode for additional information.

def python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(hlquery, pc, rc, op):
    p = hlquery.p
    db = hlquery.db
    pIn1, flags1 = op.mem_and_flags_of_p(1)    # 1st input operand
    pIn3, flags3 = op.mem_and_flags_of_p(3)    # 3st input operand
    orig_flags1 = flags1
    orig_flags3 = flags3
    opcode = op.get_opcode()
    flags_can_have_changed = False
    p5 = op.p_Unsigned(5)


    if (flags1 | flags3) & CConfig.MEM_Null:
        # /* One or both operands are NULL */
        if p5 & CConfig.SQLITE_NULLEQ:
            # /* If SQLITE_NULLEQ is set (which will only happen if the operator is
            #  ** OP_Eq or OP_Ne) then take the jump or not depending on whether
            #  ** or not both operands are null.
            #  */
            assert opcode == CConfig.OP_Eq or opcode == CConfig.OP_Ne
            assert flags1 & CConfig.MEM_Cleared == 0
            assert p5 & CConfig.SQLITE_JUMPIFNULL == 0
            if (flags1 & CConfig.MEM_Null != 0
                and flags3 & CConfig.MEM_Null != 0
                and flags3 & CConfig.MEM_Cleared == 0):
                res = 0     # /* Results are equal */
            else:
                res = 1     # /* Results are not equal */
        else:
            # /* SQLITE_NULLEQ is clear and at least one operand is NULL,
            #  ** then the result is always NULL.
            #  ** The jump is taken if the SQLITE_JUMPIFNULL bit is set.
            #  */
            if p5 & CConfig.SQLITE_STOREP2:
                pOut = op.mem_of_p(2)

                pOut.MemSetTypeFlag(CConfig.MEM_Null)

                # Used only for debugging, i.e., not in production.
                # See vdbe.c lines 451-455.
                # REGISTER_TRACE(pOp->p2, pOut);

            else:
                # VdbeBranchTaken() is used for test suite validation only and
                # does not appear an production builds.
                # See vdbe.c lines 110-136.
                # VdbeBranchTaken(2,3);
                if p5 & CConfig.SQLITE_JUMPIFNULL:
                    pc = op.p2as_pc()
            return pc, rc
    else:

    ################################ MODIFIED BLOCK STARTS ################################

        if (flags1 | flags3) & (CConfig.MEM_Int | CConfig.MEM_Real):
            # both are ints
            if flags1 & flags3 & CConfig.MEM_Int:
                i1 = pIn1.get_u_i()
                i3 = pIn3.get_u_i()
                res = int(_cmp_depending_on_opcode(opcode, i3, i1))
            else:
                # mixed int and real comparison, convert to real
                # XXX this is wrong if one of them is not an int nor a float
                n1 = pIn1.get_u_i() if flags1 & CConfig.MEM_Int else pIn1.get_u_r()
                n3 = pIn3.get_u_i() if flags3 & CConfig.MEM_Int else pIn3.get_u_r()

                res = int(_cmp_depending_on_opcode(opcode, n3, n1))
        else:
            flags_can_have_changed = True

            # Call C functions
            # /* Neither operand is NULL.  Do a comparison. */
            affinity = rffi.cast(lltype.Signed, p5 & CConfig.SQLITE_AFF_MASK)
            if affinity != 0:
                encoding = hlquery.enc()
                pIn1.applyAffinity(affinity, encoding)
                flags1 = pIn1.get_flags()
                pIn3.applyAffinity(affinity, encoding)
                flags3 = pIn3.get_flags()
                if rffi.cast(lltype.Unsigned, db.mallocFailed) != 0:
                    # goto no_mem;
                    print 'In python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(): no_mem.'
                    rc = hlquery.gotoNoMem(pc)
                    return pc, rc

            assert op.p4type() == CConfig.P4_COLLSEQ or not op.p4_pColl()
            pIn1.ExpandBlob()
            pIn3.ExpandBlob()

            res = pIn3.sqlite3MemCompare(pIn1, op.p4_pColl())
            if opcode == CConfig.OP_Eq:
                res = 1 if res == 0 else 0
            elif opcode == CConfig.OP_Ne:
                res = 1 if res != 0 else 0
            elif opcode == CConfig.OP_Lt:
                res = 1 if res < 0 else 0
            elif opcode == CConfig.OP_Le:
                res = 1 if res <= 0 else 0
            elif opcode == CConfig.OP_Gt:
                res = 1 if res > 0 else 0
            else:
                res = 1 if res >= 0 else 0

    ################################# MODIFIED BLOCK ENDS #################################

    if p5 & CConfig.SQLITE_STOREP2:
        pOut = op.mem_of_p(2)
        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 24-37.
        #   memAboutToChange(p, pOut);

        pOut.MemSetTypeFlag(CConfig.MEM_Int)

        pOut.set_u_i(res)

        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 451-455.
        # REGISTER_TRACE(pOp->p2, pOut);
    else:
        # VdbeBranchTaken() is used for test suite validation only and 
        # does not appear an production builds.
        # See vdbe.c lines 110-136.
        # VdbeBranchTaken(res!=0, (pOp->p5 & SQLITE_NULLEQ)?2:3);

        if res != 0:
            pc = op.p2as_pc()

    if flags_can_have_changed:
        # /* Undo any changes made by applyAffinity() to the input registers. */
        new_flags1 = (flags1 & ~CConfig.MEM_TypeMask) | (orig_flags1 & CConfig.MEM_TypeMask)
        new_flags3 = (flags3 & ~CConfig.MEM_TypeMask) | (orig_flags3 & CConfig.MEM_TypeMask)
        if new_flags1 != flags1:
            pIn1.set_flags(new_flags1)
        if new_flags3 != flags3:
            pIn3.set_flags(new_flags3)

    return pc, rc

def python_OP_Noop_Explain_translated(op):
    opcode = rffi.cast(lltype.Unsigned, op.get_opcode())
    assert opcode == CConfig.OP_Noop or opcode == CConfig.OP_Explain


# Opcode: IsNull P1 P2 * * *
# Synopsis:  if r[P1]==NULL goto P2
#
# Jump to P2 if the value in register P1 is NULL.

def python_OP_IsNull(hlquery, pc, op):
    pIn1, flags1 = op.mem_and_flags_of_p(1)
    if flags1 & CConfig.MEM_Null != 0:
        pc = op.p2as_pc()
    return pc


# Opcode: NotNull P1 P2 * * *
# Synopsis: if r[P1]!=NULL goto P2
#
# Jump to P2 if the value in register P1 is not NULL.  

def python_OP_NotNull(hlquery, pc, op):
    pIn1, flags1 = op.mem_and_flags_of_p(1)
    if flags1 & CConfig.MEM_Null == 0:
        pc = op.p2as_pc()
    return pc


# Opcode: Once P1 P2 * * *
#
# Check if OP_Once flag P1 is set. If so, jump to instruction P2. Otherwise,
# set the flag and fall through to the next instruction.  In other words,
# this opcode causes all following opcodes up through P2 (but not including
# P2) to run just once and to be skipped on subsequent times through the loop.

def python_OP_Once(hlquery, pc, op):
    p = hlquery.p
    p1 = op.p_Signed(1)
    assert p1 < rffi.cast(lltype.Signed, p.nOnceFlag)
    if rffi.cast(lltype.Signed, p.aOnceFlag[p1]):
        pc = op.p2as_pc()
    else:
        p.aOnceFlag[p1] = rffi.cast(CConfig.u8, 1)
    return pc


def python_OP_MustBeInt(hlquery, pc, rc, op):
    # not a full translation, only translate the fast path where the argument
    # is already an int
    pIn1, flags1 = op.mem_and_flags_of_p(1)
    if not flags1 & CConfig.MEM_Int:
        hlquery.internalPc[0] = rffi.cast(rffi.LONG, pc)
        pIn1.invalidate_cache()
        rc = capi.impl_OP_MustBeInt(hlquery.p, hlquery.db, hlquery.internalPc, rc, op.pOp)
        retPc = hlquery.internalPc[0]
        return retPc, rc
    pIn1.MemSetTypeFlag(CConfig.MEM_Int)
    return pc, rc


# Opcode: Affinity P1 P2 * P4 *
# Synopsis: affinity(r[P1@P2])
#
# Apply affinities to a range of P2 registers starting with P1.
#
# P4 is a string that is P2 characters long. The nth character of the
# string indicates the column affinity that should be used for the nth
# memory cell in the range.

@jit.unroll_safe
def python_OP_Affinity(hlquery, op):
    zAffinity = op.p4_z() # The affinity to be applied
    encoding = hlquery.enc()
    index = op.p_Signed(1)
    length =  op.p_Signed(2)
    assert len(zAffinity) == length
    for i in range(length):
        hlquery.mem_with_index(index).applyAffinity(ord(zAffinity[i]), encoding)
        index += 1


# Opcode: RealAffinity P1 * * * *
#
# If register P1 holds an integer convert it to a real value.
#
# This opcode is used when extracting information from a column that
# has REAL affinity.  Such column values may still be stored as
# integers, for space efficiency, but after extraction we want them
# to have only a real value.

def python_OP_RealAffinity(hlquery, op):
    pIn1, flags = op.mem_and_flags_of_p(1)
    if flags & CConfig.MEM_Int and not flags & CConfig.MEM_Real:
        # only relevant parts of sqlite3VdbeMemRealify
        pIn1.set_u_r(float(pIn1.get_u_i()))
        pIn1.MemSetTypeFlag(CConfig.MEM_Real)


# Opcode: If P1 P2 P3 * *
#
# Jump to P2 if the value in register P1 is true.  The value
# is considered true if it is numeric and non-zero.  If the value
# in P1 is NULL then take the jump if P3 is non-zero.
#
# Opcode: IfNot P1 P2 P3 * *
#
# Jump to P2 if the value in register P1 is False.  The value
# is considered false if it has a numeric value of zero.  If the value
# in P1 is NULL then take the jump if P3 is zero.

def python_OP_If_IfNot(hlquery, pc, op):
    p = hlquery.p
    pIn1, flags1 = op.mem_and_flags_of_p(1)    # 1st input operand
    opcode = op.get_opcode()

    if flags1 & CConfig.MEM_Null:
        c = op.p_Signed(3)
    else:
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        # #ifdef SQLITE_OMIT_FLOATING_POINT
        #     c = sqlite3VdbeIntValue(pIn1)!=0;
        # #else
        c = pIn1.sqlite3VdbeRealValue() != 0.0
        # #endif
        if opcode == CConfig.OP_IfNot:
            c = not c
    # VdbeBranchTaken() is used for test suite validation only and 
    # does not appear an production builds.
    # See vdbe.c lines 110-136.
    # VdbeBranchTaken(c!=0, 2);
    if c:
        pc = op.p2as_pc()

    return pc




# Opcode: Add P1 P2 P3 * *
# Synopsis:  r[P3]=r[P1]+r[P2]
#
# Add the value in register P1 to the value in register P2
# and store the result in register P3.
# If either input is NULL, the result is NULL.
#
# Opcode: Multiply P1 P2 P3 * *
# Synopsis:  r[P3]=r[P1]*r[P2]
#
#
# Multiply the value in register P1 by the value in register P2
# and store the result in register P3.
# If either input is NULL, the result is NULL.
#
# Opcode: Subtract P1 P2 P3 * *
# Synopsis:  r[P3]=r[P2]-r[P1]
#
# Subtract the value in register P1 from the value in register P2
# and store the result in register P3.
# If either input is NULL, the result is NULL.
#
# Opcode: Divide P1 P2 P3 * *
# Synopsis:  r[P3]=r[P2]/r[P1]
#
# Divide the value in register P1 by the value in register P2
# and store the result in register P3 (P3=P2/P1). If the value in 
# register P1 is zero, then the result is NULL. If either input is 
# NULL, the result is NULL.
#
# Opcode: Remainder P1 P2 P3 * *
# Synopsis:  r[P3]=r[P2]%r[P1]
#
# Compute the remainder after integer register P2 is divided by 
# register P1 and store the result in register P3. 
# If the value in register P1 is zero the result is NULL.
# If either operand is NULL, the result is NULL.

def python_OP_Add_Subtract_Multiply_Divide_Remainder(hlquery, op):
    p = hlquery.p
    opcode = op.get_opcode()

    pIn1, flags1 = op.mem_and_flags_of_p(1)    # 1st input operand
    type1 = pIn1.numericType()
    pIn2, flags2 = op.mem_and_flags_of_p(2)    # 2nd input operand
    type2 = pIn2.numericType()
    pOut = op.mem_of_p(3)
    flags = flags1 | flags2

    if flags & CConfig.MEM_Null != 0:
        pOut.sqlite3VdbeMemSetNull()
        return

    bIntint = False
    constant = pIn1.is_constant_u_i() and pIn2.is_constant_u_i()
    if opcode == CConfig.OP_Add:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.get_u_i()
            iB = pIn2.get_u_i()
            try:
                iB = rarithmetic.ovfcheck(iA + iB)
            except OverflowError:
                pass
            else:
                pOut.set_u_i(iB, constant=constant)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1.sqlite3VdbeRealValue()
        rB = pIn2.sqlite3VdbeRealValue()
        rB += rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            pOut.sqlite3VdbeMemSetNull()
            return
        pOut.set_u_r(float(rB))
        pOut.MemSetTypeFlag(CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            pOut.sqlite3VdbeIntegerAffinity()
        #endif                
        return
    elif opcode == CConfig.OP_Subtract:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.get_u_i()
            iB = pIn2.get_u_i()
            try:
                iB = rarithmetic.ovfcheck(iB - iA)
            except OverflowError:
                pass
            else:
                pOut.set_u_i(iB, constant=constant)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1.sqlite3VdbeRealValue()
        rB = pIn2.sqlite3VdbeRealValue()
        rB -= rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            pOut.sqlite3VdbeMemSetNull()
            return
        pOut.set_u_r(float(rB))
        pOut.MemSetTypeFlag(CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            pOut.sqlite3VdbeIntegerAffinity()
        #endif                
        return
    elif opcode == CConfig.OP_Multiply:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.get_u_i()
            iB = pIn2.get_u_i()
            try:
                iB = rarithmetic.ovfcheck(iA * iB)
            except OverflowError:
                pass
            else:
                pOut.set_u_i(iB, constant=constant)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1.sqlite3VdbeRealValue()
        rB = pIn2.sqlite3VdbeRealValue()
        rB *= rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            pOut.sqlite3VdbeMemSetNull()
            return
        pOut.set_u_r(float(rB))
        pOut.MemSetTypeFlag(CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            pOut.sqlite3VdbeIntegerAffinity()
        #endif                
        return
    elif opcode == CConfig.OP_Divide:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.get_u_i()
            iB = pIn2.get_u_i()
            if iA == 0:
                pOut.sqlite3VdbeMemSetNull()
                return
            try:
                iB = rarithmetic.ovfcheck(iB / iA) # XXX how's the rounding behaviour?
            except OverflowError:
                pass
            else:
                pOut.set_u_i(iB, constant=constant)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1.sqlite3VdbeRealValue()
        rB = pIn2.sqlite3VdbeRealValue()
        # /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
        if rA == 0.0:
            pOut.sqlite3VdbeMemSetNull()
            return
        rB /= rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            pOut.sqlite3VdbeMemSetNull()
            return
        pOut.set_u_r(float(rB))
        pOut.MemSetTypeFlag(CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            pOut.sqlite3VdbeIntegerAffinity()
        #endif                
        return
    elif opcode == CConfig.OP_Remainder:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.get_u_i()
            iB = pIn2.get_u_i()
            bIntint = 1
            if iA == 0:
                pOut.sqlite3VdbeMemSetNull()
                return
            if iA == -1:
                iA = 1
            iB %= iA
            pOut.set_u_i(iB)
            pOut.MemSetTypeFlag(CConfig.MEM_Int)
            return
        else:
            bIntint = 0
            rA = pIn1.sqlite3VdbeRealValue()
            rB = pIn2.sqlite3VdbeRealValue()
            iA = int(rA)
            iB = int(rB)
            if iA == 0:
                pOut.sqlite3VdbeMemSetNull()
                return
            if iA == -1:
                iA = 1
            rB = iB % iA
            # SQLITE_OMIT_FLOATING_POINT is not defined.
            #ifdef SQLITE_OMIT_FLOATING_POINT
            # pOut->u.i = rB;
            # MemSetTypeFlag(pOut, MEM_Int);
            #else
            # if( sqlite3IsNaN(rB) ){
            if math.isnan(rB):
                pOut.sqlite3VdbeMemSetNull()
                return
            pOut.set_u_r(float(rB))
            pOut.MemSetTypeFlag(CConfig.MEM_Real)
            if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
                pOut.sqlite3VdbeIntegerAffinity()
            #endif                
            return;      
    else:
        print "Error: Unknown opcode %s in python_OP_Add_Subtract_Multiply_Divide_Remainder()." % opcode
    return


# Opcode: Integer P1 P2 * * *
# Synopsis: r[P2]=P1
#
# The 32-bit integer value P1 is written into register P2.

def python_OP_Integer(hlquery, op):
    pOut = op.mem_of_p(2)
    pOut.set_u_i(op.p_Signed(1), constant=True)


# Opcode: NotExists P1 P2 P3 * *
# Synopsis: intkey=r[P3]
#
# P1 is the index of a cursor open on an SQL table btree (with integer
# keys).  P3 is an integer rowid.  If P1 does not contain a record with
# rowid P3 then jump immediately to P2.  If P1 does contain a record
# with rowid P3 then leave the cursor pointing at that record and fall
# through to the next instruction.
#
# The OP_NotFound opcode performs the same operation on index btrees
# (with arbitrary multi-value keys).
#
# See also: Found, NotFound, NoConflict

def python_OP_NotExists(hlquery, pc, op):
    p = hlquery.p
    pIn3 = op.mem_of_p(3)
    pC = p.apCsr[op.p_Signed(1)]
    assert pC
    #assert rffi.cast(lltype.Signed, pC.isTable)
    assert not rffi.cast(lltype.Signed, pC.pseudoTableReg)
    pCrsr = pC.pCursor
    assert pCrsr
    iKey = pIn3.get_u_i()
    rc = capi.sqlite3BtreeMovetoUnpacked(pCrsr, lltype.nullptr(rffi.VOIDP.TO), iKey, 0, hlquery.intp)
    res = rffi.cast(lltype.Signed, hlquery.intp[0])
    pC.rowidIsValid = rffi.cast(lltype.typeOf(pC.rowidIsValid), 1 if res == 0 else 0)
    pC.nullRow = rffi.cast(lltype.typeOf(pC.nullRow), 0)
    pC.cacheStatus = rffi.cast(lltype.typeOf(pC.cacheStatus), CConfig.CACHE_STALE)
    pC.deferredMoveto = rffi.cast(lltype.typeOf(pC.deferredMoveto), 0)
    if res:
        pc = op.p2as_pc()
        assert not rffi.cast(lltype.Signed, pC.rowidIsValid)
    pC.seekResult = rffi.cast(lltype.typeOf(pC.seekResult), res)
    return pc, rc

# Opcode: IfPos P1 P2 * * *
# Synopsis: if r[P1]>0 goto P2
#
# If the value of register P1 is 1 or greater, jump to P2.
#
# It is illegal to use this instruction on a register that does
# not contain an integer.  An assertion fault will result if you try.


def python_OP_IfPos(hlquery, pc, op):
    pIn1 = op.mem_of_p(1)
    if pIn1.get_u_i() > 0:
        pc = op.p2as_pc()
    return pc


# Opcode: IdxRowid P1 P2 * * *
# Synopsis: r[P2]=rowid
#
# Write into register P2 an integer which is the last entry in the record at
# the end of the index key pointed to by cursor P1.  This integer should be
# the rowid of the table entry to which this index entry points.
#
# See also: Rowid, MakeRecord.

def python_OP_IdxRowid(hlquery, pc, rc, op):
    p = hlquery.p
    db = hlquery.db

    pOut = op.mem_of_p(2)
    # assert( pOp.p1>=0 && pOp.p1<p.nCursor );
    pC = p.apCsr[op.p_Signed(1)]
    assert pC
    pCrsr = pC.pCursor
    assert pCrsr
    rc = rffi.cast(lltype.Signed, capi.sqlite3VdbeCursorMoveto(pC))
    if rc:
        print "In python_OP_IdxRowid():1: abort_due_to_error."
        rc = capi.gotoAbortDueToError(p, db, pc, rc)
        return rc
    #assert pC.deferredMoveto == 0
    #assert pC.isTable == 0
    if not rffi.cast(lltype.Signed, pC.nullRow):
        rc = capi.sqlite3VdbeIdxRowid(db, pCrsr, hlquery.longp)
        jit.promote(rc)
        if rc != CConfig.SQLITE_OK:
            print "In python_OP_IdxRowid():2: abort_due_to_error."
            rc = capi.gotoAbortDueToError(p, db, pc, rc)
            return rc
        pOut.set_u_i(hlquery.longp[0])
        pOut.set_flags(CConfig.MEM_Int)
    else:
        pOut.set_flags(CConfig.MEM_Null)
    return rc


# Opcode: Seek P1 P2 * * *
# Synopsis:  intkey=r[P2]
#
# P1 is an open table cursor and P2 is a rowid integer.  Arrange
# for P1 to move so that it points to the rowid given by P2.
#
# This is actually a deferred seek.  Nothing actually happens until
# the cursor is used to read a record.  That way, if no reads
# occur, no unnecessary I/O happens.

def python_OP_Seek(hlquery, op):
    p = hlquery.p
    pC = p.apCsr[op.p_Signed(1)]
    assert pC
    assert pC.pCursor
    # assert( pC.isTable );
    rffi.setintfield(pC, 'nullRow', 0)
    pIn2 = op.mem_of_p(2)
    pC.movetoTarget = pIn2.sqlite3VdbeIntValue()
    rffi.setintfield(pC, 'rowidIsValid', 0)
    rffi.setintfield(pC, 'deferredMoveto', 1)


# Opcode: AggStep * P2 P3 P4 P5
# Synopsis: accum=r[P3] step(r[P2@P5])
#
# Execute the step function for an aggregate.  The
# function has P5 arguments.   P4 is a pointer to the FuncDef
# structure that specifies the function.  Use register
# P3 as the accumulator.
#
# The P5 arguments are taken from register P2 and its
# successors.

@jit.unroll_safe
def python_OP_AggStep(hlquery, rc, pc, op):
    from sqpyte.mem import Mem
    p = hlquery.p
    db = hlquery.db
    n = op.p_Signed(5)
    index = op.p_Signed(2)
    func = op.p4_pFunc()
    if func.exists_in_python():
        return func.aggstep_in_python(hlquery, op, index, n)
    hlquery.invalidate_caches()
    apVal = p.apArg
    assert apVal or n == 0
    for i in range(n):
        apVal[i] = hlquery.mem_with_index(index + i).pMem
    pFunc = func.pfunc
    mem = op.mem_of_p(3)
    with lltype.scoped_alloc(capi.CONTEXT) as ctx:
        mems = Mem(hlquery, ctx.s)
        ctx.pFunc = pFunc
        assert op.p_Signed(3) > 0 and op.p_Signed(3) <= (rffi.getintfield(p, 'nMem') - rffi.getintfield(p, 'nCursor'))
        ctx.pMem = mem.pMem
        mem.set_n(mem.get_n() + 1)
        mems.set_flags(CConfig.MEM_Null)
        mems.set_z_null()
        mems.set_zMalloc_null()
        mems.set_xDel_null()
        mems.set_db(db)
        rffi.setintfield(ctx, 'isError', 0)
        ctx.pColl = lltype.nullptr(lltype.typeOf(ctx).TO.pColl.TO)
        rffi.setintfield(ctx, 'skipFlag', 0)
        if rffi.getintfield(ctx.pFunc, 'funcFlags') & CConfig.SQLITE_FUNC_NEEDCOLL:
            prevop = hlquery._hlops[pc - 1]
            ctx.pColl = prevop.p4_pColl()
        xStep = rffi.cast(capi.FUNCTYPESTEPP, pFunc.xStep)
        xStep(ctx, rffi.cast(rffi.INT, n), apVal)  # /* IMP: R-24505-23230 */
        if rffi.getintfield(ctx, 'isError'):
            assert 0
            # XXX fix error handling: sqlite3SetString(&p->zErrMsg, db, "%s", sqlite3_value_text(&ctx.s));
            rc = rffi.cast(lltype.Signed, ctx.isError)
        if rffi.getintfield(ctx, 'skipFlag'):
            prevop = hlquery._hlops[pc - 1]
            assert prevop.get_opcode() == CConfig.OP_CollSeq
            i = prevop.p_Signed(1)
            if i:
                hlquery.mem_with_index(i).sqlite3VdbeMemSetInt64(1)
        mems.sqlite3VdbeMemRelease()
        return rc

# Opcode: AggFinal P1 P2 * P4 *
# Synopsis: accum=r[P1] N=P2
#
# Execute the finalizer function for an aggregate.  P1 is
# the memory location that is the accumulator for the aggregate.
#
# P2 is the number of arguments that the step function takes and
# P4 is a pointer to the FuncDef for this function.  The P2
# argument is not used by this opcode.  It is only there to disambiguate
# functions that can take varying numbers of arguments.  The
# P4 argument is only needed for the degenerate case where
# the step function was not previously called.

def python_OP_AggFinal(hlquery, pc, rc, op):
    func = op.p4_pFunc()
    mem = op.mem_of_p(1)
    if func.exists_in_python():
        return func.aggfinal_in_python(hlquery, op, mem)
    hlquery.invalidate_caches()
    return capi.impl_OP_AggFinal(hlquery.p, hlquery.db, pc, rc, op.pOp)

# Opcode: IdxGE P1 P2 P3 P4 P5
# Synopsis: key=r[P3@P4]
#
# The P4 register values beginning with P3 form an unpacked index 
# key that omits the PRIMARY KEY.  Compare this key value against the index 
# that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID 
# fields at the end.
#
# If the P1 index entry is greater than or equal to the key value
# then jump to P2.  Otherwise fall through to the next instruction.
#
# Opcode: IdxGT P1 P2 P3 P4 P5
# Synopsis: key=r[P3@P4]
#
# The P4 register values beginning with P3 form an unpacked index 
# key that omits the PRIMARY KEY.  Compare this key value against the index 
# that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID 
# fields at the end.
#
# If the P1 index entry is greater than the key value
# then jump to P2.  Otherwise fall through to the next instruction.
#
# Opcode: IdxLT P1 P2 P3 P4 P5
# Synopsis: key=r[P3@P4]
#
# The P4 register values beginning with P3 form an unpacked index 
# key that omits the PRIMARY KEY or ROWID.  Compare this key value against
# the index that P1 is currently pointing to, ignoring the PRIMARY KEY or
# ROWID on the P1 index.
#
# If the P1 index entry is less than the key value then jump to P2.
# Otherwise fall through to the next instruction.
#
# Opcode: IdxLE P1 P2 P3 P4 P5
# Synopsis: key=r[P3@P4]
#
# The P4 register values beginning with P3 form an unpacked index 
# key that omits the PRIMARY KEY or ROWID.  Compare this key value against
# the index that P1 is currently pointing to, ignoring the PRIMARY KEY or
# ROWID on the P1 index.
#
# If the P1 index entry is less than or equal to the key value then jump
# to P2. Otherwise fall through to the next instruction.

def python_OP_IdxLE_IdxGT_IdxLT_IdxGE(hlquery, pc, op):
    p = hlquery.p

    assert op.p_Unsigned(1) >= 0 and op.p_Signed(1) < rffi.getintfield(p, 'nCursor')
    pC = p.apCsr[op.p_Unsigned(1)]
    assert pC
    # assert pC.isOrdered
    assert pC.pCursor
    assert rffi.getintfield(pC, 'deferredMoveto') == 0
    assert op.p_Unsigned(5) == 0 or op.p_Unsigned(5) == 1
    assert op.p4type() == CConfig.P4_INT32
    r = hlquery.unpackedrecordp
    r.pKeyInfo = pC.pKeyInfo
    r.nField = rffi.cast(CConfig.u16, op.p4_i())
    if op.get_opcode() < CConfig.OP_IdxLT:
        assert op.get_opcode() == CConfig.OP_IdxLE or op.get_opcode() == CConfig.OP_IdxGT
        r.default_rc = rffi.cast(CConfig.i8, -1)
    else:
        assert op.get_opcode() == CConfig.OP_IdxGE or op.get_opcode() == CConfig.OP_IdxLT
        r.default_rc = rffi.cast(CConfig.i8, 0)
    r.aMem = op.mem_of_p(3).pMem

    # Used only for debugging.
    #ifdef SQLITE_DEBUG
      # { int i; for(i=0; i<r.nField; i++) assert( memIsValid(&r.aMem[i]) ); }
    #endif

    resMem = hlquery.intp
    resMem[0] = rffi.cast(rffi.INT, 0)
    rc = capi.sqlite3VdbeIdxKeyCompare(hlquery.db, pC, r, resMem)
    res = rffi.cast(lltype.Signed, resMem[0])
    jit.promote(res)

    assert (CConfig.OP_IdxLE & 1) == (CConfig.OP_IdxLT & 1) and (CConfig.OP_IdxGE & 1) == (CConfig.OP_IdxGT & 1)
    if (op.get_opcode() & 1) == (CConfig.OP_IdxLT & 1):
        assert op.get_opcode() == CConfig.OP_IdxLE or op.get_opcode() == CConfig.OP_IdxLT
        res = -res
    else:
        assert op.get_opcode() == CConfig.OP_IdxGE or op.get_opcode() == CConfig.OP_IdxGT
        res += 1

    # VdbeBranchTaken() is used for test suite validation only and 
    # does not appear an production builds.
    # See vdbe.c lines 110-136.
    # VdbeBranchTaken(res>0,2);

    if res > 0:
        pc = op.p2as_pc()

    return pc, rc


# Opcode: Compare P1 P2 P3 P4 P5
# Synopsis: r[P1@P3] <-> r[P2@P3]
#
# Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this
# vector "A") and in reg(P2)..reg(P2+P3-1) ("B").  Save the result of
# the comparison for use by the next OP_Jump instruct.
#
# If P5 has the OPFLAG_PERMUTE bit set, then the order of comparison is
# determined by the most recent OP_Permutation operator.  If the
# OPFLAG_PERMUTE bit is clear, then register are compared in sequential
# order.
#
# P4 is a KeyInfo structure that defines collating sequences and sort
# orders for the comparison.  The permutation applies to registers
# only.  The KeyInfo elements are used sequentially.
#
# The comparison is a sort comparison, so NULLs compare equal,
# NULLs are less than numbers, numbers are less than strings,
# and strings are less than blobs.

@jit.unroll_safe
def python_OP_Compare(hlquery, op):
    p = hlquery.p

    if (op.p_Unsigned(5) & CConfig.OPFLAG_PERMUTE) == 0:
        aPermute = lltype.nullptr(rffi.INTP.TO)
    else:
        assert 0, "Not implemented, need support for OP_Permutation."
    n = op.p_Signed(3)
    pKeyInfo = op.p4_pKeyInfo()
    assert n > 0
    assert pKeyInfo
    p1 = op.p_Signed(1)
    p2 = op.p_Signed(2)

    # Used only for debugging.
    #if SQLITE_DEBUG
      # if( aPermute ){
      #   int k, mx = 0;
      #   for(k=0; k<n; k++) if( aPermute[k]>mx ) mx = aPermute[k];
      #   assert( p1>0 && p1+mx<=(p->nMem-p->nCursor)+1 );
      #   assert( p2>0 && p2+mx<=(p->nMem-p->nCursor)+1 );
      # }else{
      #   assert( p1>0 && p1+n<=(p->nMem-p->nCursor)+1 );
      #   assert( p2>0 && p2+n<=(p->nMem-p->nCursor)+1 );
      # }
    #endif /* SQLITE_DEBUG */

    for i in range(n):
        if aPermute:
            idx = rffi.cast(lltype.Signed, aPermute[i])
        else:
            idx = i
        # assert( memIsValid(&aMem[p1+idx]) );
        # assert( memIsValid(&aMem[p2+idx]) );

        # REGISTER_TRACE() is used only for debugging,
        # i.e., not in production.
        # See vdbe.c lines 451-455.
        # REGISTER_TRACE(p1+idx, &aMem[p1+idx]);
        # REGISTER_TRACE(p2+idx, &aMem[p2+idx]);

        pColl = op.p4_pKeyInfo_aColl(i)
        pIn1 = hlquery.mem_with_index(p1 + idx)
        pIn2 = hlquery.mem_with_index(p2 + idx)
        iCompare = pIn1.sqlite3MemCompare(pIn2, pColl)
        jit.promote(iCompare)
        if iCompare:
            bRev = op.p4_pKeyInfo_aSortOrder(i)
            if bRev:
                iCompare = -iCompare
            hlquery.iCompare = iCompare
            return
    hlquery.iCompare = 0
    aPermute = lltype.nullptr(rffi.INTP.TO)
    return


# Opcode: Jump P1 P2 P3 * *
#
# Jump to the instruction at address P1, P2, or P3 depending on whether
# in the most recent OP_Compare instruction the P1 vector was less than
# equal to, or greater than the P2 vector, respectively.

def python_OP_Jump(hlquery, op):

    # VdbeBranchTaken() is used for test suite validation only and 
    # does not appear an production builds.
    # See vdbe.c lines 110-136.

    if hlquery.iCompare < 0:
        pc = op.p_Signed(1) - 1
        # VdbeBranchTaken(0,3);
    elif hlquery.iCompare == 0:
        pc = op.p_Signed(2) - 1
        # VdbeBranchTaken(1,3);
    else:
        pc = op.p_Signed(3) - 1
        # VdbeBranchTaken(2,3);
    hlquery.iCompare = 0
    return pc

# Opcode: SCopy P1 P2 * * *
# Synopsis: r[P2]=r[P1]
#
# Make a shallow copy of register P1 into register P2.
#
# This instruction makes a shallow copy of the value.  If the value
# is a string or blob, then the copy is only a pointer to the
# original and hence if the original changes so will the copy.
# Worse, if the original is deallocated, the copy becomes invalid.
# Thus the program must guarantee that the original will not change
# during the lifetime of the copy.  Use OP_Copy to make a complete
# copy.

def python_OP_SCopy(hlquery, op):
    pIn1 = op.mem_of_p(1)
    pOut = op.mem_of_p(2)
    assert pOut != pIn1
    pOut.sqlite3VdbeMemShallowCopy(pIn1, CConfig.MEM_Ephem)

    # Used only for debugging.
    #ifdef SQLITE_DEBUG
      # if( pOut->pScopyFrom==0 ) pOut->pScopyFrom = pIn1;
    #endif


# Opcode: Copy P1 P2 P3 * *
# Synopsis: r[P2@P3+1]=r[P1@P3+1]
#
# Make a copy of registers P1..P1+P3 into registers P2..P2+P3.
#
# This instruction makes a deep copy of the value.  A duplicate
# is made of any string or blob constant.  See also OP_SCopy.

@jit.unroll_safe
def python_OP_Copy(hlquery, pc, rc, op):
    n = op.p_Signed(3)
    for i in range(n + 1):
        pIn1 = hlquery.mem_with_index(op.p_Signed(1) + i)
        pOut = hlquery.mem_with_index(op.p_Signed(2) + i)
        pOut.sqlite3VdbeMemShallowCopy(pIn1, CConfig.MEM_Ephem)
        if pOut.get_flags() & CConfig.MEM_Ephem and pOut.sqlite3VdbeMemMakeWriteable():
            # goto no_mem
            print "In python_OP_Copy(): no_mem."
            return hlquery.gotoNoMem(pc)
    return rc


# Opcode: Sequence P1 P2 * * *
# Synopsis: r[P2]=cursor[P1].ctr++
#
# Find the next available sequence number for cursor P1.
# Write the sequence number into register P2.
# The sequence number on the cursor is incremented after this
# instruction.

def python_OP_Sequence(hlquery, op):
    p = hlquery.p
    pOut = op.mem_of_p(2)
    pOut.set_flags(CConfig.MEM_Int)
    assert op.p_Signed(1) >= 0 and op.p_Signed(1) < rffi.getintfield(p, 'nCursor')
    cursor = p.apCsr[op.p_Signed(1)]
    assert cursor
    seqCount = cursor.seqCount
    cursor.seqCount = seqCount + 1
    pOut.set_u_i(seqCount)

def python_OP_Column(hlquery, pc, op):
    result = capi.impl_OP_Column(hlquery.p, hlquery.db, pc, op.pOp)
    return _decode_combined_flags_rc_for_p3(result, op)

def _decode_combined_flags_rc_for_p3(result, op):
    # this is just a trick to promote two values at once, rc and the new flags
    # of p3
    # it also invalidates p3 before doing that
    jit.promote(result)
    rc = result & 0xffff
    if rc:
        return rc
    flags = rarithmetic.r_uint(result >> 16)
    pOut = op.mem_of_p(3)
    pOut.invalidate_cache()
    pOut.assure_flags(flags)
    return rc

# Opcode:  Return P1 * * * *
#
# Jump to the next instruction after the address in register P1.  After
# the jump, register P1 becomes undefined.

def python_OP_Return(hlquery, op):
    p = hlquery.p
    pIn1, flags1 = op.mem_and_flags_of_p(1)    # 1st input operand
    assert flags1 == CConfig.MEM_Int
    pc = pIn1.get_u_i()
    pIn1.set_flags(CConfig.MEM_Undefined)
    return pc

# Opcode:  Gosub P1 P2 * * *
#
# Write the current address onto register P1
# and then jump to address P2.

def python_OP_Gosub(hlquery, pc, op):
    p = hlquery.p
    p1 = op.p_Signed(1)
    assert p1 > 0 and p1 <= (rffi.getintfield(p, 'nMem') - rffi.getintfield(p, 'nCursor'))
    pIn1 = op.mem_of_p(1)
    assert pIn1.VdbeMemDynamic() == 0
    
    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 24-37.
    # memAboutToChange(p, pIn1);
    
    pIn1.set_flags(CConfig.MEM_Int)
    pIn1.set_u_i(pc, constant=True)

    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 451-455.
    # REGISTER_TRACE(pOp->p1, pIn1);

    pc = op.p2as_pc()

    return pc

# Opcode: Move P1 P2 P3 * *
# Synopsis:  r[P2@P3]=r[P1@P3]
#
# Move the P3 values in register P1..P1+P3-1 over into
# registers P2..P2+P3-1.  Registers P1..P1+P3-1 are
# left holding a NULL.  It is an error for register ranges
# P1..P1+P3-1 and P2..P2+P3-1 to overlap.  It is an error
# for P3 to be less than 1.

@jit.unroll_safe
def python_OP_Move(hlquery, op):
    p = hlquery.p
    n = op.p_Signed(3)
    p1 = op.p_Signed(1)
    p2 = op.p_Signed(2)

    assert n > 0 and p1 > 0 and p2 > 0
    assert p1 + n <= p2 or p2 + n <= p1

    pIn1 = op.mem_of_p(1)
    pOut = op.mem_of_p(2)

    # Runs at least once. See assert above.
    while n > 0:
        # assert( pOut<=&aMem[(p->nMem-p->nCursor)] );
        # assert( pIn1<=&aMem[(p->nMem-p->nCursor)] );
        # assert( memIsValid(pIn1) );

        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 24-37.
        # memAboutToChange(p, pOut);

        pOut.VdbeMemRelease()
        zMalloc = pOut.get_zMalloc()
        pOut.memcpy_full(pIn1)
        #ifdef SQLITE_DEBUG
            # if( pOut->pScopyFrom>=&aMem[p1] && pOut->pScopyFrom<&aMem[p1+pOp->p3] ){
            #   pOut->pScopyFrom += p1 - pOp->p2;
            # }
        #endif
        pIn1.set_flags(CConfig.MEM_Undefined)
        pIn1.set_xDel_null()
        pIn1.set_zMalloc(zMalloc)

        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 451-455.
        # REGISTER_TRACE(p2++, pOut);

        p1 += 1
        p2 += 1
        pIn1 = hlquery.mem_with_index(p1)
        pOut = hlquery.mem_with_index(p2)

        n -= 1

@jit.dont_look_inside
def _check_too_big_hidden_from_jit(nByte, db):
    # the JIT can't deal with FixedSizeArrays
    return nByte > rffi.cast(lltype.Signed, db.aLimit[CConfig.SQLITE_LIMIT_LENGTH])


# Opcode: MakeRecord P1 P2 P3 P4 *
# Synopsis: r[P3]=mkrec(r[P1@P2])
#
# Convert P2 registers beginning with P1 into the [record format]
# use as a data record in a database table or as a key
# in an index.  The OP_Column opcode can decode the record later.
#
# P4 may be a string that is P2 characters long.  The nth character of the
# string indicates the column affinity that should be used for the nth
# field of the index key.
#
# The mapping from character to affinity is given by the SQLITE_AFF_
# macros defined in sqliteInt.h.
#
# If P4 is NULL then all index fields have the affinity NONE.

@jit.unroll_safe
def python_OP_MakeRecord(hlquery, pc, rc, op):
    p = hlquery.p
    db = hlquery.db
    #u8 *zNewRecord;        # A buffer to hold the data for the new record
    #Mem *pRec;             # The new record
    #u64 nData;             # Number of bytes of data space
    #int nHdr;              # Number of bytes of header space
    #i64 nByte;             # Data space required for this record
    #int nZero;             # Number of zero bytes at the end of the record
    #int nVarint;           # Number of bytes in a varint
    #u32 serial_type;       # Type field
    #Mem *pData0;           # First field to be combined into the record
    #Mem *pLast;            # Last field of the record
    #int nField;            # Number of fields in the record
    #char *zAffinity;       # The affinity string for the record
    #int file_format;       # File format to use for encoding
    #int i;                 # Space used in zNewRecord[] header
    #int j;                 # Space used in zNewRecord[] content
    #int len;               # Length of a field


    # Assuming the record contains N fields, the record format looks
    # like this:
    #
    # ------------------------------------------------------------------------
    # | hdr-size | type 0 | type 1 | ... | type N-1 | data0 | ... | data N-1 | 
    # ------------------------------------------------------------------------
    #
    # Data(0) is taken from register P1.  Data(1) comes from register P1+1
    # and so froth.
    #
    # Each type field is a varint representing the serial type of the 
    # corresponding data element (see sqlite3VdbeSerialType()). The
    # hdr-size field is also a varint which is the offset from the beginning
    # of the record to data0.

    nData = 0         # Number of bytes of data space
    nHdr = 0          # Number of bytes of header space
    nZero = 0         # Number of zero bytes at the end of the record
    zAffinity = op.p4_z()
    data0 = op.p_Signed(1)
    nField = op.p_Signed(2)
    file_format = p.minWriteFileFormat

    # Identify the output register
    pOut = op.mem_of_p(3) # Output operand

    # Apply the requested affinity to all inputs
    #
    if zAffinity:
        encoding = hlquery.enc() # The database encoding
        j = 0
        for memindex in range(data0, data0 + nField):
            pRec = hlquery.mem_with_index(memindex)
            pRec.applyAffinity(ord(zAffinity[j]), encoding)

    # Loop through the elements that will make up the record to figure
    # out how much space is required for the new record.
    types_lengths_and_hdrlens = [(0, 0, 0)] * nField
    index = 0
    for memindex in range(data0, data0 + nField):
        pRec = hlquery.mem_with_index(memindex)
        assert pRec.memIsValid()
        serial_type, length, hdrlen = pRec.sqlite3VdbeSerialType_Len_and_HdrLen(file_format)
        types_lengths_and_hdrlens[index] = (serial_type, length, hdrlen)
        if pRec.get_flags() & CConfig.MEM_Zero:
            if nData:
                pRec.ExpandBlob()
            else:
                nZero = pRec.get_u_nZero()
                nZero += nZero
                length -= nZero
        nData += length
        nHdr += hdrlen
        index += 1

    # Add the initial header varint and total the size
    if nHdr <= 126:
        # The common case
        nHdr += 1
    else:
        # Rare case of a really large header
        nVarint = sqlite3VarintLen(nHdr)
        nHdr += nVarint
        if nVarint < sqlite3VarintLen(nHdr):
            nHdr += 1
    nByte = nHdr + nData
    if _check_too_big_hidden_from_jit(nByte, db):
        # goto too_big;
        print "In python_OP_MakeRecord(): too_big."
        return hlquery.gotoTooBig(pc)

    # Make sure the output register has a buffer large enough to store
    # the new record. The output register (pOp.p3) is not allowed to
    # be one of the input registers (because the following call to
    # sqlite3VdbeMemGrow() could clobber the value before it is used).
    if pOut.sqlite3VdbeMemGrow(nByte, 0):
        # goto no_mem;
        print "In python_OP_MakeRecord(): no_mem."
        return hlquery.gotoNoMem(pc)

    zNewRecord = pOut.get_z()
    curr_content_ptr = rffi.ptradd(zNewRecord, nHdr)

    # Write the record
    i = putVarint32(zNewRecord, nHdr)
    index = 0
    for memindex in range(data0, data0 + nField):
        pRec = hlquery.mem_with_index(memindex)
        serial_type, length, hdrlen = types_lengths_and_hdrlens[index]
        if hdrlen == 1:
            zNewRecord[i] = chr(serial_type)
            i += 1
        else:
            i += putVarint32(zNewRecord, serial_type, i) # serial type
        addj = pRec._sqlite3VdbeSerialPut_with_length(rffi.cast(capi.U8P, curr_content_ptr), serial_type, length) # content
        assert addj == length
        curr_content_ptr = rffi.ptradd(curr_content_ptr, length)
        index += 1
    assert i == nHdr

    pOut.set_n(nByte)
    pOut.set_flags(CConfig.MEM_Blob)
    pOut.set_xDel_null()
    if nZero:
        pOut.set_u_nZero(nZero)
        pOut.set_flags(pOut.get_flags() | CConfig.MEM_Zero)
    pOut.set_enc_utf8() # In case the blob is ever converted to text

    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 451-455.
    # REGISTER_TRACE(pOp.p3, pOut);

    # Used only for testing, i.e., not in production.
    # See vdbe.c lines 100-108.
    # UPDATE_MAX_BLOBSIZE(pOut);

    return rc

def sqlite3VarintLen(v):
    # Return the number of bytes that will be needed to store the given
    # 64-bit integer.
    i = 0
    while True:
        i += 1
        v >>= 7
        if v == 0:
            break
    return i

def putVarint32(buf, val, index=0):
    if rarithmetic.r_uint(val) < rarithmetic.r_uint(0x80):
        buf[index] = chr(val)
        return 1
    return capi.sqlite3PutVarint32(rffi.cast(capi.U8P, rffi.ptradd(buf, index)), rffi.cast(CConfig.u32, val))

# Opcode: Function P1 P2 P3 P4 P5
# Synopsis: r[P3]=func(r[P2@P5])
#
# Invoke a user function (P4 is a pointer to a Function structure that
# defines the function) with P5 arguments taken from register P2 and
# successors.  The result of the function is stored in register P3.
# Register P3 must not be one of the function inputs.
#
# P1 is a 32-bit bitmask indicating whether or not each argument to the 
# function was determined to be constant at compile time. If the first
# argument was constant then bit 0 of P1 is set. This is used to determine
# whether meta data associated with a user function argument using the
# sqlite3_set_auxdata() API may be safely retained until the next
# invocation of this opcode.
#
# See also: AggStep and AggFinal
def python_OP_Function(hlquery, pc, rc, op):
    hlquery.p2_p5_mutation(op)
    result = capi.impl_OP_Function(hlquery.p, hlquery.db, pc, rc, op.pOp)
    return _decode_combined_flags_rc_for_p3(result, op)

def incomplete_python_OP_Function(hlquery, pc, rc, op):
    p = hlquery.p
    db = hlquery.db
    encoding = hlquery.enc() # The database encoding
    n = op.p_Signed(5)
    p2 = op.p_Signed(2)
    p3 = op.p_Signed(3)
    apVal = p.apArg
    assert apVal or n == 0
    assert p3 > 0 and p3 <= (rffi.getintfield(p, 'nMem') - rffi.getintfield(p, 'nCursor'))
    pOut = op.mem_of_p(3) # Output operand

    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 24-37.
    # memAboutToChange(p, pOut);

    assert n == 0 or (p2 > 0 and p2 + n <= (rffi.getintfield(p, 'nMem') - rffi.getintfield(p, 'nCursor')) + 1)
    assert p3 < p2 or p3 >= p2 + n

    pArg = op.mem_of_p(2)
    for i in range(n):
        assert pArg.memIsValid()
        apVal[i] = hlquery.mem_with_index(p2 + i).pMem

        # // Translated Deephemeralize(pArg);
        # if( ((pArg)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pArg) ) {
        #   // goto no_mem;
        #   printf("In impl_OP_Function():1: no_mem.\n");
        #   rc = (long)gotoNoMem(p, db, (int)pc);
        #   return rc;            
        # }

        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 451-455.
        # REGISTER_TRACE(pOp->p2+i, pArg);

    assert op.p4type() == CConfig.P4_FUNCDEF

    with lltype.scoped_alloc(capi.CONTEXT) as ctx:
        ctx.pFunc = pFunc = op.p4_pFunc()
        ctx.iOp = rffi.cast(rffi.INT, pc)
        ctx.pVdbe = p

        # The output cell may already have a buffer allocated. Move
        # the pointer to ctx.s so in case the user-function can use
        # the already allocated buffer instead of allocating a new one.
        rffi.c_memcpy(rffi.cast(rffi.VOIDP, ctx.s), rffi.cast(rffi.VOIDP, pOut), rffi.sizeof(capi.MEM))
        pOut.set_flags(CConfig.MEM_Null)
        pOut.set_xDel_null()
        pOut.set_zMalloc_null()
        # ctx.s.MemSetTypeFlag(CConfig.MEM_Null) # XXX

        ctx.fErrorOrAux = rffi.cast(rffi.UCHAR, 0)
        if rffi.getintfield(ctx.pFunc, 'funcFlags') & CConfig.SQLITE_FUNC_NEEDCOLL:
            pass
            # assert( pOp>aOp );
            # assert( pOp[-1].p4type==P4_COLLSEQ );
            # assert( pOp[-1].opcode==OP_CollSeq );
            # ctx.pColl = pOp[-1].p4.pColl;
        # db->lastRowid = lastRowid;

        # (*ctx.pFunc->xFunc)(&ctx, n, apVal); /* IMP: R-24505-23230 */
        xFunc = rffi.cast(capi.FUNCTYPESTEPP, pFunc.xFunc)
        xFunc(ctx, rffi.cast(rffi.INT, n), apVal)  # /* IMP: R-24505-23230 */

        # lastRowid = db->lastRowid;

        if rffi.cast(lltype.Bool, db.mallocFailed):
            # Even though a malloc() has failed, the implementation of the
            # user function may have called an sqlite3_result_XXX() function
            # to return a value. The following call releases any resources
            # associated with such a value.
            # ctx.s.sqlite3VdbeMemRelease() # XXX
            # goto no_mem;
            print "In python_OP_Function(): no_mem."
            return hlquery.gotoNoMem(pc)

        # If the function returned an error, throw an exception
        if rffi.cast(lltype.Bool, ctx.fErrorOrAux):
            if rffi.cast(lltype.Bool, ctx.isError):
                assert 0
                # XXX fix error handling: sqlite3SetString(&p->zErrMsg, db, "%s", sqlite3_value_text(&ctx.s));
                rc = rffi.cast(lltype.Signed, ctx.isError)
            # sqlite3VdbeDeleteAuxData(p, (int)pc, pOp->p1);

        # Copy the result of the function into register P3
        # sqlite3VdbeChangeEncoding(&ctx.s, encoding);
        assert pOut.get_flags() == CConfig.MEM_Null
        rffi.c_memcpy(rffi.cast(rffi.VOIDP, pOut), rffi.cast(rffi.VOIDP, ctx.s), rffi.sizeof(capi.MEM))
        # if( sqlite3VdbeMemTooBig(pOut) ){
        #     // goto too_big;
        #     printf("In impl_OP_Function():2: too_big.\n");
        #     rc = (long)gotoTooBig(p, db, (int)pc);
        #     return rc;    
        # }

        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 451-455.
        # REGISTER_TRACE(pOp->p3, pOut);

        # Used only for testing, i.e., not in production.
        # See vdbe.c lines 100-108.
        # UPDATE_MAX_BLOBSIZE(pOut);

        return rc


# Opcode: SeekGe P1 P2 P3 P4 *
# Synopsis: key=r[P3@P4]
#
# If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
# use the value in register P3 as the key.  If cursor P1 refers 
# to an SQL index, then P3 is the first in an array of P4 registers 
# that are used as an unpacked index key. 
#
# Reposition cursor P1 so that  it points to the smallest entry that 
# is greater than or equal to the key value. If there are no records 
# greater than or equal to the key and P2 is not zero, then jump to P2.
#
# See also: Found, NotFound, Distinct, SeekLt, SeekGt, SeekLe
#
# Opcode: SeekGt P1 P2 P3 P4 *
# Synopsis: key=r[P3@P4]
#
# If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
# use the value in register P3 as a key. If cursor P1 refers 
# to an SQL index, then P3 is the first in an array of P4 registers 
# that are used as an unpacked index key. 
#
# Reposition cursor P1 so that  it points to the smallest entry that 
# is greater than the key value. If there are no records greater than 
# the key and P2 is not zero, then jump to P2.
#
# See also: Found, NotFound, Distinct, SeekLt, SeekGe, SeekLe
#
# Opcode: SeekLt P1 P2 P3 P4 * 
# Synopsis: key=r[P3@P4]
#
# If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
# use the value in register P3 as a key. If cursor P1 refers 
# to an SQL index, then P3 is the first in an array of P4 registers 
# that are used as an unpacked index key. 
#
# Reposition cursor P1 so that  it points to the largest entry that 
# is less than the key value. If there are no records less than 
# the key and P2 is not zero, then jump to P2.
#
# See also: Found, NotFound, Distinct, SeekGt, SeekGe, SeekLe
#
# Opcode: SeekLe P1 P2 P3 P4 *
# Synopsis: key=r[P3@P4]
#
# If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
# use the value in register P3 as a key. If cursor P1 refers 
# to an SQL index, then P3 is the first in an array of P4 registers 
# that are used as an unpacked index key. 
#
# Reposition cursor P1 so that it points to the largest entry that 
# is less than or equal to the key value. If there are no records 
# less than or equal to the key and P2 is not zero, then jump to P2.
#
# See also: Found, NotFound, Distinct, SeekGt, SeekGe, SeekLt

def python_OP_SeekLT_SeekLE_SeekGE_SeekGT(hlquery, pc, rc, op):
    p = hlquery.p
    db = hlquery.db

    assert op.p_Signed(1) >= 0 and op.p_Signed(1) < rffi.getintfield(p, "nCursor")
    assert op.p_Signed(2) != 0
    pC = p.apCsr[op.p_Unsigned(1)]
    assert pC
    assert rffi.getintfield(pC, "pseudoTableReg") == 0
    assert CConfig.OP_SeekLE == CConfig.OP_SeekLT + 1
    assert CConfig.OP_SeekGE == CConfig.OP_SeekLT + 2
    assert CConfig.OP_SeekGT == CConfig.OP_SeekLT + 3
    assert get_isOrdered(pC)
    assert pC.pCursor
    oc = op.get_opcode()
    rffi.setintfield(pC, 'nullRow', 0)
    if get_isTable(pC):
        # The input value in P3 might be of any type: integer, real, string,
        # blob, or NULL.  But it needs to be an integer before we can do
        # the seek, so covert it.
        pIn3, flags3 = op.mem_and_flags_of_p(3)
        pIn3.applyNumericAffinity()
        iKey = pIn3.sqlite3VdbeIntValue()
        pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)

        # If the P3 value could not be converted into an integer without
        # loss of information, then special processing is required...
        if flags3 & CConfig.MEM_Int == 0:
            if flags3 & CConfig.MEM_Real == 0:
                # If the P3 value cannot be converted into any kind of a number,
                # then the seek is not possible, so jump to P2
                pc = op.p2as_pc()

                # VdbeBranchTaken() is used for test suite validation only and 
                # does not appear an production builds.
                # See vdbe.c lines 110-136.
                # VdbeBranchTaken(1,2);
                
                return pc, rc

            # If the approximation iKey is larger than the actual real search
            # term, substitute >= for > and < for <=. e.g. if the search term
            # is 4.9 and the integer approximation 5:
            #
            #        (x >  4.9)    ->     (x >= 5)
            #        (x <= 4.9)    ->     (x <  5)
            #
            if pIn3.get_u_r() < iKey:
                assert CConfig.OP_SeekGE == CConfig.OP_SeekGT - 1
                assert CConfig.OP_SeekLT == CConfig.OP_SeekLE - 1
                assert (CConfig.OP_SeekLE & 0x0001) == (CConfig.OP_SeekGT & 0x0001)
                if (oc & 0x0001) == (CConfig.OP_SeekGT & 0x0001):
                    oc -= 1

            # If the approximation iKey is smaller than the actual real search
            # term, substitute <= for < and > for >=.
            elif pIn3.get_u_r() > iKey:
                assert CConfig.OP_SeekLE == CConfig.OP_SeekLT + 1
                assert CConfig.OP_SeekGT == CConfig.OP_SeekGE + 1
                assert (CConfig.OP_SeekLT & 0x0001) == (CConfig.OP_SeekGE & 0x0001)
                if (oc & 0x0001) == (CConfig.OP_SeekLT & 0x0001):
                    oc += 1


        rc = capi.sqlite3BtreeMovetoUnpacked(pC.pCursor, lltype.nullptr(rffi.VOIDP.TO), iKey, 0, hlquery.intp)
        res = rffi.cast(lltype.Signed, hlquery.intp[0])
        if rc != CConfig.SQLITE_OK:
            # goto abort_due_to_error;
            print "In python_OP_SeekLT_SeekLE_SeekGE_SeekGT():1: abort_due_to_error."
            rc = capi.gotoAbortDueToError(p, db, pc, rc)
            return pc, rc
    else:
        r = hlquery.unpackedrecordp
        nField = rffi.cast(lltype.Signed, op.p4_i())
        assert op.p4type() == CConfig.P4_INT32
        assert nField > 0
        r.pKeyInfo = pC.pKeyInfo
        r.nField = rffi.cast(CConfig.u16, op.p4_i())

        # The next line of code computes as follows, only faster:
        #   if( oc==OP_SeekGT || oc==OP_SeekLE ){
        #     r.default_rc = -1;
        #   }else{
        #     r.default_rc = +1;
        #   }
        r.default_rc = rffi.cast(CConfig.i8, -1 if (1 & (oc - CConfig.OP_SeekLT)) else 1)
        assert oc != CConfig.OP_SeekGT or rffi.getintfield(r, "default_rc") == -1
        assert oc != CConfig.OP_SeekLE or rffi.getintfield(r, "default_rc") == -1
        assert oc != CConfig.OP_SeekGE or rffi.getintfield(r, "default_rc") == 1
        assert oc != CConfig.OP_SeekLT or rffi.getintfield(r, "default_rc") == 1

        r.aMem = op.mem_of_p(3).pMem
        op.mem_of_p(3).ExpandBlob()
        rc = capi.sqlite3BtreeMovetoUnpacked(pC.pCursor, rffi.cast(rffi.VOIDP, r), 0, 0, hlquery.intp)
        res = rffi.cast(lltype.Signed, hlquery.intp[0])
        if rc != CConfig.SQLITE_OK:
            # goto abort_due_to_error;
            print "In python_OP_SeekLT_SeekLE_SeekGE_SeekGT():2: abort_due_to_error."
            rc = capi.gotoAbortDueToError(p, db, pc, rc)
            return pc, rc
        pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)

    pC.deferredMoveto = rffi.cast(lltype.typeOf(pC.deferredMoveto), 0)
    pC.cacheStatus = rffi.cast(lltype.typeOf(pC.cacheStatus), CConfig.CACHE_STALE)
    if oc >= CConfig.OP_SeekGE:
        assert oc == CConfig.OP_SeekGE or oc == CConfig.OP_SeekGT
        if res < 0 or (res == 0 and oc == CConfig.OP_SeekGT):
            res = 0
            rc, resRet = sqlite3BtreeNext(hlquery, pC.pCursor, res)
            res = rffi.cast(lltype.Signed, resRet)
            if rc != CConfig.SQLITE_OK:
                # goto abort_due_to_error;
                print "In python_OP_SeekLT_SeekLE_SeekGE_SeekGT():3: abort_due_to_error."
                rc = capi.gotoAbortDueToError(p, db, pc, rc)
                return pc, rc
            pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)
        else:
            res = 0
    else:
        assert oc == CConfig.OP_SeekLT or oc == CConfig.OP_SeekLE
        if res > 0 or (res == 0 and oc == CConfig.OP_SeekLT):
            res = 0
            rc, resRet = sqlite3BtreePrevious(hlquery, pC.pCursor, res)
            res = rffi.cast(lltype.Signed, resRet)
            if rc != CConfig.SQLITE_OK:
                # goto abort_due_to_error;
                print "In python_OP_SeekLT_SeekLE_SeekGE_SeekGT():4: abort_due_to_error."
                rc = capi.gotoAbortDueToError(p, db, pc, rc)
                return pc, rc
            pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)
        else:
            # res might be negative because the table is empty.  Check to
            # see if this is the case.
            res = CConfig.CURSOR_VALID != rffi.getintfield(pC.pCursor, "eState")

    assert op.p_Signed(2) > 0

    # VdbeBranchTaken() is used for test suite validation only and 
    # does not appear an production builds.
    # See vdbe.c lines 110-136.
    # VdbeBranchTaken(res!=0,2);

    if res:
        pc = op.p2as_pc()

    return pc, rc


def get_isEphemeral(vdbecursor):
    return vdbecursor.scary_bitfield & 1

def get_useRandomRowid(vdbecursor):
    return vdbecursor.scary_bitfield & 2

def get_isTable(vdbecursor):
    return vdbecursor.scary_bitfield & 4

def get_isOrdered(vdbecursor):
    return vdbecursor.scary_bitfield & 8


# Opcode:  Yield P1 P2 * * *
#
# Swap the program counter with the value in register P1.
#
# If the co-routine ends with OP_Yield or OP_Return then continue
# to the next instruction.  But if the co-routine ends with
# OP_EndCoroutine, jump immediately to P2.

def python_OP_Yield(hlquery, pc, op):
    pIn1 = op.mem_of_p(1)
    assert pIn1.VdbeMemDynamic() == 0
    pIn1.set_flags(CConfig.MEM_Int)
    pcDest = pIn1.get_u_i()
    pIn1.set_u_i(pc) # XXX constant
    return pcDest


# Opcode: Null P1 P2 P3 * *
# Synopsis:  r[P2..P3]=NULL
#
# Write a NULL into registers P2.  If P3 greater than P2, then also write
# NULL into register P3 and every register in between P2 and P3.  If P3
# is less than P2 (typically P3 is zero) then only register P2 is
# set to NULL.
#
# If the P1 value is non-zero, then also set the MEM_Cleared flag so that
# NULL values will not compare equal even if SQLITE_NULLEQ is set on
# OP_Ne or OP_Eq.

@jit.unroll_safe
def python_OP_Null(hlquery, op):
    # out2-prerelease
    pOut = op.mem_of_p(2)
    if op.p_Signed(1):
        nullFlag = CConfig.MEM_Null | CConfig.MEM_Cleared
    else:
        nullFlag = CConfig.MEM_Null
    pOut.set_flags(nullFlag)
    index = op.p_Signed(2) + 1
    while index <= op.p_Signed(3):
        pOut = hlquery.mem_with_index(index)
        pOut.VdbeMemRelease()
        pOut.set_flags(nullFlag)
        index += 1


# Opcode: IfZero P1 P2 P3 * *
# Synopsis: r[P1]+=P3, if r[P1]==0 goto P2
#
# The register P1 must contain an integer.  Add literal P3 to the
# value in register P1.  If the result is exactly 0, jump to P2.
#
# It is illegal to use this instruction on a register that does
# not contain an integer.  An assertion fault will result if you try.

def python_OP_IfZero(hlquery, pc, op):
    pIn1 = op.mem_of_p(1)
    assert pIn1.get_flags() & CConfig.MEM_Int
    constant = pIn1.is_constant_u_i()
    i = pIn1.get_u_i() + op.p_Signed(3)
    pIn1.set_u_i(i, constant=constant)
    if i == 0:
        pc = op.p2as_pc()
    return pc

