from rpython.rlib import jit, rarithmetic
from rpython.rlib.objectmodel import specialize
from rpython.rtyper.lltypesystem import rffi
from capi import CConfig
from rpython.rtyper.lltypesystem import lltype
import capi
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
    pIn1, flags1 = op.mem_and_flags_of_p(1, promote=True)    # 1st input operand
    pIn3, flags3 = op.mem_and_flags_of_p(3, promote=True)    # 3st input operand
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
                n1 = pIn1.get_u_i() if flags1 & CConfig.MEM_Int else pIn1.get_r()
                n3 = pIn3.get_u_i() if flags3 & CConfig.MEM_Int else pIn3.get_r()

                res = int(_cmp_depending_on_opcode(opcode, n3, n1))
        else:
            flags_can_have_changed = True

            # Call C functions
            # /* Neither operand is NULL.  Do a comparison. */
            affinity = rffi.cast(lltype.Signed, p5 & CConfig.SQLITE_AFF_MASK)
            if affinity != 0:
                encoding = hlquery.enc()
                flags1 = pIn1._applyAffinity_flags_read(flags1, affinity, encoding)
                flags3 = pIn3._applyAffinity_flags_read(flags3, affinity, encoding)
                if rffi.cast(lltype.Unsigned, db.mallocFailed) != 0:
                    # goto no_mem;
                    print 'In python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(): no_mem.'
                    rc = capi.sqlite3_gotoNoMem(p, db, pc)
                    return pc, rc

            assert op.p4type() == CConfig.P4_COLLSEQ or not op.p4_pColl()
            # ExpandBlob() is used if SQLITE_OMIT_INCRBLOB is *not* defined.
            # SQLITE_OMIT_INCRBLOB doesn't appear to be defined in production.
            # See vdbeInt.h lines 475-481.
            #   ExpandBlob(pIn1);
            #   ExpandBlob(pIn3);

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
    pIn1, flags1 = op.mem_and_flags_of_p(1, promote=True)
    if not flags1 & CConfig.MEM_Int:
        hlquery.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_MustBeInt(hlquery.p, hlquery.db, hlquery.internalPc, rc, op.pOp)
        retPc = hlquery.internalPc[0]
        return retPc, rc
    pIn1._MemSetTypeFlag_flags(flags1, CConfig.MEM_Int)
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
    pIn1, flags = op.mem_and_flags_of_p(1, promote=True)
    if flags & CConfig.MEM_Int and not flags & CConfig.MEM_Real:
        # only relevant parts of sqlite3VdbeMemRealify
        pIn1.set_r(float(pIn1.get_u_i()))
        pIn1._MemSetTypeFlag_flags(flags, CConfig.MEM_Real)


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
    pIn1, flags1 = op.mem_and_flags_of_p(1, promote=True)    # 1st input operand
    opcode = op.get_opcode()

    if flags1 & CConfig.MEM_Null:
        c = op.p_Signed(3)
    else:
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        # #ifdef SQLITE_OMIT_FLOATING_POINT
        #     c = sqlite3VdbeIntValue(pIn1)!=0;
        # #else
        c = pIn1._sqlite3VdbeRealValue_flags(flags1) != 0.0
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

    pIn1, flags1 = op.mem_and_flags_of_p(1, promote=True)    # 1st input operand
    type1 = pIn1._numericType_with_flags(flags1)
    pIn2, flags2 = op.mem_and_flags_of_p(2, promote=True)    # 1st input operand
    type2 = pIn2._numericType_with_flags(flags2)
    pOut = op.mem_of_p(3)
    flags = flags1 | flags2

    if flags & CConfig.MEM_Null != 0:
        pOut.sqlite3VdbeMemSetNull()
        return

    bIntint = False
    if opcode == CConfig.OP_Add:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.get_u_i()
            iB = pIn2.get_u_i()
            try:
                iB = rarithmetic.ovfcheck(iA + iB)
            except OverflowError:
                pass
            else:
                pOut.set_u_i(iB)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1._sqlite3VdbeRealValue_flags(flags1)
        rB = pIn2._sqlite3VdbeRealValue_flags(flags2)
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
        pOut.set_r(float(rB))
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
                pOut.set_u_i(iB)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1._sqlite3VdbeRealValue_flags(flags1)
        rB = pIn2._sqlite3VdbeRealValue_flags(flags2)
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
        pOut.set_r(float(rB))
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
                pOut.set_u_i(iB)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1._sqlite3VdbeRealValue_flags(flags1)
        rB = pIn2._sqlite3VdbeRealValue_flags(flags2)
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
        pOut.set_r(float(rB))
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
                pOut.set_u_i(iB)
                pOut.MemSetTypeFlag(CConfig.MEM_Int)
                return
            bIntint = True
        rA = pIn1._sqlite3VdbeRealValue_flags(flags1)
        rB = pIn2._sqlite3VdbeRealValue_flags(flags2)
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
        pOut.set_r(float(rB))
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
            rA = pIn1._sqlite3VdbeRealValue_flags(flags1)
            rB = pIn2._sqlite3VdbeRealValue_flags(flags2)
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
            pOut.set_r(float(rB))
            pOut.MemSetTypeFlag(CConfig.MEM_Real)
            if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
                pOut.sqlite3VdbeIntegerAffinity()
            #endif                
            return;      
    else:
        print "Error: Unknown opcode %s in python_OP_Add_Subtract_Multiply_Divide_Remainder()." % opcode
    return



# Return the numeric type for pMem, either MEM_Int or MEM_Real or both or
# none.  
#
# Unlike applyNumericAffinity(), this routine does not modify pMem->flags.
# But it does set pMem->r and pMem->u.i appropriately.



# Opcode: Integer P1 P2 * * *
# Synopsis: r[P2]=P1
#
# The 32-bit integer value P1 is written into register P2.

def python_OP_Integer(hlquery, op):
    pOut = op.mem_of_p(2)
    pOut.set_u_i(op.p_Signed(1))


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
    pC.lastRowid = iKey
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
        print "In impl_OP_IdxRowid():1: abort_due_to_error."
        rc = capi.gotoAbortDueToError(p, db, pc, rc)
        return rc
    #assert pC.deferredMoveto == 0
    #assert pC.isTable == 0
    if not rffi.cast(lltype.Signed, pC.nullRow):
        rc = capi.sqlite3VdbeIdxRowid(db, pCrsr, hlquery.longp)
        if rc != CConfig.SQLITE_OK:
            print "In impl_OP_IdxRowid():2: abort_due_to_error."
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
    pIn2, flags = op.mem_and_flags_of_p(2, promote=True)
    pC.movetoTarget = pIn2._sqlite3VdbeIntValue_flags(flags)
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
    apVal = p.apArg
    assert apVal or n == 0
    index = op.p_Signed(2)
    for i in range(n):
        apVal[i] = hlquery.mem_with_index(index + i).pMem
    mem = op.mem_of_p(3)
    with lltype.scoped_alloc(capi.CONTEXT) as ctx:
        mems = Mem(hlquery, ctx.s)
        ctx.pFunc = pFunc = op.p4_pFunc()
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
            # XXX fix error handling
            rc = rffi.cast(lltype.Signed, ctx.isError)
        if rffi.getintfield(ctx, 'skipFlag'):
            prevop = hlquery._hlops[pc - 1]
            assert prevop.get_opcode() == CConfig.OP_CollSeq
            i = prevop.p_Signed(1)
            if i:
                hlquery.mem_with_index(i).sqlite3VdbeMemSetInt64(1)
        mems.sqlite3VdbeMemRelease()
        return rc


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
    rc = capi.sqlite3VdbeIdxKeyCompare(pC, r, resMem)
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

# Opcode:  Return P1 * * * *
#
# Jump to the next instruction after the address in register P1.  After
# the jump, register P1 becomes undefined.

def python_OP_Return(hlquery, op):
    p = hlquery.p
    pIn1, flags1 = op.mem_and_flags_of_p(1, promote=True)    # 1st input operand
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
    from sqpyte.mem import Mem
    assert Mem.VdbeMemDynamic(pIn1) == 0
    
    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 24-37.
    # memAboutToChange(p, pIn1);
    
    pIn1.set_flags(CConfig.MEM_Int)
    pIn1.set_u_i(pc)

    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 451-455.
    # REGISTER_TRACE(pOp->p1, pIn1);

    pc = op.p_Signed(2) - 1

    return pc

