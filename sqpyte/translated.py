from rpython.rlib import jit, rarithmetic
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

def sqlite3VdbeMemIntegerify(pMem):
    return capi.sqlite3_sqlite3VdbeMemIntegerify(pMem)

def sqlite3BtreeCursor(p, iTable, wrFlag, pKeyInfo, pCur):
    return capi.sqlite3_sqlite3BtreeCursor(p, iTable, wrFlag, pKeyInfo, pCur)

def sqlite3BtreeCursorHints(btCursor, mask):
    return capi.sqlite3_sqlite3BtreeCursorHints(btCursor, mask)

def sqlite3VdbeSorterRewind(db, pC, res):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as res:
        errorcode = capi.sqlite3_sqlite3VdbeSorterRewind(db, pC, res)
        return rffi.cast(rffi.INTP, res[0])

def applyAffinity(mem, affinity, enc):
    """
     Processing is determine by the affinity parameter:

     SQLITE_AFF_INTEGER:
     SQLITE_AFF_REAL:
     SQLITE_AFF_NUMERIC:
        Try to convert pRec to an integer representation or a
        floating-point representation if an integer representation
        is not possible.  Note that the integer representation is
        always preferred, even if the affinity is REAL, because
        an integer representation is more space efficient on disk.

     SQLITE_AFF_TEXT:
        Convert pRec to a text representation.

     SQLITE_AFF_NONE:
        No-op.  pRec is unchanged.
    """
    flags = rffi.cast(lltype.Unsigned, mem.flags)

    return _applyAffinity_flags_read(mem, flags, affinity, enc)

def _applyAffinity_flags_read(mem, flags, affinity, enc):
    assert isinstance(affinity, int)
    if affinity == CConfig.SQLITE_AFF_TEXT:
        # Only attempt the conversion to TEXT if there is an integer or real
        # representation (blob and NULL do not get converted) but no string
        # representation.

        if not (flags & CConfig.MEM_Str) and flags & (CConfig.MEM_Real|CConfig.MEM_Int):
            capi.sqlite3_sqlite3VdbeMemStringify(mem, enc)
        mem.flags = rffi.cast(CConfig.u16, mem.flags & ~(CConfig.MEM_Real|CConfig.MEM_Int))
    elif affinity != CConfig.SQLITE_AFF_NONE:
        assert affinity in (CConfig.SQLITE_AFF_INTEGER,
                            CConfig.SQLITE_AFF_REAL,
                            CConfig.SQLITE_AFF_NUMERIC)
        applyNumericAffinity(mem)
        if mem.flags & CConfig.MEM_Real:
            sqlite3VdbeIntegerAffinity(mem)

def sqlite3VdbeIntegerAffinity(mem):
    """
    The MEM structure is already a MEM_Real.  Try to also make it a
    MEM_Int if we can.
    """
    assert mem.flags & CConfig.MEM_Real
    assert not mem.flags & CConfig.MEM_RowSet
    # assert( mem->db==0 || sqlite3_mutex_held(mem->db->mutex) );
    # assert( EIGHT_BYTE_ALIGNMENT(mem) );
    floatval = mem.r
    intval = int(floatval)
    # Only mark the value as an integer if
    #
    #    (1) the round-trip conversion real->int->real is a no-op, and
    #    (2) The integer is neither the largest nor the smallest
    #        possible integer (ticket #3922)
    #
    # The second and third terms in the following conditional enforces
    # the second condition under the assumption that addition overflow causes
    # values to wrap around.
    if floatval == float(intval) and intval < sys.maxint and intval > (-sys.maxint - 1):
        mem.flags = rffi.cast(CConfig.u16, mem.flags | CConfig.MEM_Int)



def applyNumericAffinity(mem):
    """
    Try to convert a value into a numeric representation if we can
    do so without loss of information.  In other words, if the string
    looks like a number, convert it into a number.  If it does not
    look like a number, leave it alone.
    """
    if mem.flags & (CConfig.MEM_Real|CConfig.MEM_Int):
        return
    if not mem.flags & CConfig.MEM_Str:
        return
    # use the C function as a slow path for now
    return capi.sqlite3_applyNumericAffinity(mem)



def MemSetTypeFlag(mem, flag):
    mem.flags = rffi.cast(CConfig.u16, (mem.flags & ~(CConfig.MEM_TypeMask | CConfig.MEM_Zero)) | flag)

def ENC(db):
    return db.aDb[0].pSchema.enc

def python_OP_Init_translated(hlquery, pc, pOp):
    cond = rffi.cast(lltype.Bool, pOp.p2)
    p2 = rffi.cast(lltype.Signed, pOp.p2)
    if cond:
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

def python_OP_Goto_translated(hlquery, pc, rc, pOp):
    db = hlquery.db
    pc = hlquery.p2as_pc(pOp)

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


def python_OP_OpenRead_OpenWrite_translated(hlquery, db, pc, pOp):
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
        sqlite3VdbeMemIntegerify(pIn2)
        p2 = rffi.cast(rffi.INT, pIn2.u.i)
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

def python_OP_Next_translated(hlquery, pc, pOp):
    p = hlquery.p
    db = hlquery.db
    pcRet = pc
    p1 = hlquery.p_Signed(pOp, 1)
    p5 = hlquery.p_Unsigned(pOp, 5)
    assert p1 >= 0 and p1 < rffi.cast(lltype.Signed, p.nCursor)
    assert p5 < len(p.aCounter)
    pC = p.apCsr[p1]
    res = hlquery.p_Signed(pOp, 3)
    assert pC
    assert rffi.cast(lltype.Unsigned, pC.deferredMoveto) == 0
    assert pC.pCursor
    assert res == 0 or (res == 1 and rffi.cast(lltype.Signed, pC.isTable) == 0)
    
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
        pcRet = hlquery.p2as_pc(pOp)

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

def python_OP_NextIfOpen_translated(hlquery, pc, rc, pOp):
    p = hlquery.p
    db = hlquery.db
    p1 = rffi.cast(lltype.Signed, pOp.p1)
    if not p.apCsr[p1]:
        return pc, rc
    else:
        return python_OP_Next_translated(hlquery, pc, pOp)

def python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(hlquery, pc, rc, pOp):
    p = hlquery.p
    db = hlquery.db
    aMem = p.aMem           # /* Copy of p->aMem */
    pIn1, flags1 = hlquery.mem_and_flags_of_p(pOp, 1)    # 1st input operand
    pIn3, flags3 = hlquery.mem_and_flags_of_p(pOp, 3)    # 3st input operand
    flags1 = jit.promote(flags1)
    flags3 = jit.promote(flags3)
    opcode = hlquery.get_opcode(pOp)
    flags_can_have_changed = False
    p5 = hlquery.p_Unsigned(pOp, 5)


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
                pOut = aMem[pOp.p2]

                MemSetTypeFlag(pOut, CConfig.MEM_Null)

                # Used only for debugging, i.e., not in production.
                # See vdbe.c lines 451-455.
                # REGISTER_TRACE(pOp->p2, pOut);

            else:
                # VdbeBranchTaken() is used for test suite validation only and 
                # does not appear an production builds.
                # See vdbe.c lines 110-136.
                # VdbeBranchTaken(2,3);
                if p5 & CConfig.SQLITE_JUMPIFNULL:
                    pc = rffi.cast(lltype.Signed, pOp.p2) - 1
            return pc, rc
    else:

    ################################ MODIFIED BLOCK STARTS ################################

        if (flags1 | flags3) & (CConfig.MEM_Int | CConfig.MEM_Real):
            # both are ints
            if flags1 & flags3 & CConfig.MEM_Int:
                i1 = pIn1.u.i
                i3 = pIn3.u.i
                if i1 > i3:
                    res = -1
                elif i1 < i3:
                    res = 1
                else:
                    res = 0
            else:
                # mixed int and real comparison, convert to real
                n1 = pIn1.u.i if flags1 & CConfig.MEM_Int else pIn1.r
                n3 = pIn3.u.i if flags3 & CConfig.MEM_Int else pIn3.r

                if n1 > n3:
                    res = -1
                elif n1 < n3:
                    res = 1
                else:
                    res = 0
        else:
            flags_can_have_changed = True

            # Call C functions
            # /* Neither operand is NULL.  Do a comparison. */
            affinity = rffi.cast(lltype.Signed, p5 & CConfig.SQLITE_AFF_MASK)
            if affinity != 0:
                encoding = ENC(db)
                _applyAffinity_flags_read(pIn1, flags1, affinity, encoding)
                _applyAffinity_flags_read(pIn3, flags3, affinity, encoding)
                if rffi.cast(lltype.Unsigned, db.mallocFailed) != 0:
                    # goto no_mem;
                    print 'In python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(): no_mem.'
                    rc = capi.sqlite3_gotoNoMem(p, db, pc)
                    return pc, rc

            assert hlquery.p4type(pOp) == CConfig.P4_COLLSEQ or not pOp.p4.pColl
            # ExpandBlob() is used if SQLITE_OMIT_INCRBLOB is *not* defined.
            # SQLITE_OMIT_INCRBLOB doesn't appear to be defined in production.
            # See vdbeInt.h lines 475-481.
            #   ExpandBlob(pIn1);
            #   ExpandBlob(pIn3);

            res = capi.sqlite3_sqlite3MemCompare(pIn3, pIn1, pOp.p4.pColl)

    ################################# MODIFIED BLOCK ENDS #################################

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

    if p5 & CConfig.SQLITE_STOREP2:
        pOut = aMem[pOp.p2]
        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 24-37.
        #   memAboutToChange(p, pOut);

        MemSetTypeFlag(pOut, CConfig.MEM_Int)

        pOut.u.i = res

        # Used only for debugging, i.e., not in production.
        # See vdbe.c lines 451-455.
        # REGISTER_TRACE(pOp->p2, pOut);
    else:
        # VdbeBranchTaken() is used for test suite validation only and 
        # does not appear an production builds.
        # See vdbe.c lines 110-136.
        # VdbeBranchTaken(res!=0, (pOp->p5 & SQLITE_NULLEQ)?2:3);

        if res != 0:
            pc = hlquery.p2as_pc(pOp)

    if flags_can_have_changed:
        # /* Undo any changes made by applyAffinity() to the input registers. */
        pIn1.flags = rffi.cast(CConfig.u16, (pIn1.flags & ~CConfig.MEM_TypeMask) | (flags1 & CConfig.MEM_TypeMask))
        pIn3.flags = rffi.cast(CConfig.u16, (pIn3.flags & ~CConfig.MEM_TypeMask) | (flags3 & CConfig.MEM_TypeMask))

    return pc, rc

def python_OP_Noop_Explain_translated(pOp):
    opcode = rffi.cast(lltype.Unsigned, pOp.opcode)
    assert opcode == CConfig.OP_Noop or opcode == CConfig.OP_Explain


# Opcode: IsNull P1 P2 * * *
# Synopsis:  if r[P1]==NULL goto P2
#
# Jump to P2 if the value in register P1 is NULL.

def python_OP_IsNull(hlquery, pc, pOp):
    pIn1, flags1 = hlquery.mem_and_flags_of_p(pOp, 1)
    if flags1 & CConfig.MEM_Null != 0:
        pc = hlquery.p2as_pc(pOp)
    return pc


# Opcode: NotNull P1 P2 * * *
# Synopsis: if r[P1]!=NULL goto P2
#
# Jump to P2 if the value in register P1 is not NULL.  

def python_OP_NotNull(hlquery, pc, pOp):
    pIn1, flags1 = hlquery.mem_and_flags_of_p(pOp, 1)
    if flags1 & CConfig.MEM_Null == 0:
        pc = hlquery.p2as_pc(pOp)
    return pc


# Opcode: Once P1 P2 * * *
#
# Check if OP_Once flag P1 is set. If so, jump to instruction P2. Otherwise,
# set the flag and fall through to the next instruction.  In other words,
# this opcode causes all following opcodes up through P2 (but not including
# P2) to run just once and to be skipped on subsequent times through the loop.

def python_OP_Once(hlquery, pc, pOp):
    p = hlquery.p
    p1 = hlquery.p_Signed(pOp, 1)
    assert p1 < rffi.cast(lltype.Signed, p.nOnceFlag)
    if rffi.cast(lltype.Signed, p.aOnceFlag[p1]):
        pc = hlquery.p2as_pc(pOp)
    else:
        p.aOnceFlag[p1] = rffi.cast(CConfig.u8, 1)
    return pc


def python_OP_MustBeInt(hlquery, pc, rc, pOp):
    # not a full translation, only translate the fast path where the argument
    # is already an int
    pIn1, flags1 = hlquery.mem_and_flags_of_p(pOp, 1)
    if not flags1 & CConfig.MEM_Int:
        hlquery.internalPc[0] = rffi.cast(rffi.LONG, pc)
        rc = capi.impl_OP_MustBeInt(hlquery.p, hlquery.db, hlquery.internalPc, rc, pOp)
        retPc = hlquery.internalPc[0]
        return retPc, rc
    MemSetTypeFlag(pIn1, CConfig.MEM_Int)
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
def python_OP_Affinity(hlquery, pOp):
    zAffinity = hlquery.p4_z(pOp) # The affinity to be applied
    encoding = ENC(hlquery.db)
    index = hlquery.p_Signed(pOp, 1)
    length =  hlquery.p_Signed(pOp, 2)
    assert len(zAffinity) == length
    for i in range(length):
        applyAffinity(hlquery.p.aMem[index], ord(zAffinity[i]), encoding)
        index += 1


# Opcode: RealAffinity P1 * * * *
#
# If register P1 holds an integer convert it to a real value.
#
# This opcode is used when extracting information from a column that
# has REAL affinity.  Such column values may still be stored as
# integers, for space efficiency, but after extraction we want them
# to have only a real value.

def python_OP_RealAffinity(hlquery, pOp):
    pIn1, flags = hlquery.mem_and_flags_of_p(pOp, 1)
    if flags & CConfig.MEM_Int and not flags & CConfig.MEM_Real:
        # only relevant parts of sqlite3VdbeMemRealify
        pIn1.r = float(pIn1.u.i)
        MemSetTypeFlag(pIn1, CConfig.MEM_Real)


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

def python_OP_If_IfNot(hlquery, pc, pOp):
    p = hlquery.p
    pIn1, flags1 = hlquery.mem_and_flags_of_p(pOp, 1)    # 1st input operand
    opcode = hlquery.get_opcode(pOp)

    if flags1 & CConfig.MEM_Null:
        c = pOp.p3
    else:
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        # #ifdef SQLITE_OMIT_FLOATING_POINT
        #     c = sqlite3VdbeIntValue(pIn1)!=0;
        # #else
        c = _sqlite3VdbeRealValue_flags(pIn1, flags1) != 0.0
        # #endif
        if opcode == CConfig.OP_IfNot:
            c = not c
    # VdbeBranchTaken() is used for test suite validation only and 
    # does not appear an production builds.
    # See vdbe.c lines 110-136.
    # VdbeBranchTaken(c!=0, 2);
    if rffi.cast(lltype.Unsigned, c):
        pc = rffi.cast(lltype.Signed, pOp.p2) - 1

    return pc


# Return the best representation of pMem that we can get into a
# double.  If pMem is already a double or an integer, return its
# value.  If it is a string or blob, try to convert it to a double.
# If it is a NULL, return 0.0.

def sqlite3VdbeRealValue(pMem):
    return _sqlite3VdbeRealValue_flags(pMem, flags)

def _sqlite3VdbeRealValue_flags(pMem, flags):
    # assert( pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex) );
    # assert( EIGHT_BYTE_ALIGNMENT(pMem) );
    if flags & CConfig.MEM_Real:
        return pMem.r
    elif flags & CConfig.MEM_Int:
        return pMem.u.i
    elif flags & (CConfig.MEM_Str | CConfig.MEM_Blob):
        val = lltype.malloc(rffi.DOUBLEP.TO, 1, flavor='raw')
        val[0] = 0.0
        capi.sqlite3AtoF(pMem.z, val, pMem.n, pMem.enc)
        ret = val[0]
        lltype.free(val, flavor='raw')
        return ret
    else:
        return 0.0


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

def python_OP_Add_Subtract_Multiply_Divide_Remainder(hlquery, pOp):
    p = hlquery.p
    opcode = hlquery.get_opcode(pOp)

    aMem = p.aMem
    pIn1, flags1 = hlquery.mem_and_flags_of_p(pOp, 1)    # 1st input operand
    jit.promote(flags1)
    type1 = _numericType_with_flags(pIn1, flags1)
    pIn2, flags2 = hlquery.mem_and_flags_of_p(pOp, 2)    # 1st input operand
    jit.promote(flags2)
    type2 = _numericType_with_flags(pIn2, flags2)
    pOut = hlquery.mem_of_p(pOp, 3)
    flags = flags1 | flags2

    if flags & CConfig.MEM_Null != 0:
        capi.sqlite3VdbeMemSetNull(pOut)
        return

    bIntint = False
    if opcode == CConfig.OP_Add:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.u.i
            iB = pIn2.u.i
            try:
                iB = rarithmetic.ovfcheck(iA + iB)
            except OverflowError:
                pass
            else:
                pOut.u.i = iB
                MemSetTypeFlag(pOut, CConfig.MEM_Int)
                return
            bIntint = True
        rA = _sqlite3VdbeRealValue_flags(pIn1, flags1)
        rB = _sqlite3VdbeRealValue_flags(pIn2, flags2)
        rB += rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            capi.sqlite3VdbeMemSetNull(pOut)
            return
        pOut.r = float(rB)
        MemSetTypeFlag(pOut, CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            sqlite3VdbeIntegerAffinity(pOut)
        #endif                
        return
    elif opcode == CConfig.OP_Subtract:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.u.i
            iB = pIn2.u.i
            try:
                iB = rarithmetic.ovfcheck(iB - iA)
            except OverflowError:
                pass
            else:
                pOut.u.i = iB
                MemSetTypeFlag(pOut, CConfig.MEM_Int)
                return
            bIntint = True
        rA = _sqlite3VdbeRealValue_flags(pIn1, flags1)
        rB = _sqlite3VdbeRealValue_flags(pIn2, flags2)
        rB -= rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            capi.sqlite3VdbeMemSetNull(pOut)
            return
        pOut.r = float(rB)
        MemSetTypeFlag(pOut, CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            sqlite3VdbeIntegerAffinity(pOut)
        #endif                
        return
    elif opcode == CConfig.OP_Multiply:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.u.i
            iB = pIn2.u.i
            try:
                iB = rarithmetic.ovfcheck(iA * iB)
            except OverflowError:
                pass
            else:
                pOut.u.i = iB
                MemSetTypeFlag(pOut, CConfig.MEM_Int)
                return
            bIntint = True
        rA = _sqlite3VdbeRealValue_flags(pIn1, flags1)
        rB = _sqlite3VdbeRealValue_flags(pIn2, flags2)
        rB *= rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            capi.sqlite3VdbeMemSetNull(pOut)
            return
        pOut.r = float(rB)
        MemSetTypeFlag(pOut, CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            sqlite3VdbeIntegerAffinity(pOut)
        #endif                
        return
    elif opcode == CConfig.OP_Divide:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.u.i
            iB = pIn2.u.i
            if iA == 0:
                capi.sqlite3VdbeMemSetNull(pOut)
                return
            try:
                iB = rarithmetic.ovfcheck(iB / iA) # XXX how's the rounding behaviour?
            except OverflowError:
                pass
            else:
                pOut.u.i = iB
                MemSetTypeFlag(pOut, CConfig.MEM_Int)
                return
            bIntint = True
        rA = _sqlite3VdbeRealValue_flags(pIn1, flags1)
        rB = _sqlite3VdbeRealValue_flags(pIn2, flags2)
        # /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
        if rA == 0.0:
            capi.sqlite3VdbeMemSetNull(pOut)
            return
        rB /= rA
        # SQLITE_OMIT_FLOATING_POINT is not defined.
        #ifdef SQLITE_OMIT_FLOATING_POINT
        # pOut->u.i = rB;
        # MemSetTypeFlag(pOut, MEM_Int);
        #else
        # if( sqlite3IsNaN(rB) ){
        if math.isnan(rB):
            capi.sqlite3VdbeMemSetNull(pOut)
            return
        pOut.r = float(rB)
        MemSetTypeFlag(pOut, CConfig.MEM_Real)
        if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
            sqlite3VdbeIntegerAffinity(pOut)
        #endif                
        return
    elif opcode == CConfig.OP_Remainder:
        if (type1 & type2 & CConfig.MEM_Int) != 0:
            iA = pIn1.u.i
            iB = pIn2.u.i
            bIntint = 1
            if iA == 0:
                capi.sqlite3VdbeMemSetNull(pOut)
                return
            if iA == -1:
                iA = 1
            iB %= iA
            pOut.u.i = iB
            MemSetTypeFlag(pOut, CConfig.MEM_Int)
            return
        else:
            bIntint = 0
            rA = _sqlite3VdbeRealValue_flags(pIn1, flags1)
            rB = _sqlite3VdbeRealValue_flags(pIn2, flags2)
            iA = int(rA)
            iB = int(rB)
            if iA == 0:
                capi.sqlite3VdbeMemSetNull(pOut)
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
                capi.sqlite3VdbeMemSetNull(pOut)
                return
            pOut.r = float(rB)
            MemSetTypeFlag(pOut, CConfig.MEM_Real)
            if ((type1 | type2) & CConfig.MEM_Real) == 0 and not bIntint:
                sqlite3VdbeIntegerAffinity(pOut)
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

def numericType(pMem):
    return _numericType_with_flags(pMem, pMem.flags)

def _numericType_with_flags(pMem, flags):
    if flags & (CConfig.MEM_Int | CConfig.MEM_Real):
        return flags & (CConfig.MEM_Int | CConfig.MEM_Real)
    if flags & (CConfig.MEM_Str | CConfig.MEM_Blob):
        val1 = lltype.malloc(rffi.DOUBLEP.TO, 1, flavor='raw')
        val1[0] = 0.0
        atof = capi.sqlite3AtoF(pMem.z, val1, pMem.n, pMem.enc)
        pMem.r = val1[0]
        lltype.free(val1, flavor='raw')

        if atof == 0:
            return 0

        val2 = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor='raw')
        val2[0] = 0
        atoi64 = capi.sqlite3Atoi64(pMem.z, val2, pMem.n, pMem.enc)
        pMem.u.i = val2[0]
        lltype.free(val2, flavor='raw')

        if atoi64 == CConfig.SQLITE_OK:
            return CConfig.MEM_Int

        return CConfig.MEM_Real
    return 0

