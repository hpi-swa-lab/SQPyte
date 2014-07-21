from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi
from capi import CConfig
from rpython.rtyper.lltypesystem import lltype
import capi

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

def python_OP_Goto_translated(hlquery, db, pc, rc, pOp):
    p2 = hlquery.p_Signed(pOp, 2)
    pc = p2 - 1

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

def python_OP_Next_translated(hlquery, db, pc, pOp):
    p = hlquery.p
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
        pcRet = hlquery.p_Signed(pOp, 2) - 1

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

def python_OP_NextIfOpen_translated(hlquery, db, pc, rc, pOp):
    p = hlquery.p
    p1 = rffi.cast(lltype.Signed, pOp.p1)
    if not p.apCsr[p1]:
        return pc, rc
    else:
        return python_OP_Next_translated(hlquery, db, pc, pOp)

def python_OP_Ne_Eq_Gt_Le_Lt_Ge_translated(hlquery, db, pc, rc, pOp):
    p = hlquery.p
    aMem = p.aMem           # /* Copy of p->aMem */
    pIn1 = aMem[hlquery.p_Signed(pOp, 1)]     # /* 1st input operand */
    pIn3 = aMem[hlquery.p_Signed(pOp, 3)]     # /* 3rd input operand */
    flags1 = rffi.cast(lltype.Unsigned, pIn1.flags)
    flags3 = rffi.cast(lltype.Unsigned, pIn3.flags)
    opcode = hlquery.get_opcode(pOp)
    mem_int = rffi.cast(lltype.Unsigned, CConfig.MEM_Int)
    mem_real = rffi.cast(lltype.Unsigned, CConfig.MEM_Real)
    mem_null = rffi.cast(lltype.Unsigned, CConfig.MEM_Null)
    mem_cleared = rffi.cast(lltype.Unsigned, CConfig.MEM_Cleared)
    mem_zero = rffi.cast(lltype.Unsigned, CConfig.MEM_Zero)
    mem_typemask = rffi.cast(lltype.Unsigned, CConfig.MEM_TypeMask)
    p5 = hlquery.p_Unsigned(pOp, 5)


    if (flags1 | flags3) & mem_null:
        # /* One or both operands are NULL */
        if p5 & CConfig.SQLITE_NULLEQ:
            # /* If SQLITE_NULLEQ is set (which will only happen if the operator is
            #  ** OP_Eq or OP_Ne) then take the jump or not depending on whether
            #  ** or not both operands are null.
            #  */
            assert opcode == CConfig.OP_Eq or opcode == CConfig.OP_Ne
            assert flags1 & mem_cleared == 0
            assert p5 & CConfig.SQLITE_JUMPIFNULL == 0
            if (flags1 & mem_null != 0
                and flags3 & mem_null != 0
                and flags3 & mem_cleared == 0):
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

                # Translated: MemSetTypeFlag(pOut, MEM_Null);
                pOut.flags = rffi.cast(rffi.USHORT, (pOut.flags & ~(mem_typemask | mem_zero)) | mem_null)

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

        if (flags1 | flags3) & (mem_int | mem_real):
            n1 = pIn1.u.i if flags1 & mem_int else pIn1.r
            n3 = pIn3.u.i if flags3 & mem_int else pIn3.r

            if n1 > n3:
                res = -1
            elif n1 < n3:
                res = 1
            else:
                res = 0
        else:
            # Call C functions
            # /* Neither operand is NULL.  Do a comparison. */
            affinity = p5 & CConfig.SQLITE_AFF_MASK
            if affinity != 0:
                encoding = db.aDb[0].pSchema.enc
                capi.sqlite3_applyAffinity(pIn1, rffi.cast(rffi.CHAR, affinity), encoding)
                capi.sqlite3_applyAffinity(pIn3, rffi.cast(rffi.CHAR, affinity), encoding)
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

        # Translated: MemSetTypeFlag(pOut, MEM_Int);
        pOut.flags = rffi.cast(rffi.USHORT, (pOut.flags & ~(mem_typemask | mem_zero)) | mem_int)

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
            pc = hlquery.p_Signed(pOp, 2) - 1

    # /* Undo any changes made by applyAffinity() to the input registers. */
    pIn1.flags = rffi.cast(rffi.USHORT, (pIn1.flags & ~mem_typemask) | (flags1 & mem_typemask))
    pIn3.flags = rffi.cast(rffi.USHORT, (pIn3.flags & ~mem_typemask) | (flags3 & mem_typemask))

    return pc, rc

def python_OP_IsNull(hlquery, pc, pOp):
    pIn1 = hlquery.p.aMem[hlquery.p_Signed(pOp, 1)]
    flags1 = rffi.cast(lltype.Unsigned, pIn1.flags)
    mem_null = rffi.cast(lltype.Unsigned, CConfig.MEM_Null)
    if flags1 & mem_null != 0:
        pc = hlquery.p_Signed(pOp, 2) - 1
    return pc

def python_OP_Column_translated(hlquery, db, pc, pOp):
    p = hlquery.p
    aMem = p.aMem
    encoding = db.aDb[0].pSchema.enc
    p2 = pOp.p2
    assert pOp.p3 > 0 and pOp.p3 <= (p.nMem - p.nCursor)
    pDest = aMem[pOp.p3]

    # Used only for debugging, i.e., not in production.
    # See vdbe.c lines 24-37.
    # memAboutToChange(p, pDest);

    assert pOp.p1 >= 0 and pOp.p1 < p.nCursor
    pC = p.apCsr[pOp.p1]
    assert pC
    assert p2 < pC.nField
    aType = pC.aType
    # aOffset = aType + pC.nField   # <<< FIX
    aOffset = aType[pC.nField]

    # #ifndef SQLITE_OMIT_VIRTUALTABLE
    #   assert( pC->pVtabCursor==0 ); /* OP_Column never called on virtual table */
    # #endif

    pCrsr = pC.pCursor
    assert pCrsr != 0 or pC.pseudoTableReg > 0
    assert pCrsr != 0 or pC.nullRow

    # /* If the cursor cache is stale, bring it up-to-date */
    # rc = sqlite3VdbeCursorMoveto(pC);  <<< CALL INTO C

    if rc != 0:
        # goto abort_due_to_error;  <<< CALL INTO C
        print "In python_OP_Column_translated(): abort_due_to_error."
        assert False

    if pC.cacheStatus != p.cacheCtr or (pOp.p5 & CConfig.OPFLAG_CLEARCACHE) != 0:
        if pC.nullRow != 0:
            if pCrsr == 0:
                assert pC.pseudoTableReg > 0
                pReg = aMem[pC.pseudoTableReg]
                assert pReg.flags & MEM_Blob

                # Used only for debugging, i.e., not in production.
                # See vdbeInt.c lines 228-234.                
                # assert( memIsValid(pReg) );
                
                pC.payloadSize = pC.szRow = avail = pReg.n
                pC.aRow = pReg.z # pC->aRow = (u8*)pReg->z;
            else:
                # Translated: MemSetTypeFlag(pDest, MEM_Null);
                pDest.flags = rffi.cast(rffi.USHORT, (pDest.flags & ~(MEM_TypeMask | MEM_Zero)) | MEM_Null)

                # goto op_column_out;   <<< TRANSLATE
        else:
            assert pCrsr
            if pC.isTable == 0:
                # Translated: assert( sqlite3BtreeCursorIsValid(pCrsr) );
                # assert pCrsr and pCrsr->eState == CURSOR_VALID  <<< FIX

                # Used only in verification processes, i.e., not in production.
                # See sqliteInt.h 301-313.
                # VVA_ONLY(rc =) sqlite3BtreeKeySize(pCrsr, &payloadSize64);

                assert rc == CConfig.SQLITE_OK

                # assert( (payloadSize64 & SQLITE_MAX_U32)==(u64)payloadSize64 );
                # pC->aRow = sqlite3BtreeKeyFetch(pCrsr, &avail);
                pC.payloadSize = payloadSize # pC->payloadSize = (u32)payloadSize64;
            else:
                # assert( sqlite3BtreeCursorIsValid(pCrsr) );
                # VVA_ONLY(rc =) sqlite3BtreeDataSize(pCrsr, &pC->payloadSize);
                assert rc == CConfig.SQLITE_OK
                # pC->aRow = sqlite3BtreeDataFetch(pCrsr, &avail);
            assert avail <= 65536
            if pC.payloadSize <= avail:
                pC.szRow = pC.payloadSize
            else:
                pC.szRow = avail
            if pC.payloadSize > db.aLimit[CConfig.SQLITE_LIMIT_LENGTH]:
                # goto too_big;
                print "In python_OP_Column_translated(): too_big."
                assert False
                pass

        pass
        if pC.cacheStatus != p.cacheCtr or (pOp.p5 & CConfig.OPFLAG_CLEARCACHE) != 0:
            pC.cacheStatus = p.cacheCtr
            # pC->iHdrOffset = getVarint32(pC->aRow, offset);
            pC.nHdrParsed = 0
            aOffset[0] = offset
            if avail < offset:
                pC.aRow = 0
                pC.szRow = 0
            if offset > 98307 or offset > pC.payloadSize:
                rc = CConfig.SQLITE_CORRUPT_BKPT
                # UPDATE_MAX_BLOBSIZE(pDest);
                # REGISTER_TRACE(pOp->p3, pDest);
                return rc

    # end of if pC.cacheStatus != p.cacheCtr or (pOp.p5 & CConfig.OPFLAG_CLEARCACHE) != 0

    if pC.nHdrParsed <= p2:
        if pC.iHdrOffset < aOffset[0]:
            if pC.aRow == 0:
                # memset(&sMem, 0, sizeof(sMem));
                # rc = sqlite3VdbeMemFromBtree(pCrsr, 0, aOffset[0], !pC->isTable, &sMem);
                if rc != CConfig.SQLITE_OK:
                    # UPDATE_MAX_BLOBSIZE(pDest);
                    # REGISTER_TRACE(pOp->p3, pDest);
                    return rc
                zData = sMem.z # zData = (u8*)sMem.z;
            else:
                zData = pC.aRow

            i = pC.nHdrParsed
            offset = aOffset[i]
            zHdr = zData + pC.iHdrOffset
            zEndHdr = zData + aOffset[0]
            assert i <= p2 and zHdr < zEndHdr

            while i <= p2 and zHdr < zEndHdr:
                if zHdr[0] < 128: # if( zHdr[0]<0x80 ){
                    t = zHdr[0]
                    zHdr += 1
                else:
                    # zHdr += sqlite3GetVarint32(zHdr, &t);
                    pass
                aType[i] = t
                # szField = sqlite3VdbeSerialTypeLen(t);
                offset += szField
                if offset < szField:
                    zHdr = zEndHdr[1] # zHdr = &zEndHdr[1];  /* Forces SQLITE_CORRUPT return below */
                    break
                i += 1
                aOffset[i] = offset

            pC.nHdrParsed = i
            pC.iHdrOffset = zHdr - zData # pC->iHdrOffset = (u32)(zHdr - zData);
            if pC.aRow == 0:
                # sqlite3VdbeMemRelease(&sMem);
                # sMem.flags = CConfig.
                pass

            if zHdr > zEndHdr or offset > pC.payloadSize or (zHdr == zEndHdr and offset != pC.payloadSize):
                rc = CConfig.SQLITE_CORRUPT_BKPT
                # UPDATE_MAX_BLOBSIZE(pDest);
                # REGISTER_TRACE(pOp->p3, pDest);
                return rc

        # end of if pC.iHdrOffset < aOffset[0]

        if pC.nHdrParsed <= p2:
            if pOp.p4type == CConfig.P4_MEM:
                # sqlite3VdbeMemShallowCopy(pDest, pOp->p4.pMem, MEM_Static);
                pass
            else:
                # MemSetTypeFlag(pDest, MEM_Null);
                pass
            pass
            # // Translated Deephemeralize(pDest);
            # if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
            #     // goto no_mem;
            #     printf("In impl_OP_Column(): no_mem.\n");
            #     assert(0);
            # }

    # end of if pC.nHdrParsed <= p2

    assert p2 < pC.nHdrParsed
    assert rc == CConfig.SQLITE_OK
    # assert( sqlite3VdbeCheckMemInvariants(pDest) );  
    if pC.szRow >= aOffset[p2 + 1]:
        # VdbeMemRelease(pDest);
        # sqlite3VdbeSerialGet(pC->aRow+aOffset[p2], aType[p2], pDest);
        pass
    else:
        t = aType[p2]
        if (pOp.p5 & (CConfig.OPFLAG_LENGTHARG | CConfig.OPFLAG_TYPEOFARG)) != 0 and \
           ((t >= 12 and t & 1 == 0) or (pOp.p5 & CConfig.OPFLAG_TYPEOFARG) != 0):
        # if( ((pOp->p5 & (OPFLAG_LENGTHARG|OPFLAG_TYPEOFARG))!=0
        #       && ((t>=12 && (t&1)==0) || (pOp->p5 & OPFLAG_TYPEOFARG)!=0))
        #  || (len = sqlite3VdbeSerialTypeLen(t))==0
            zData = payloadSize64 if t <= 13 else 0 # zData = t<=13 ? (u8*)&payloadSize64 : 0;
            sMem.zMalloc = 0
        else:
            # memset(&sMem, 0, sizeof(sMem));
            # sqlite3VdbeMemMove(&sMem, pDest);
            # rc = sqlite3VdbeMemFromBtree(pCrsr, aOffset[p2], len, !pC->isTable, &sMem);
            if rc != CConfig.SQLITE_OK:
                # // Translated Deephemeralize(pDest);
                # if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
                #     // goto no_mem;
                #     printf("In impl_OP_Column(): no_mem.\n");
                #     assert(0);
                # }
                pass
            zData = sMem.z # zData = (u8*)sMem.z;
        # sqlite3VdbeSerialGet(zData, t, pDest);
        if sMem.zMalloc:
            assert sMem.z == sMem.zMalloc
            # assert( VdbeMemDynamic(pDest)==0 );
            # assert( (pDest->flags & (MEM_Blob|MEM_Str))==0 || pDest->z==sMem.z );
            # pDest->flags &= ~(MEM_Ephem|MEM_Static);
            # pDest->flags |= MEM_Term;
            pDest.z = sMem.z
            pDest.zMalloc = sMem.zMalloc
    pDest.enc = encoding

    # // Translated Deephemeralize(pDest);
    # if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
    #     // goto no_mem;
    #     printf("In impl_OP_Column(): no_mem.\n");
    #     assert(0);
    # }

    # UPDATE_MAX_BLOBSIZE(pDest);
    # REGISTER_TRACE(pOp->p3, pDest);

    return rc



# int impl_OP_Column(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
#   i64 payloadSize64; /* Number of bytes in the record */
#   int p2;            /* column number to retrieve */
#   VdbeCursor *pC;    /* The VDBE cursor */
#   BtCursor *pCrsr;   /* The BTree cursor */
#   u32 *aType;        /* aType[i] holds the numeric type of the i-th column */
#   u32 *aOffset;      /* aOffset[i] is offset to start of data for i-th column */
#   int len;           /* The length of the serialized data for the column */
#   int i;             /* Loop counter */
#   Mem *pDest;        /* Where to write the extracted value */
#   Mem sMem;          /* For storing the record being decoded */
#   const u8 *zData;   /* Part of the record being decoded */
#   const u8 *zHdr;    /* Next unparsed byte of the header */
#   const u8 *zEndHdr; /* Pointer to first byte after the header */
#   u32 offset;        /* Offset into the data */
#   u32 szField;       /* Number of bytes in the content of a field */
#   u32 avail;         /* Number of bytes of available data */
#   u32 t;             /* A type code from the record header */
#   Mem *pReg;         /* PseudoTable input register */
#   int rc;
#   Mem *aMem = p->aMem;
#   u8 encoding = ENC(db);     /* The database encoding */

#   p2 = pOp->p2;
#   assert( pOp->p3>0 && pOp->p3<=(p->nMem-p->nCursor) );
#   pDest = &aMem[pOp->p3];
#   memAboutToChange(p, pDest);
#   assert( pOp->p1>=0 && pOp->p1<p->nCursor );
#   pC = p->apCsr[pOp->p1];
#   assert( pC!=0 );
#   assert( p2<pC->nField );
#   aType = pC->aType;
#   aOffset = aType + pC->nField;
# #ifndef SQLITE_OMIT_VIRTUALTABLE
#   assert( pC->pVtabCursor==0 ); /* OP_Column never called on virtual table */
# #endif
#   pCrsr = pC->pCursor;
#   assert( pCrsr!=0 || pC->pseudoTableReg>0 ); /* pCrsr NULL on PseudoTables */
#   assert( pCrsr!=0 || pC->nullRow );          /* pC->nullRow on PseudoTables */

#   /* If the cursor cache is stale, bring it up-to-date */
#   rc = sqlite3VdbeCursorMoveto(pC);
#   if( rc ) {
#     // goto abort_due_to_error;
#       printf("In impl_OP_Column(): abort_due_to_error.\n");
#       assert(0);
#   }
#   if( pC->cacheStatus!=p->cacheCtr || (pOp->p5&OPFLAG_CLEARCACHE)!=0 ){
#     if( pC->nullRow ){
#       if( pCrsr==0 ){
#         assert( pC->pseudoTableReg>0 );
#         pReg = &aMem[pC->pseudoTableReg];
#         assert( pReg->flags & MEM_Blob );
#         assert( memIsValid(pReg) );
#         pC->payloadSize = pC->szRow = avail = pReg->n;
#         pC->aRow = (u8*)pReg->z;
#       }else{
#         MemSetTypeFlag(pDest, MEM_Null);
#         goto op_column_out;
#       }
#     }else{
#       assert( pCrsr );
#       if( pC->isTable==0 ){
#         assert( sqlite3BtreeCursorIsValid(pCrsr) );
#         VVA_ONLY(rc =) sqlite3BtreeKeySize(pCrsr, &payloadSize64);
#         assert( rc==SQLITE_OK ); /* True because of CursorMoveto() call above */
#         /* sqlite3BtreeParseCellPtr() uses getVarint32() to extract the
#         ** payload size, so it is impossible for payloadSize64 to be
#         ** larger than 32 bits. */
#         assert( (payloadSize64 & SQLITE_MAX_U32)==(u64)payloadSize64 );
#         pC->aRow = sqlite3BtreeKeyFetch(pCrsr, &avail);
#         pC->payloadSize = (u32)payloadSize64;
#       }else{
#         assert( sqlite3BtreeCursorIsValid(pCrsr) );
#         VVA_ONLY(rc =) sqlite3BtreeDataSize(pCrsr, &pC->payloadSize);
#         assert( rc==SQLITE_OK );   /* DataSize() cannot fail */
#         pC->aRow = sqlite3BtreeDataFetch(pCrsr, &avail);
#       }
#       assert( avail<=65536 );  /* Maximum page size is 64KiB */
#       if( pC->payloadSize <= (u32)avail ){
#         pC->szRow = pC->payloadSize;
#       }else{
#         pC->szRow = avail;
#       }
#       if( pC->payloadSize > (u32)db->aLimit[SQLITE_LIMIT_LENGTH] ){
#         // goto too_big;
#         printf("In impl_OP_Column(): too_big.\n");
#         assert(0);
#       }
#     }
#     pC->cacheStatus = p->cacheCtr;
#     pC->iHdrOffset = getVarint32(pC->aRow, offset);
#     pC->nHdrParsed = 0;
#     aOffset[0] = offset;
#     if( avail<offset ){
#       /* pC->aRow does not have to hold the entire row, but it does at least
#       ** need to cover the header of the record.  If pC->aRow does not contain
#       ** the complete header, then set it to zero, forcing the header to be
#       ** dynamically allocated. */
#       pC->aRow = 0;
#       pC->szRow = 0;
#     }

#     /* Make sure a corrupt database has not given us an oversize header.
#     ** Do this now to avoid an oversize memory allocation.
#     **
#     ** Type entries can be between 1 and 5 bytes each.  But 4 and 5 byte
#     ** types use so much data space that there can only be 4096 and 32 of
#     ** them, respectively.  So the maximum header length results from a
#     ** 3-byte type for each of the maximum of 32768 columns plus three
#     ** extra bytes for the header length itself.  32768*3 + 3 = 98307.
#     */
#     if( offset > 98307 || offset > pC->payloadSize ){
#       rc = SQLITE_CORRUPT_BKPT;
#       goto op_column_error;
#     }
#   }

#   /* Make sure at least the first p2+1 entries of the header have been
#   ** parsed and valid information is in aOffset[] and aType[].
#   */
#   if( pC->nHdrParsed<=p2 ){
#     /* If there is more header available for parsing in the record, try
#     ** to extract additional fields up through the p2+1-th field 
#     */
#     if( pC->iHdrOffset<aOffset[0] ){
#       /* Make sure zData points to enough of the record to cover the header. */
#       if( pC->aRow==0 ){
#         memset(&sMem, 0, sizeof(sMem));
#         rc = sqlite3VdbeMemFromBtree(pCrsr, 0, aOffset[0], 
#                                      !pC->isTable, &sMem);
#         if( rc!=SQLITE_OK ){
#           goto op_column_error;
#         }
#         zData = (u8*)sMem.z;
#       }else{
#         zData = pC->aRow;
#       }
  
#       /* Fill in aType[i] and aOffset[i] values through the p2-th field. */
#       i = pC->nHdrParsed;
#       offset = aOffset[i];
#       zHdr = zData + pC->iHdrOffset;
#       zEndHdr = zData + aOffset[0];
#       assert( i<=p2 && zHdr<zEndHdr );
#       do{
#         if( zHdr[0]<0x80 ){
#           t = zHdr[0];
#           zHdr++;
#         }else{
#           zHdr += sqlite3GetVarint32(zHdr, &t);
#         }
#         aType[i] = t;
#         szField = sqlite3VdbeSerialTypeLen(t);
#         offset += szField;
#         if( offset<szField ){  /* True if offset overflows */
#           zHdr = &zEndHdr[1];  /* Forces SQLITE_CORRUPT return below */
#           break;
#         }
#         i++;
#         aOffset[i] = offset;
#       }while( i<=p2 && zHdr<zEndHdr );
#       pC->nHdrParsed = i;
#       pC->iHdrOffset = (u32)(zHdr - zData);
#       if( pC->aRow==0 ){
#         sqlite3VdbeMemRelease(&sMem);
#         sMem.flags = MEM_Null;
#       }
  
#       /* If we have read more header data than was contained in the header,
#       ** or if the end of the last field appears to be past the end of the
#       ** record, or if the end of the last field appears to be before the end
#       ** of the record (when all fields present), then we must be dealing 
#       ** with a corrupt database.
#       */
#       if( (zHdr > zEndHdr)
#        || (offset > pC->payloadSize)
#        || (zHdr==zEndHdr && offset!=pC->payloadSize)
#       ){
#         rc = SQLITE_CORRUPT_BKPT;
#         goto op_column_error;
#       }
#     }

#     /* If after trying to extra new entries from the header, nHdrParsed is
#     ** still not up to p2, that means that the record has fewer than p2
#     ** columns.  So the result will be either the default value or a NULL.
#     */
#     if( pC->nHdrParsed<=p2 ){
#       if( pOp->p4type==P4_MEM ){
#         sqlite3VdbeMemShallowCopy(pDest, pOp->p4.pMem, MEM_Static);
#       }else{
#         MemSetTypeFlag(pDest, MEM_Null);
#       }
#       goto op_column_out;
#     }
#   }

#   /* Extract the content for the p2+1-th column.  Control can only
#   ** reach this point if aOffset[p2], aOffset[p2+1], and aType[p2] are
#   ** all valid.
#   */
#   assert( p2<pC->nHdrParsed );
#   assert( rc==SQLITE_OK );
#   assert( sqlite3VdbeCheckMemInvariants(pDest) );
#   if( pC->szRow>=aOffset[p2+1] ){
#     /* This is the common case where the desired content fits on the original
#     ** page - where the content is not on an overflow page */
#     VdbeMemRelease(pDest);
#     sqlite3VdbeSerialGet(pC->aRow+aOffset[p2], aType[p2], pDest);
#   }else{
#     /* This branch happens only when content is on overflow pages */
#     t = aType[p2];
#     if( ((pOp->p5 & (OPFLAG_LENGTHARG|OPFLAG_TYPEOFARG))!=0
#           && ((t>=12 && (t&1)==0) || (pOp->p5 & OPFLAG_TYPEOFARG)!=0))
#      || (len = sqlite3VdbeSerialTypeLen(t))==0
#     ){
#       /* Content is irrelevant for the typeof() function and for
#       ** the length(X) function if X is a blob.  So we might as well use
#       ** bogus content rather than reading content from disk.  NULL works
#       ** for text and blob and whatever is in the payloadSize64 variable
#       ** will work for everything else.  Content is also irrelevant if
#       ** the content length is 0. */
#       zData = t<=13 ? (u8*)&payloadSize64 : 0;
#       sMem.zMalloc = 0;
#     }else{
#       memset(&sMem, 0, sizeof(sMem));
#       sqlite3VdbeMemMove(&sMem, pDest);
#       rc = sqlite3VdbeMemFromBtree(pCrsr, aOffset[p2], len, !pC->isTable,
#                                    &sMem);
#       if( rc!=SQLITE_OK ){
#         goto op_column_error;
#       }
#       zData = (u8*)sMem.z;
#     }
#     sqlite3VdbeSerialGet(zData, t, pDest);
#     /* If we dynamically allocated space to hold the data (in the
#     ** sqlite3VdbeMemFromBtree() call above) then transfer control of that
#     ** dynamically allocated space over to the pDest structure.
#     ** This prevents a memory copy. */
#     if( sMem.zMalloc ){
#       assert( sMem.z==sMem.zMalloc );
#       assert( VdbeMemDynamic(pDest)==0 );
#       assert( (pDest->flags & (MEM_Blob|MEM_Str))==0 || pDest->z==sMem.z );
#       pDest->flags &= ~(MEM_Ephem|MEM_Static);
#       pDest->flags |= MEM_Term;
#       pDest->z = sMem.z;
#       pDest->zMalloc = sMem.zMalloc;
#     }
#   }
#   pDest->enc = encoding;

# op_column_out:
#   // Translated Deephemeralize(pDest);
#   if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
#     // goto no_mem;
#     printf("In impl_OP_Column(): no_mem.\n");
#     assert(0);
#   }

# op_column_error:
#   UPDATE_MAX_BLOBSIZE(pDest);
#   REGISTER_TRACE(pOp->p3, pDest);

#   return rc;
# }

