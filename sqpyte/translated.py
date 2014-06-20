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

def python_OP_OpenRead_translated(p, db, pc, pOp):
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


def sqlite3BtreeNext(pCur, pRes):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as res:
        rc = capi.sqlite3_sqlite3BtreeNext(pCur, res)
        return rc, res[0]

def python_OP_Next_translated(p, db, pc, pOp):
    pcRet = pc
    assert pOp.p1 >= 0 and pOp.p1 < p.nCursor
    assert pOp.p5 < len(p.aCounter)
    pC = p.apCsr[pOp.p1]
    res = pOp.p3
    assert pC
    assert pC.deferredMoveto == 0
    assert pC.pCursor
    assert res == 0 or (res == 1 and pC.isTable == 0)
    # testcase( res==1 );
    # assert( pOp->opcode!=OP_Next || pOp->p4.xAdvance==sqlite3BtreeNext );
    # assert( pOp->opcode!=OP_Prev || pOp->p4.xAdvance==sqlite3BtreePrevious );
    # assert( pOp->opcode!=OP_NextIfOpen || pOp->p4.xAdvance==sqlite3BtreeNext );
    # assert( pOp->opcode!=OP_PrevIfOpen || pOp->p4.xAdvance==sqlite3BtreePrevious);
    
    # rc = pOp->p4.xAdvance(pC->pCursor, &res);
    rc, resRet = sqlite3BtreeNext(pC.pCursor, res)

    # next_tail:
    pC.cacheStatus = rffi.cast(rffi.UINT, CConfig.CACHE_STALE)
    # VdbeBranchTaken(res==0, 2)
    if resRet == 0:
        pC.nullRow = rffi.cast(rffi.UCHAR, 0)
        pcRet = pOp.p2 - 1
        p.aCounter[pOp.p5] += 1
        #ifdef SQLITE_TEST
            # sqlite3_search_count++;
        #endif
    else:
        pC.nullRow = rffi.cast(rffi.UCHAR, 1)

    pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)
    # goto check_for_interrupt;
    return pcRet, rc


# int impl_OP_Next(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
#   VdbeCursor *pC;
#   int res;
#   int rc;

#   assert( pOp->p1>=0 && pOp->p1<p->nCursor );
#   assert( pOp->p5<ArraySize(p->aCounter) );
#   pC = p->apCsr[pOp->p1];
#   res = pOp->p3;
#   assert( pC!=0 );
#   assert( pC->deferredMoveto==0 );
#   assert( pC->pCursor );
#   assert( res==0 || (res==1 && pC->isTable==0) );
#   testcase( res==1 );
#   assert( pOp->opcode!=OP_Next || pOp->p4.xAdvance==sqlite3BtreeNext );
#   assert( pOp->opcode!=OP_Prev || pOp->p4.xAdvance==sqlite3BtreePrevious );
#   assert( pOp->opcode!=OP_NextIfOpen || pOp->p4.xAdvance==sqlite3BtreeNext );
#   assert( pOp->opcode!=OP_PrevIfOpen || pOp->p4.xAdvance==sqlite3BtreePrevious);
#   rc = pOp->p4.xAdvance(pC->pCursor, &res);
# next_tail:
#   pC->cacheStatus = CACHE_STALE;
#   VdbeBranchTaken(res==0,2);
#   if( res==0 ){
#     pC->nullRow = 0;
#     *pc = pOp->p2 - 1;
#     p->aCounter[pOp->p5]++;
# #ifdef SQLITE_TEST
#     sqlite3_search_count++;
# #endif
#   }else{
#     pC->nullRow = 1;
#   }
#   pC->rowidIsValid = 0;
#   // goto check_for_interrupt;
#   return rc;
# }
            