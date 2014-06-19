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
        return rffi.cast(capi.INTP, res[0])

def python_OP_OpenRead_translated(p, db, pc, pOp):
        assert(pOp.p5 & (CConfig.OPFLAG_P2ISREG | CConfig.OPFLAG_BULKCSR) == pOp.p5)
        assert(pOp.opcode == CConfig.OP_OpenWrite or pOp.p5 == 0)
        # FIX: Initialize bIsReader to 1
        # assert(p.bIsReader)
        assert(pOp.opcode == CConfig.OP_OpenRead or p.readOnly == 0)

        if (p.expired):
            rc = CConfig.SQLITE_ABORT
            return

        nField = 0
        pKeyInfo = lltype.nullptr(capi.KEYINFO)
        p2 = pOp.p2;
        iDb = pOp.p3
        wrFlag = 0

        assert(iDb >= 0 and iDb < db.nDb)
        #   assert( (p->btreeMask & (((yDbMask)1)<<iDb))!=0 );

        pDb = db.aDb[iDb]
        pX = pDb.pBt

        assert(pX)

        if pOp.opcode == CConfig.OP_OpenWrite:
            wrFlag = 1
            #     assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
            if pDb.pSchema.file_format < p.minWriteFileFormat:
                p.minWriteFileFormat = pDb.pSchema.file_format
            else:
                wrFlag = 0

        if pOp.p5 & CConfig.OPFLAG_P2ISREG:
            assert(p2 > 0)
            assert(p2 <= p.nMem - p.nCursor)
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

            assert(pOp.p1 >= 0)
            assert(nField >= 0)
            #   testcase( nField==0 );  /* Table with INTEGER PRIMARY KEY and nothing else */
            pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
            #   if( pCur==0 ) goto no_mem;
            pCur.nullRow = 1
            pCur.isOrdered = bool(1)
            rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
            pCur.pKeyInfo = pKeyInfo
            assert(CConfig.OPFLAG_BULKCSR == CConfig.BTREE_BULKLOAD)
            sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))

            #   /* Since it performs no memory allocation or IO, the only value that
            #   ** sqlite3BtreeCursor() may return is SQLITE_OK. */
            assert(rc == CConfig.SQLITE_OK)
            #   /* Set the VdbeCursor.isTable variable. Previous versions of
            #   ** SQLite used to check if the root-page flags were sane at this point
            #   ** and report database corruption if they were not, but this check has
            #   ** since moved into the btree layer.  */
            pCur.isTable = pOp.p4type != CConfig.P4_KEYINFO

            