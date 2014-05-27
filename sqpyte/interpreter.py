from rpython.rtyper.lltypesystem import rffi, lltype
from capi import CConfig
from rpython.rlib.rarithmetic import intmask
import sys
import os
import capi

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

def opendb(db_name):
    with rffi.scoped_str2charp(db_name) as db_name, lltype.scoped_alloc(capi.SQLITE3PP.TO, 1) as result:
        errorcode = capi.sqlite3_open(db_name, result)
        assert errorcode == 0
        return rffi.cast(capi.SQLITE3P, result[0])

def prepare(db, query):
    length = len(query)
    with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
        errorcode = capi.sqlite3_prepare(db, query, length, result, unused_buffer)
        assert errorcode == 0
        return rffi.cast(capi.VDBEP, result[0])

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

def mainloop(vdbe_struct):
    pc = 0
    length = vdbe_struct.nOp
    ops = vdbe_struct.aOp
    rc = CConfig.SQLITE_OK
    db = vdbe_struct.db
    p = vdbe_struct
    aMem = p.aMem

    while pc < length:
        pOp = ops[pc]

        if pOp.opcode == CConfig.OP_Init:
            if pOp.p2:
                pc = pOp.p2 - 1
        elif pOp.opcode == CConfig.OP_OpenRead or pOp.opcode == CConfig.OP_OpenWrite:
            assert(pOp.p5 & (CConfig.OPFLAG_P2ISREG | CConfig.OPFLAG_BULKCSR) == pOp.p5)
            assert(pOp.opcode == CConfig.OP_OpenWrite or pOp.p5 == 0)
            assert(p.bIsReader)
            assert(pOp.opcode == CConfig.OP_OpenRead or p.readOnly == 0)

            if (p.expired):
                rc = CConfig.SQLITE_ABORT
                break

            nField = 0
            pKeyInfo = lltype.nullptr(capi.KEYINFO)
            p2 = pOp.p2;
            iDb = pOp.p3
            wrFlag = 0

            assert(iDb >= 0 and iDb < db.nDb)
            #   assert( (p->btreeMask & (((yDbMask)1)<<iDb))!=0 );

            pDb = db.aDb[iDb]
            pX = pDb.pBt

            assert(not pX)

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
                break
        elif pOp.opcode == CConfig.OP_Rewind:
            assert(pOp.p1 >= 0 and pOp.p1 < p.nCursor)
            pC = p.apCsr[pOp.p1]
            assert(pC != 0)
            #define isSorter(x) ((x)->pSorter!=0)
            #   assert( isSorter(pC)==(pOp->opcode==OP_SorterSort) );
            assert((pC.pSorter != 0) == (pOp.opcode == CConfig.OP_SorterSort))
            # res = 1
            if pC.pSorter != 0:
                pass
                # rc = sqlite3VdbeSorterRewind(db, pC, &res)
            else:
                pCrsr = pC.pCursor

            # case OP_Rewind: {        /* jump */
            #   VdbeCursor *pC;
            #   BtCursor *pCrsr;
            #   int res;

            #   assert( pOp->p1>=0 && pOp->p1<p->nCursor );
            #   pC = p->apCsr[pOp->p1];
            #   assert( pC!=0 );
            #   assert( isSorter(pC)==(pOp->opcode==OP_SorterSort) );
            #   res = 1;
            #   if( isSorter(pC) ){
            #     rc = sqlite3VdbeSorterRewind(db, pC, &res);
            #   }else{
            #     pCrsr = pC->pCursor;
            #     assert( pCrsr );
            #     rc = sqlite3BtreeFirst(pCrsr, &res);
            #     pC->deferredMoveto = 0;
            #     pC->cacheStatus = CACHE_STALE;
            #     pC->rowidIsValid = 0;
            #   }
            #   pC->nullRow = (u8)res;
            #   assert( pOp->p2>0 && pOp->p2<p->nOp );
            #   VdbeBranchTaken(res!=0,2);
            #   if( res ){
            #     pc = pOp->p2 - 1;
            #   }
            #   break;
            # }
        else:
            # raise Exception("unimplemented bytecode %s " % pOp.opcode)
            pass

        pc += 1

    return pc


def run():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;')
    pc = mainloop(ops)

def entry_point(argv):
    run()
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point()
