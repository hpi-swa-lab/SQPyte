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


def python_OP_Init(p, db, pc, pOp):
    if pOp.p2:
        pc = pOp.p2 - 1
    return pc

def python_OP_OpenRead_translated(p, db, pc, pOp):
    assert(pOp.p5 & (CConfig.OPFLAG_P2ISREG | CConfig.OPFLAG_BULKCSR) == pOp.p5)
    assert(pOp.opcode == CConfig.OP_OpenWrite or pOp.p5 == 0)
    assert(p.bIsReader)
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

def python_OP_Rewind(p, db, pc, pOp):
    return capi.impl_OP_Rewind(p, db, pc, pOp)

# def python_OP_Transaction(p, db, pc, pOp):
#     return capi.impl_OP_Transaction(p, db, pc, pOp)

def python_OP_Transaction(p, db, pc, pOp):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as internalPc:
        rc = capi.impl_OP_Transaction(p, db, internalPc, pOp)
    return internalPc[0], rc

def python_OP_TableLock(p, db, pc, pOp):
    capi.impl_OP_TableLock(p, db, pc, pOp)

def python_OP_Goto(p, db, pc, pOp):
    return capi.impl_OP_Goto(p, db, pc, pOp)

def python_OP_OpenRead(p, db, pc, pOp):
    capi.impl_OP_OpenRead(p, db, pc, pOp)

def python_OP_Column(p, db, pc, pOp):
    capi.impl_OP_Column(p, db, pc, pOp)

def python_OP_ResultRow(p, db, pc, pOp):
    return capi.impl_OP_ResultRow(p, db, pc, pOp)

def python_OP_Next(p, db, pc, pOp):
    capi.impl_OP_Next(p, db, pc, pOp)

def python_OP_Close(p, db, pc, pOp):
    capi.impl_OP_Close(p, db, pc, pOp)

def python_OP_Halt(p, db, pc, pOp):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as internalPc:
        rc = capi.impl_OP_Halt(p, db, internalPc, pOp)
    return internalPc[0], rc

def mainloop(vdbe_struct):
    # pc = 0
    length = vdbe_struct.nOp
    ops = vdbe_struct.aOp
    rc = CConfig.SQLITE_OK
    db = vdbe_struct.db
    p = vdbe_struct
    aMem = p.aMem
    pc = p.pc
    rc = 0
    # while pc < length:
    while pc >= 0:
    # while rc == CConfig.SQLITE_OK:
        pOp = ops[pc]

        if pOp.opcode == CConfig.OP_Init:
            print 'OP_Init'
            pc = python_OP_Init(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_OpenRead or pOp.opcode == CConfig.OP_OpenWrite:
            print 'OP_OpenRead'
            python_OP_OpenRead(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Rewind:
            print 'OP_Rewind'
            pc = python_OP_Rewind(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Transaction:
            print 'OP_Transaction'
            pc, rc = python_OP_Transaction(p, db, pc, pOp)
            print pc + ' ' + rc
        elif pOp.opcode == CConfig.OP_TableLock:
            print 'OP_TableLock'
            python_OP_TableLock(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Goto:
            print 'OP_Goto'
            pc = python_OP_Goto(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Column:
            print 'OP_Column'
            python_OP_Column(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_ResultRow:
            print 'OP_ResultRow'
            pc = python_OP_ResultRow(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Next:
            print 'OP_Next'
            python_OP_Next(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Close:
            print 'OP_Close'
            python_OP_Close(p, db, pc, pOp)
        elif pOp.opcode == CConfig.OP_Halt:
            print 'OP_Halt'
            pc, rc = python_OP_Halt(p, db, pc, pOp)
            print pc + ' ' + rc
        else:
            print 'Opcode %s is not there yet!' % pOp.opcode
            # raise Exception("Unimplemented bytecode %s." % pOp.opcode)
            pass
        print pc
        pc += 1


def run():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;')
    mainloop(ops)

def entry_point(argv):
    run()
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point()
