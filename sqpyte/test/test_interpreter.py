from rpython.rtyper.lltypesystem import rffi
from sqpyte.interpreter import opendb, prepare, mainloop, allocateCursor
from sqpyte.interpreter import sqlite3VdbeMemIntegerify, sqlite3BtreeCursor, sqlite3BtreeCursorHints, sqlite3VdbeSorterRewind
from sqpyte.capi import CConfig
from sqpyte import capi
import os, sys

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

def test_opendb():
    db = opendb(testdb)
    assert db

def test_prepare():
    db = opendb(testdb)
    ops = prepare(db, 'select * from contacts;')
    assert ops and db
    assert ops.db == db
    assert ops.nOp == 17

    assert ops.aOp[0].opcode == 155
    assert ops.aOp[0].p1 == 0
    assert ops.aOp[0].p2 == 14
    assert ops.aOp[0].p3 == 0

    assert ops.aOp[1].opcode == 52
    assert ops.aOp[1].p1 == 0
    assert ops.aOp[1].p2 == 2
    assert ops.aOp[1].p3 == 0

    assert ops.aOp[2].opcode == 105
    assert ops.aOp[2].p1 == 0
    assert ops.aOp[2].p2 == 12
    assert ops.aOp[2].p3 == 0

def test_mainloop():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;')
    pc = mainloop(ops)
    assert pc == ops.nOp

def test_allocateCursor():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;')
    vdbe = allocateCursor(ops, ops.aOp[0].p1, ops.aOp[0].p4.i, ops.aOp[0].p3, 1)

def test_sqlite3VdbeMemIntegerify():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;')
    op = ops.aOp[0]
    p2 = op.p2
    aMem = ops.aMem
    pMem = aMem[p2]
    rc = sqlite3VdbeMemIntegerify(pMem)
    assert(rc == CConfig.SQLITE_OK)

def test_sqlite3BtreeCursor():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;') # a.k.a.  p
    op = ops.aOp[0] # a.k.a.  pOp
    p2 = op.p2
    iDb = op.p3
    pDb = db.aDb[iDb]
    pX = pDb.pBt
    wrFlag = 1
    pKeyInfo = op.p4.pKeyInfo
    nField = ops.aOp[0].p4.i
    pCur = allocateCursor(ops, op.p1, nField, iDb, 1)
    pCur.nullRow = rffi.r_uchar(1)
    pCur.isOrdered = bool(1)
    rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
    assert(rc == CConfig.SQLITE_OK)

def test_sqlite3BtreeCursorHints():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;') # a.k.a.  p
    op = ops.aOp[0] # a.k.a.  pOp
    iDb = op.p3
    nField = ops.aOp[0].p4.i
    pCur = allocateCursor(ops, op.p1, nField, iDb, 1)
    sqlite3BtreeCursorHints(pCur.pCursor, (op.p5 & CConfig.OPFLAG_BULKCSR))

def test_sqlite3VdbeSorterRewind():
    db = opendb(testdb)
    ops = prepare(db, 'select name from contacts;') # a.k.a.  p
    op = ops.aOp[0] # a.k.a.  pOp
    p2 = op.p2
    iDb = op.p3
    pDb = db.aDb[iDb]
    pX = pDb.pBt
    wrFlag = 1
    pKeyInfo = op.p4.pKeyInfo
    nField = ops.aOp[0].p4.i
    pCur = allocateCursor(ops, op.p1, nField, iDb, 1)
    pCur.nullRow = rffi.r_uchar(1)
    pCur.isOrdered = bool(1)
    rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
    sqlite3BtreeCursorHints(pCur.pCursor, (op.p5 & CConfig.OPFLAG_BULKCSR))
    pC = ops.apCsr[op.p1]
    res = 1
    rc = sqlite3VdbeSorterRewind(db, pC, res)
    assert(rc == CConfig.SQLITE_OK)

def test_sqpyte_test_function():
    assert capi.sqpyte_test_function(1) == 2
