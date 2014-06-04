from rpython.rtyper.lltypesystem import rffi
from sqpyte.interpreter import opendb, prepare, mainloop, allocateCursor, python_sqlite3_column_text, python_sqlite3_column_bytes
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
    p = prepare(db, 'select * from contacts;')
    assert p and db
    assert p.db == db
    assert p.nOp == 17

    assert p.aOp[0].opcode == 155
    assert p.aOp[0].p1 == 0
    assert p.aOp[0].p2 == 14
    assert p.aOp[0].p3 == 0

    assert p.aOp[1].opcode == 52
    assert p.aOp[1].p1 == 0
    assert p.aOp[1].p2 == 2
    assert p.aOp[1].p3 == 0

    assert p.aOp[2].opcode == 105
    assert p.aOp[2].p1 == 0
    assert p.aOp[2].p2 == 12
    assert p.aOp[2].p3 == 0

def test_mainloop():
    db = opendb(testdb)
    p = prepare(db, 'select name from contacts;')
    rc = mainloop(p)
    textlen = python_sqlite3_column_bytes(p, 0)
    print python_sqlite3_column_bytes(p, 0)
    print rffi.charpsize2str(rffi.cast(rffi.CCHARP, python_sqlite3_column_text(p, 0)), textlen)
    rc = mainloop(p)
    textlen = python_sqlite3_column_bytes(p, 2)
    print python_sqlite3_column_bytes(p, 2)
    print rffi.charpsize2str(rffi.cast(rffi.CCHARP, python_sqlite3_column_text(p, 2)), textlen)
    # while rc == CConfig.SQLITE_ROW:
    #     textlen = python_sqlite3_column_bytes(p, 0)
    #     print python_sqlite3_column_bytes(p, 0)
    #     print rffi.charpsize2str(rffi.cast(rffi.CCHARP, python_sqlite3_column_text(p, 0)), textlen)
    #     rc = mainloop(p)

def test_allocateCursor():
    db = opendb(testdb)
    p = prepare(db, 'select name from contacts;')
    vdbe = allocateCursor(p, p.aOp[0].p1, p.aOp[0].p4.i, p.aOp[0].p3, 1)

def test_sqlite3VdbeMemIntegerify():
    db = opendb(testdb)
    p = prepare(db, 'select name from contacts;')
    pOp = p.aOp[0]
    p2 = pOp.p2
    aMem = p.aMem
    pMem = aMem[p2]
    rc = sqlite3VdbeMemIntegerify(pMem)
    assert(rc == CConfig.SQLITE_OK)

def test_sqlite3BtreeCursor():
    db = opendb(testdb)
    p = prepare(db, 'select name from contacts;')
    pOp = p.aOp[0]
    p2 = pOp.p2
    iDb = pOp.p3
    pDb = db.aDb[iDb]
    pX = pDb.pBt
    wrFlag = 1
    pKeyInfo = pOp.p4.pKeyInfo
    nField = p.aOp[0].p4.i
    pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
    pCur.nullRow = rffi.r_uchar(1)
    pCur.isOrdered = bool(1)
    rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
    assert(rc == CConfig.SQLITE_OK)

def test_sqlite3BtreeCursorHints():
    db = opendb(testdb)
    p = prepare(db, 'select name from contacts;')
    pOp = p.aOp[0]
    iDb = pOp.p3
    nField = p.aOp[0].p4.i
    pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
    sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))

def test_sqlite3VdbeSorterRewind():
    db = opendb(testdb)
    p = prepare(db, 'select name from contacts;')
    pOp = p.aOp[0]
    p2 = pOp.p2
    iDb = pOp.p3
    pDb = db.aDb[iDb]
    pX = pDb.pBt
    wrFlag = 1
    pKeyInfo = pOp.p4.pKeyInfo
    nField = p.aOp[0].p4.i
    pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
    pCur.nullRow = rffi.r_uchar(1)
    pCur.isOrdered = bool(1)
    rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
    sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))
    pC = p.apCsr[pOp.p1]
    res = 1
    rc = sqlite3VdbeSorterRewind(db, pC, res)
    assert(rc == CConfig.SQLITE_OK)

def test_sqpyte_test_function():
    assert capi.sqpyte_test_function(1) == 2
