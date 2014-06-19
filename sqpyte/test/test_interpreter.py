from rpython.rtyper.lltypesystem import rffi
from sqpyte.interpreter import Sqlite3DB, Sqlite3Query
from sqpyte.capi import CConfig
from sqpyte import capi
import os, sys
from sqpyte.translated import allocateCursor

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

def test_opendb():
    db = Sqlite3DB(testdb).db
    assert db

def test_prepare():
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, 'select * from contacts;')
    assert query.p and query.db
    assert query.p.db == query.db
    assert query.p.nOp == 17

    assert query.p.aOp[0].opcode == 155
    assert query.p.aOp[0].p1 == 0
    assert query.p.aOp[0].p2 == 14
    assert query.p.aOp[0].p3 == 0

    assert query.p.aOp[1].opcode == 52
    assert query.p.aOp[1].p1 == 0
    assert query.p.aOp[1].p2 == 2
    assert query.p.aOp[1].p3 == 0

    assert query.p.aOp[2].opcode == 105
    assert query.p.aOp[2].p1 == 0
    assert query.p.aOp[2].p2 == 12
    assert query.p.aOp[2].p3 == 0

def test_multiple_queries():
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, 'select name from contacts;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 100)
    query = Sqlite3Query(db, 'select name from contacts where age > 50;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 48)

def test_reset():
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, 'select name from contacts;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    name = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    query.reset_query()
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    name2 = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert name == name2
    
def test_mainloop_over50():
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, 'select name from contacts where age > 50;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 48)

def test_mainloop_namelist():
    fname = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'names.txt')
    names = [name.strip() for name in open(fname)]
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, 'select name from contacts;')
    rc = query.mainloop()
    i = 0
    while rc == CConfig.SQLITE_ROW:
        textlen = query.python_sqlite3_column_bytes(0)
        name = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
        rc = query.mainloop()
        assert(name == names[i])
        i += 1
    assert(len(names) == i)


def test_allocateCursor():
    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, 'select name from contacts;')
    p = query.p #prepare(db, 'select name from contacts;')
    vdbe = allocateCursor(p, p.aOp[0].p1, p.aOp[0].p4.i, p.aOp[0].p3, 1)

# def test_sqlite3VdbeMemIntegerify():
#     db = opendb(testdb)
#     p = prepare(db, 'select name from contacts;')
#     pOp = p.aOp[0]
#     p2 = pOp.p2
#     aMem = p.aMem
#     pMem = aMem[p2]
#     rc = sqlite3VdbeMemIntegerify(pMem)
#     assert(rc == CConfig.SQLITE_OK)

# def test_sqlite3BtreeCursor():
#     db = opendb(testdb)
#     p = prepare(db, 'select name from contacts;')
#     pOp = p.aOp[0]
#     p2 = pOp.p2
#     iDb = pOp.p3
#     pDb = db.aDb[iDb]
#     pX = pDb.pBt
#     wrFlag = 1
#     pKeyInfo = pOp.p4.pKeyInfo
#     nField = p.aOp[0].p4.i
#     pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
#     pCur.nullRow = rffi.r_uchar(1)
#     pCur.isOrdered = bool(1)
#     rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
#     assert(rc == CConfig.SQLITE_OK)

# def test_sqlite3BtreeCursorHints():
#     db = opendb(testdb)
#     p = prepare(db, 'select name from contacts;')
#     pOp = p.aOp[0]
#     iDb = pOp.p3
#     nField = p.aOp[0].p4.i
#     pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
#     sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))

# def test_sqlite3VdbeSorterRewind():
#     db = opendb(testdb)
#     p = prepare(db, 'select name from contacts;')
#     pOp = p.aOp[0]
#     p2 = pOp.p2
#     iDb = pOp.p3
#     pDb = db.aDb[iDb]
#     pX = pDb.pBt
#     wrFlag = 1
#     pKeyInfo = pOp.p4.pKeyInfo
#     nField = p.aOp[0].p4.i
#     pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
#     pCur.nullRow = rffi.r_uchar(1)
#     pCur.isOrdered = bool(1)
#     rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
#     sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))
#     pC = p.apCsr[pOp.p1]
#     res = 1
#     rc = sqlite3VdbeSorterRewind(db, pC, res)
#     assert(rc == CConfig.SQLITE_OK)

