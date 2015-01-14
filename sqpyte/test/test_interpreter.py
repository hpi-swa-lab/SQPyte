from rpython.rtyper.lltypesystem import rffi
from sqpyte.interpreter import Sqlite3DB, Sqlite3Query
from sqpyte.capi import CConfig
from sqpyte import capi
from sqpyte.translated import allocateCursor, sqlite3BtreeCursor
from sqpyte.translated import sqlite3BtreeCursorHints, sqlite3VdbeSorterRewind
import os, sys

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")
# testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "big-test.db")

def test_opendb():
    db = Sqlite3DB(testdb).db
    assert db

def test_prepare():
    db = Sqlite3DB(testdb)
    query = db.execute('select * from contacts;')
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
    db = Sqlite3DB(testdb)
    query = db.execute('select name from contacts;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 100)
    query = db.execute('select name from contacts where age > 50;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 48)

def test_reset():
    db = Sqlite3DB(testdb)
    query = db.execute('select name from contacts;')
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
    db = Sqlite3DB(testdb)
    query = db.execute('select name from contacts where age > 50;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 48)

def test_mainloop_arithmetic():
    db = Sqlite3DB(testdb)
    query = db.execute('select name from contacts where 2 * age + 2 - age / 1 > 48;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 53)

def test_mainloop_mixed_arithmetic():
    db = Sqlite3DB(testdb)
    query = db.execute('select name from contacts where 2.1 * age + 2 - age / 0.909 > 48;')
    rc = query.mainloop()
    count = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        count += 1
    assert(count == 53)

def test_count_avg_sum():
    db = Sqlite3DB(testdb)
    query = db.execute('select count(*), avg(age), sum(age) from contacts where 2 * age + 2 - age / 1 > 48;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    count = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert count == "53"
    textlen = query.python_sqlite3_column_bytes(1)
    avg = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(1)), textlen)
    assert avg == "72.5283018867924"
    textlen = query.python_sqlite3_column_bytes(2)
    sum = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(2)), textlen)
    assert sum == "3844"

def test_mainloop_namelist():
    fname = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'names.txt')
    names = [name.strip() for name in open(fname)]
    db = Sqlite3DB(testdb)
    query = db.execute('select name from contacts;')
    rc = query.mainloop()
    i = 0
    while rc == CConfig.SQLITE_ROW:
        textlen = query.python_sqlite3_column_bytes(0)
        name = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
        rc = query.mainloop()
        assert(name == names[i])
        i += 1
    assert(len(names) == i)

def test_count():
    db = Sqlite3DB(testdb)
    query = db.execute('select count(name) from contacts where age > 20;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    count = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert int(count) == 76

def test_null_comparison():
    db = Sqlite3DB(testdb)
    query = db.execute('select count(*) from contacts where age > 10 and age < 14;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    count = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert int(count) == 3

def test_comparison():
    db = Sqlite3DB(testdb)
    query = db.execute('select count(*) from contacts where age > 40 and age < 60;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    count = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert int(count) == 18

def test_string_comparison():
    db = Sqlite3DB(testdb)
    query = db.execute("select count(*) from contacts where name = 'Raphael Paul';")
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    count = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert int(count) == 1    

def test_makerecord():
    db = Sqlite3DB(testdb)
    query = db.execute("select age, name from contacts order by age;")
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(1)
    name = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(1)), textlen)
    assert name == "Jermaine Mayo"

def test_translated_allocateCursor():
    db = Sqlite3DB(testdb)
    p = db.execute('select name from contacts;').p
    vdbe = allocateCursor(p, p.aOp[0].p1, p.aOp[0].p4.i, p.aOp[0].p3, 1)

def test_translated_sqlite3BtreeCursorHints():
    db = Sqlite3DB(testdb)
    p = db.execute('select name from contacts;').p
    pOp = p.aOp[0]
    iDb = pOp.p3
    nField = p.aOp[0].p4.i
    pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
    sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))

#
# NOTE: Currently sqlite3VdbeSorterRewind() function is not used and segfaults.
#
# def test_translated_sqlite3VdbeSorterRewind():
#     db = Sqlite3DB(testdb)
#     p = db.execute('select name from contacts;').p
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

