import pytest
from rpython.rtyper.lltypesystem import rffi, lltype
from sqpyte.interpreter import Sqlite3DB
from sqpyte import function
from sqpyte.capi import CConfig
from sqpyte import capi
import os, sys

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

def test_create_function():
    class MyContext(function.Context):
        def __init__(self, pfunc):
            self.pfunc = pfunc
            self.currlength = 0

        def step(self, args):
            arg, = args
            self.currlength += arg.get_n()

        def finalize(self, memout):
            memout.sqlite3_result_int64(self.currlength)

    db = Sqlite3DB(testdb)
    db.create_aggregate("sumlength", 1, MyContext)
    query = db.execute('select sumlength(name), sum(length(name)) from contacts;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    assert query.python_sqlite3_column_int64(0) == query.python_sqlite3_column_int64(1)

def test_sum():
    db = Sqlite3DB(testdb)
    query = db.execute('select sum(age) from contacts;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    res = query.python_sqlite3_column_int64(0)
    assert res == 4832


def test_avg():
    db = Sqlite3DB(testdb)
    query = db.execute('select avg(age) from contacts where age > 18;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    res = query.python_sqlite3_column_double(0)
    assert round(res, 2) == 58.91
