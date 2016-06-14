import pytest
from rpython.rtyper.lltypesystem import rffi, lltype
from sqpyte.interpreter import SQPyteDB
from sqpyte import function
from sqpyte.capi import CConfig
from sqpyte import capi
import os, sys
import math

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

def test_create_aggregate():
    class MyContext(function.Context):
        def __init__(self, pfunc):
            self.pfunc = pfunc
            self.currlength = 0

        def step(self, args):
            arg, = args
            self.currlength += arg.get_n()

        def finalize(self, memout):
            memout.sqlite3_result_int64(self.currlength)

    db = SQPyteDB(testdb)
    db.create_aggregate("sumlength", 1, MyContext)
    query = db.execute('select sumlength(name), sum(length(name)) from contacts;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    assert query.column_int64(0) == query.column_int64(1)

def test_create_function():
    def sin(func, args, result):
        arg, = args
        arg = arg.sqlite3_value_double()
        result.sqlite3_result_double(math.sin(arg))

    db = SQPyteDB(testdb)
    db.create_function("sin", 1, sin)
    query = db.execute('select sin(age), age from contacts;')
    while True:
        rc = query.mainloop()
        if rc != CConfig.SQLITE_ROW:
            break
        assert query.column_double(0) == math.sin(query.column_double(1))

def test_sum():
    db = SQPyteDB(testdb)
    query = db.execute('select sum(age) from contacts;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    res = query.column_int64(0)
    assert res == 4832


def test_sum_none():
    db = SQPyteDB(testdb)
    query = db.execute('select sum(age) from contacts where age < 0;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    res = query.column_int64(0)
    assert res == 0

def test_avg():
    db = SQPyteDB(testdb)
    query = db.execute('select avg(age) from contacts where age > 18;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    res = query.column_double(0)
    assert round(res, 2) == 58.91

def test_max():
    db = SQPyteDB(testdb)
    query = db.execute('select max(age) from contacts where age > 18;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    res = query.column_double(0)
    assert round(res, 2) == 99
