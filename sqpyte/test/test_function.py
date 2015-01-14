from rpython.rtyper.lltypesystem import rffi, lltype
from sqpyte.interpreter import Sqlite3DB
from sqpyte import function
from sqpyte.capi import CConfig
from sqpyte import capi
import os, sys

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test.db")

def test_create_function():
    class MyContext(function.Context):
        def __init__(self):
            self.currlength = 0

        def step(self, args):
            arg, = args
            self.currlength += arg.get_n()

        def finalize(self, memout):
            memout.sqlite3_result_int64(self.currlength)

    db = Sqlite3DB(testdb)
    db.create_aggregate("stringlength", 1, MyContext)
    query = db.execute('select stringlength(name), sum(length(name)) from contacts;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    assert query.python_sqlite3_column_int64(0) == query.python_sqlite3_column_int64(1)
