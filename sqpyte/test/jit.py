import sys, os
from rpython.rlib import jit
from rpython import conftest
class o:
    view = False
    viewloops = True
conftest.option = o

from rpython.jit.metainterp.test.test_ajit import LLJitMixin

from sqpyte.interpreter import Sqlite3DB, Sqlite3Query
from sqpyte.capi import CConfig
from sqpyte.test.test_interpreter import testdb

sys.setrecursionlimit(5000)

jitdriver = jit.JitDriver(
    greens=['query'], 
    reds=['i', 'rc'],
    )
    # get_printable_location=get_printable_location)


class TestLLtype(LLJitMixin):

    def test_miniloop(self):

        def interp_w():
            db = Sqlite3DB(testdb).db
            query = Sqlite3Query(db, 'select name from contacts where age > 1;')

            rc = query.mainloop()
            i = 0
            while rc == CConfig.SQLITE_ROW:
                jitdriver.jit_merge_point(i=i, query=query, rc=rc)
                textlen = query.python_sqlite3_column_bytes(0)
                rc = query.mainloop()
                i += textlen
            return i
            
        res = interp_w()
        self.meta_interp(interp_w, [], listcomp=True, listops=True, backendopt=True, inline=True)