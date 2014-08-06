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
from sqpyte.test.test_tpch import tpchdb

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
            query = Sqlite3Query(db, 'select name from contacts where age * 2 + 6 / 3> 15 - 3 * 5;')

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


    def test_miniloop(self):

        def interp_w():
            db = Sqlite3DB(tpchdb).db
            query = Sqlite3Query(db, '''
select
        sum(l.extendedprice * l.discount) as revenue
from
        lineitem l
where
        l.shipdate >= date('1996-01-01')
        and l.shipdate < date('1996-01-01', '+1 year')
        and l.discount between 0.04 and 0.07
        and l.quantity < 25;
''')

            rc = query.mainloop()
            i = 0
            while rc == CConfig.SQLITE_ROW:
                jitdriver.jit_merge_point(i=i, query=query, rc=rc)
                textlen = query.python_sqlite3_column_bytes(0)
                rc = query.mainloop()
                i += textlen
            return i
        self.meta_interp(interp_w, [], listcomp=True, listops=True, backendopt=True, inline=True)
