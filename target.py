import sys, os, time
from sqpyte.interpreter import Sqlite3DB, Sqlite3Query
from rpython.rlib import jit
from sqpyte.capi import CConfig

# testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/test.db")
testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/tpch.db")
assert os.path.isfile(testdb)

jitdriver = jit.JitDriver(
    greens=['query'], 
    reds=['i', 'rc'],
    )
    # get_printable_location=get_printable_location)

def run(query):
    query.reset_query()
    rc = query.mainloop()
    i = 0
    while rc == CConfig.SQLITE_ROW:
        jitdriver.jit_merge_point(i=i, query=query, rc=rc)
        textlen = query.python_sqlite3_column_bytes(0)
        rc = query.mainloop()
        i += textlen
    return i

def entry_point(argv):
    # try:
    #     query = argv[1]
    # except IndexError:
    #     print "You must supply a query to be run: e.g., 'select first_name from people where age > 1;'."
    #     return 1

    queryStr = "select sum(l.extendedprice * l.discount) as revenue from lineitem l where l.shipdate >= date('1996-01-01') and l.shipdate < date('1996-01-01', '+1 year') and l.discount between 0.04 and 0.07 and l.quantity < 25;"

    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, queryStr)
    
    for i in range(2):
        run(query)
    t1 = time.time()
    print run(query)
    t2 = time.time()
    print "%ss" % (t2 - t1)
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point(sys.argv)
