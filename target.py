import sys, os, time
from sqpyte.interpreter import Sqlite3DB, Sqlite3Query, SQPyteException
from rpython.rlib import jit
from sqpyte.capi import CConfig
from rpython.rtyper.lltypesystem import rffi

# testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/test.db")
testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/tpch.db")
# testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/big-tpch.db")

assert os.path.isfile(testdb)

jitdriver = jit.JitDriver(
    greens=['query', 'queryRes'], 
    reds=['i', 'rc'],
    )
    # get_printable_location=get_printable_location)

def run(query, queryRes):
    try:
        query.reset_query()
        rc = query.mainloop()
        i = 0
        textlen = 0
        while rc == CConfig.SQLITE_ROW:
            jitdriver.jit_merge_point(i=i, query=query, queryRes=queryRes, rc=rc)
            textlen = query.python_sqlite3_column_bytes(0)
            rc = query.mainloop()
            i += textlen
        result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
        if queryRes != "":
            print 'Expected result:\n%s' % queryRes
        return result
    except SQPyteException:
        raise

def entry_point(argv):
    try:
        queryPath = argv[1]
    except IndexError:
        print "You must supply a file with query to be run."
        return 1

    fp = os.open(queryPath, os.O_RDONLY, 0777)
    queryStr = ""
    while True:
        read = os.read(fp, 4096)
        if len(read) == 0:
            break
        queryStr += read
    os.close(fp)

    queryRes = ""
    if len(argv) > 2:
        queryResPath = argv[2]
        fp = os.open(queryResPath, os.O_RDONLY, 0777)
        while True:
            read = os.read(fp, 4096)
            if len(read) == 0:
                break
            queryRes += read
        os.close(fp)

    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, queryStr)
    
    for i in range(2):
        run(query, "")
    t1 = time.time()
    if queryRes != "":
        print 'Query result:\n%s\n' % run(query, queryRes)
    else:    
        print run(query, queryRes)
    t2 = time.time()
    print "%s" % (t2 - t1)
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point(sys.argv)
