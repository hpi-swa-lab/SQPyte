import sys, os, time
from sqpyte.interpreter import Sqlite3DB, Sqlite3Query, SQPyteException
from rpython.rlib import jit
from sqpyte.capi import CConfig
from rpython.rtyper.lltypesystem import rffi

jitdriver = jit.JitDriver(
    greens=['printRes', 'query', 'queryRes'], 
    reds=['rc'],
    )
    # get_printable_location=get_printable_location)

def run(query, queryRes, printRes):
    try:
        query.reset_query()

        if queryRes != "" and printRes:
            print 'Query result:'

        rc = query.mainloop()
        while rc == CConfig.SQLITE_ROW:
            jitdriver.jit_merge_point(query=query, queryRes=queryRes, printRes=printRes, rc=rc)
            if printRes:
                result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), query.python_sqlite3_column_bytes(0))
                i = 1
                textlen = query.python_sqlite3_column_bytes(i)
                while textlen > 0:
                    result += "|" + rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(i)), textlen)
                    i += 1
                    textlen = query.python_sqlite3_column_bytes(i)    
                print result
            rc = query.mainloop()

        if queryRes != "" and printRes:
            print '\nExpected result:\n%s' % queryRes

    except SQPyteException:
        raise

def entry_point(argv):
    try:
        flag = argv[1]
        testdb = argv[2]
        queryPath = argv[3]
    except IndexError:
        print "Error: Not enough arguments."
        print "Usage:"
        print "For testing: './target-c -t path_to_db path_to_query_file [path_to_file_with_query_results]'"
        print "For benchmarking: './target-c -b path_to_db path_to_query_file'"
        return 1

    if flag == '-t':
        testingFlag = True
    elif flag == '-b':
        testingFlag = False
    else:
        print "Error: Unknown flag %s." % flag
        print "Usage:"
        print "For testing: './target-c -t path_to_db path_to_query_file [path_to_file_with_query_results]'"
        print "For benchmarking: './target-c -b path_to_db path_to_query_file'"
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
    if len(argv) > 4:
        queryResPath = argv[4]
        fp = os.open(queryResPath, os.O_RDONLY, 0777)
        while True:
            read = os.read(fp, 4096)
            if len(read) == 0:
                break
            queryRes += read
        os.close(fp)

    db = Sqlite3DB(testdb).db
    query = Sqlite3Query(db, queryStr)
    
    if testingFlag:
        run(query, queryRes, True)
    else:
        for i in range(5):
            run(query, "", False)
        t1 = time.time()
        run(query, queryRes, False)
        t2 = time.time()
        print "%s" % (t2 - t1)
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point(sys.argv)
