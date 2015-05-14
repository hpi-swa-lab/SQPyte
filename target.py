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
                print "|".join([
                    rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.column_text(i)), query.column_bytes(i))
                        for i in range(query.data_count())])
            rc = query.mainloop()

        if queryRes != "" and printRes:
            print '\nExpected result:\n%s' % queryRes

    except SQPyteException:
        raise

def entry_point(argv):
    usageMsg = """\n
  Usage:
  For testing:      ./target-c -t db_file query_file [query_results_file]
  For benchmarking: ./target-c -b [warm_up] db_file query_file
    """
    disabled_opcodes = ""
    try:
        flag = argv[1]
        if flag == '--jit':
            jit.set_user_param(None, argv[2])
            flag = argv[3]
            argv = [argv[0]] + argv[3:]
        if flag == '--disable-opcodes':
            disabled_opcodes = argv[2]
            flag = argv[3]
            argv = [argv[0]] + argv[3:]
        if flag == '-t':
            testingFlag = True
            warmup = 0
            testdb = argv[2]
            queryPath = argv[3]
        elif flag == '-b':
            testingFlag = False
            warmup = 3
            if len(argv) > 4:
                try:
                    warmup = int(argv[2])
                except ValueError:
                    print "Error: '%s' is not a valid number for warm_up argument.%s" % (argv[2], usageMsg)
                    return 1
                testdb = argv[3]
                queryPath = argv[4]
            else:
                testdb = argv[2]
                queryPath = argv[3]
        else:
            print "Error: Unknown flag '%s'.%s" % (flag, usageMsg)
            return 1        
    except IndexError:
        print "Error: Not enough arguments.%s" % usageMsg
        return 1

    if testdb != ':memory:':
        try:
            fp = os.open(testdb, os.O_RDONLY, 0777)
            os.close(fp)
        except OSError:
            print "Error: Can't open '%s' file provided for db_file argument.%s" % (testdb, usageMsg)
            return 1

    try:
        fp = os.open(queryPath, os.O_RDONLY, 0777)
        queryStr = ""
        while True:
            read = os.read(fp, 4096)
            if len(read) == 0:
                break
            queryStr += read
        os.close(fp)
    except OSError:
        print "Error: Can't open '%s' file provided for query_file argument.%s" % (queryPath, usageMsg)
        return 1

    queryRes = ""
    if flag == '-t' and len(argv) > 4:
        queryResPath = argv[4]
        try:
            fp = os.open(queryResPath, os.O_RDONLY, 0777)
            while True:
                read = os.read(fp, 4096)
                if len(read) == 0:
                    break
                queryRes += read
            os.close(fp)
        except OSError:
            print "Error: Can't open '%s' file provided for query_results_file argument.%s" % (queryResPath, usageMsg)
            return 1

    db = Sqlite3DB(testdb)
    query = db.execute(queryStr, use_flag_cache=not disabled_opcodes)
    if disabled_opcodes:
        query.use_translated.disable_from_cmdline(disabled_opcodes)
    
    if testingFlag:
        run(query, queryRes, True)
    else:
        for i in range(warmup):
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
