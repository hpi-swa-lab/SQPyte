import sys, os
from sqpyte.interpreter import Sqlite3
from rpython.rlib import jit
from sqpyte.interpreter import Sqlite3
from sqpyte.capi import CConfig

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/test.db")
assert os.path.isfile(testdb)

jitdriver = jit.JitDriver(
    greens=['sqlite3'], 
    reds=['i', 'rc'],
    )
    # get_printable_location=get_printable_location)

def run():
    sqlite3 = Sqlite3(testdb, 'select name from contacts;')
    rc = sqlite3.mainloop()
    i = 0
    while rc == CConfig.SQLITE_ROW:
        jitdriver.jit_merge_point(i=i, sqlite3=sqlite3, rc=rc)
        textlen = sqlite3.python_sqlite3_column_bytes(0)
        rc = sqlite3.mainloop()
        i += textlen
    return i

def entry_point(argv):
    print run()
    return 0

def target(*args):
    return entry_point, None
    
if __name__ == "__main__":
    entry_point(sys.argv)
