import sys
import sqlite3
import os
import time

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/test.db")

def run(cursor, query):
    i = 0
    for name, in cursor.execute(query):
        # i += len(name)
        i += 1
    return i

def main_work(query):
    connection = sqlite3.connect(testdb)
    cursor = connection.cursor()
    for i in range(2):
        run(cursor, query)

    t1 = time.time()
    print run(cursor, query)
    t2 = time.time()
    print t2 - t1

def entry_point(argv):
    try:
        query = argv[1]
    except IndexError:
        print "You must supply a query to be run: e.g., 'select first_name from people where age > 1;'."
        return 1
    
    main_work(query)
    return 0

def target(*args):
    return entry_point
    
if __name__ == "__main__":
    entry_point(sys.argv)