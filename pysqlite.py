import sys
import sqlite3
import os
import time

testdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqpyte/test/test.db")

def run(cursor):
    query = 'select first_name from people where age > 1;'
    i = 0
    for name, in cursor.execute(query):
        i += len(name)
    return i


def main_work():
    connection = sqlite3.connect(testdb)
    cursor = connection.cursor()
    for i in range(2):
        run(cursor)

    t1 = time.time()
    print run(cursor)
    t2 = time.time()
    print t2 - t1

main_work()