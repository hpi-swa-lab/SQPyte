from rpython.rtyper.lltypesystem import rffi
from sqpyte.interpreter import Sqlite3DB, Sqlite3Query
from sqpyte.capi import CConfig
from sqpyte import capi
import os, sys

tpchdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tpch.db")

def test_join():
    db = Sqlite3DB(tpchdb)
    query = db.execute('select S.name, N.name from Supplier S, Nation N where S.nationkey = N.nationkey;')
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    i = 0
    while rc == CConfig.SQLITE_ROW:
        rc = query.mainloop()
        i += 1
    assert i == 100

def test_query_6():
    db = Sqlite3DB(tpchdb)
    queryStr = ("select "
                    "sum(l.extendedprice * l.discount) as revenue "
                "from "
                    "lineitem l "
                "where "
                    "l.shipdate >= date('1996-01-01') "
                    "and l.shipdate < date('1996-01-01', '+1 year') "
                    "and l.discount between 0.04 and 0.07 "
                    "and l.quantity < 25;"
        )
    query = db.execute(queryStr)
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert float(result) == 1524721.6695

def test_query_14():
    db = Sqlite3DB(tpchdb)
    queryStr = ("select "
                    "100.00 * sum(case "
                        "when p.type like 'PROMO%' "
                            "then l.extendedprice * (1 - l.discount) "
                        "else 0 "
                    "end) / sum(l.extendedprice * (1 - l.discount)) as promo_revenue "
                "from "
                    "lineitem l, "
                    "part p "
                "where "
                    "l.partkey = p.partkey "
                    "and l.shipdate >= date('1995-01-01') "
                    "and l.shipdate < date('1995-01-01', '+1 month');"
        )
    query = db.execute(queryStr)
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert float(result) == 15.9871053076363

def test_query_17():
    db = Sqlite3DB(tpchdb)
    queryStr = ("select "
                    "sum(l.extendedprice) / 7.0 as avg_yearly "
                "from "
                    "lineitem l, "
                    "part p "
                "where "
                    "p.partkey = l.partkey "
                    "and p.brand = 'Brand#45' "
                    "and p.container = 'SM PKG' "
                    "and l.quantity < ( "
                        "select "
                            "0.2 * avg(quantity) "
                        "from "
                            "lineitem "
                        "where "
                            "partkey = p.partkey "
                    ");"
        )
    query = db.execute(queryStr)
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    print textlen
    result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    print result
    assert float(result) == 3655.76571428571

def test_query_19():
    db = Sqlite3DB(tpchdb)
    queryStr = ("select "
                    "sum(l.extendedprice* (1 - l.discount)) as revenue "
                "from "
                    "lineitem l, "
                    "part p "
                "where "
                    "( "
                        "p.partkey = l.partkey "
                        "and p.brand = 'Brand#12' "
                        "and p.container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
                        "and l.quantity >= 1 and l.quantity <= 1 + 10 "
                        "and p.size between 1 and 5 "
                        "and l.shipmode in ('AIR', 'AIR REG') "
                        "and l.shipinstruct = 'DELIVER IN PERSON' "
                    ") "
                    "or "
                    "( "
                        "p.partkey = l.partkey "
                        "and p.brand = 'Brand#23' "
                        "and p.container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
                        "and l.quantity >= 10 and l.quantity <= 10 + 10 "
                        "and p.size between 1 and 10 "
                        "and l.shipmode in ('AIR', 'AIR REG') "
                        "and l.shipinstruct = 'DELIVER IN PERSON' "
                    ") "
                    "or "
                    "( "
                        "p.partkey = l.partkey "
                        "and p.brand = 'Brand#34' "
                        "and p.container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
                        "and l.quantity >= 20 and l.quantity <= 20 + 10 "
                        "and p.size between 1 and 15 "
                        "and l.shipmode in ('AIR', 'AIR REG') "
                        "and l.shipinstruct = 'DELIVER IN PERSON' "
                    ");"
        )
    query = db.execute(queryStr)
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert float(result) == 22923.028

def test_query_20():
    db = Sqlite3DB(tpchdb)
    queryStr = ("select "
                    "s.name, "
                    "s.address "
                "from "
                    "supplier s, "
                    "nation n "
                "where "
                    "s.suppkey in ( "
                        "select "
                            "ps.suppkey "
                        "from "
                            "partsupp ps "
                        "where "
                            "ps.partkey in ( "
                                "select "
                                    "partkey "
                                "from "
                                    "part "
                                "where "
                                    "name like 'b%' "
                            ") "
                            "and ps.availqty > ( "
                                "select "
                                    "0.5 * sum(l.quantity) "
                                "from "
                                    "lineitem l "
                                "where "
                                    "l.partkey = ps.partkey "
                                    "and l.suppkey = ps.suppkey "
                                    "and l.shipdate >= date('1995-01-01') "
                                    "and l.shipdate < date('1995-01-01', '+1 year') "
                            ") "
                    ") "
                    "and s.nationkey = n.nationkey "
                    "and n.name = 'BRAZIL' "
                "order by "
                    "s.name; "
        )
    query = db.execute(queryStr)
    rc = query.mainloop()
    assert rc == CConfig.SQLITE_ROW
    textlen = query.python_sqlite3_column_bytes(0)
    result = rffi.charpsize2str(rffi.cast(rffi.CCHARP, query.python_sqlite3_column_text(0)), textlen)
    assert float(result) == 22923.028

