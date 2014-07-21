#!/bin/bash
DIR=$(pwd)
DB=$DIR/sqpyte/test/big-tpch.db

for i in {1..14} {16..22}
do
    echo "========== Query $i =========="
    for j in {1..10}
    do
        ./target-c -b $DB $DIR/tpch/sqlite-queries-in/$i.sql
    done
done
