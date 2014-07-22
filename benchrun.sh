#!/bin/bash
DIR=$(pwd)
DB=$DIR/sqpyte/test/big-tpch.db

for i in {1..14} {16..22}
do
    echo "========== Query $i =========="
    if [ $i == 15 ]; then
        for j in {1..10}
        do
            ./target-c -b $DB $DIR/tpch/sqlite-queries-in/$i.sql
        done
    else
        # Query 15 might leave the database in an inconsistent state!
        for j in {1..10}
        do
            ./target-c -b $DB $DIR/tpch/sqlite-queries-in/15-view-create.sql
            ./target-c -b $DB $DIR/tpch/sqlite-queries-in/15-query.sql
            ./target-c -b $DB $DIR/tpch/sqlite-queries-in/15-view-delete.sql
        done
    fi        
done
