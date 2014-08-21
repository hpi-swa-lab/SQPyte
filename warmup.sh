#!/bin/bash

WARMUP=5
SQPYTE_DIR=$(pwd)
DB=$SQPYTE_DIR/sqpyte/test/big-tpch.db

for i in {1..22}
do
    echo "========== Query $i =========="
    if [ $i != 15 ]; then
        for wu in {1..$WARMUP}
        do
            ./target-c -b $wu $DB $SQPYTE_DIR/tpch/sqlite-queries-in/$i.sql
        done
    else
        # XXX Query 15 currently leaves behind a view so this may fail,
        # but it won't affect the main query execution.
        ./target-c -b $DB $SQPYTE_DIR/tpch/sqlite-queries-in/15-view-create.sql > /dev/null
        for wu in {1..$WARMUP}
        do
            ./target-c -b $wu $DB $SQPYTE_DIR/tpch/sqlite-queries-in/15-query.sql
        done
        # XXX Deleting a view currently doesn't work.
        # ./target-c -b $DB $SQPYTE_DIR/tpch/sqlite-queries-in/15-view-delete.sql > /dev/null
    fi        
done
