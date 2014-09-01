#!/bin/bash

WARMUP=10
SQPYTE_DIR=$(pwd)
DB=$SQPYTE_DIR/sqpyte/test/big-tpch.db

for i in {1..22}
do
    echo "========== Query $i =========="
    for wu in $(seq 0 $WARMUP)
    do
        ./target-c -b $wu $DB $SQPYTE_DIR/tpch/sqlite-queries-in/$i.sql
    done
done
