#!/bin/bash

WARMUP=5
SQPYTE_DIR=$(pwd)
DB=$SQPYTE_DIR/sqpyte/test/big-tpch.db

for i in {1..22}
do
    echo "========== Query $i =========="
    if [ $i == 10 ]; then
        WARMUP=25
    fi
    for j in {1..10}
    do
        ./target-c -b $WARMUP $DB $SQPYTE_DIR/tpch/sqlite-queries-in/$i.sql
    done
done
