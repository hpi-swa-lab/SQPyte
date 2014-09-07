#!/bin/bash

WARMUP=$1
QUERY_NUM=$2
SQPYTE_DIR=$(pwd)
DB=$SQPYTE_DIR/sqpyte/test/big-tpch.db

for wu in $(seq 0 $WARMUP)
do
    ./target-c -b $wu $DB $SQPYTE_DIR/tpch/sqlite-queries-in/$QUERY_NUM.sql
done
