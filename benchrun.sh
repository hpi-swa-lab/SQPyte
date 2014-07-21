#!/bin/bash
DIR=$(pwd)

for i in {1..14} {16..22}
do
    echo "========== Query $i =========="
    for j in {1..10}
    do
        ./target-c -b $DIR/tpch/sqlite-queries-in/$i.sql
    done
done
