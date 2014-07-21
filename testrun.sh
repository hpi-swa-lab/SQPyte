#!/bin/bash
DIR=$(pwd)
DB=$DIR/sqpyte/test/tpch.db

mkdir $DIR/tmp

for i in {1..22}
do
    ./target-c -t $DB $DIR/tpch/sqlite-queries-in/$i.sql > $DIR/tmp/$i.out

    DIFF=$(diff $DIR/tmp/$i.out $DIR/tpch/sqlite-queries-out/$i.txt) 
    if [ "$DIFF" == "" ]; then
        echo -e "Query $i:\tPass"
    else
        echo -e "Query $i:\tFAIL"
        HG=$(hg st $DB)
        if [ "$HG" != "" ]; then
            hg revert $DB
            rm $DB.orig
        fi
    fi
done

rm -rf $DIR/tmp
