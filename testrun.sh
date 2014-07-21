#!/bin/bash
DIR=$(pwd)

mkdir $DIR/tmp

for i in {1..22}
do
    ./target-c -t $DIR/tpch/sqlite-queries-in/$i.sql > $DIR/tmp/$i.out

    DIFF=$(diff $DIR/tmp/$i.out $DIR/tpch/sqlite-queries-out/$i.txt) 
    if [ "$DIFF" == "" ]; then
        echo -e "Query $i:\tPass"
    else
        echo -e "Query $i:\tFAIL"
        HG=$(hg st $DIR/sqpyte/test/tpch.db)
        if [ "$HG" != "" ]; then
            hg revert $DIR/sqpyte/test/tpch.db
            rm $DIR/sqpyte/test/tpch.db.orig
        fi
    fi
done

rm -rf $DIR/tmp
