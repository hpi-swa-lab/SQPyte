#!/bin/bash
DIR=$(pwd)
DB=$DIR/sqpyte/test/tpch.db

mkdir $DIR/tmp

for i in {1..22}
do
    if [ $i != 15 ]; then
        ./target-c -t $DB $DIR/tpch/sqlite-queries-in/$i.sql > $DIR/tmp/$i.out
    else
        ./target-c -t $DB $DIR/tpch/sqlite-queries-in/15-view-create.sql
        ./target-c -t $DB $DIR/tpch/sqlite-queries-in/15-query.sql > $DIR/tmp/15.out
        ./target-c -t $DB $DIR/tpch/sqlite-queries-in/15-view-delete.sql
    fi

    DIFF=$(diff $DIR/tmp/$i.out $DIR/tpch/sqlite-queries-out/$i.txt) 
    if [ "$DIFF" == "" ]; then
        echo -e "Query $i:\tPass"
    else
        echo -e "Query $i:\tFAIL"
    fi

    HG=$(hg st $DB)
    if [ "$HG" != "" ]; then
        echo -e "*** Reverted database to that in hg. ***"
        hg revert $DB
        rm $DB.orig
        rm $DB-journal
    fi

done

rm -rf $DIR/tmp
