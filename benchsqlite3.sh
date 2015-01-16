#!/bin/bash

WARMUP=5
SQPYTE_DIR=$(pwd)
DB=$SQPYTE_DIR/sqpyte/test/big-tpch.db

mkdir $SQPYTE_DIR/tpch/tmp-bench/

echo ".timer ON
.output $SQPYTE_DIR/tpch/tmp-bench/global-output.txt
" > $SQPYTE_DIR/tpch/tmp-bench/prefix.txt

for i in {1..22}
do
    cat $SQPYTE_DIR/tpch/tmp-bench/prefix.txt $SQPYTE_DIR/tpch/sqlite-queries-in/$i.sql > $SQPYTE_DIR/tpch/tmp-bench/input$i.sql
done

rm $SQPYTE_DIR/tpch/tmp-bench/prefix.txt


for i in {1..22}
do
    echo "========== Query $i =========="
    for j in {1..$WARMUP}
    do
        $SQPYTE_DIR/sqlite-install/bin/sqlite3 $DB < $SQPYTE_DIR/tpch/tmp-bench/input$i.sql > /dev/null
    done
    for j in {1..10}
    do
        $SQPYTE_DIR/sqlite-install/bin/sqlite3 $DB < $SQPYTE_DIR/tpch/tmp-bench/input$i.sql
    done    
done

rm -rf $SQPYTE_DIR/tpch/tmp-bench/
